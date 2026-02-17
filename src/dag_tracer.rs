use std::collections::HashMap;

use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::Direction;
use polars::prelude::*;

use crate::error::SdtError;
use crate::schema::{factors, traceability, transfer};

/// Edge payload: the four share/trace factors.
#[derive(Debug, Clone)]
struct EdgeFactors {
    values: [f64; 4], // indexed same as factors::ALL
}

/// Core directed-acyclic-graph tracer.
///
/// Builds a petgraph DiGraph from a transfers DataFrame and exposes
/// forward / backward tracing with factor aggregation.
pub struct DagTracer {
    graph: DiGraph<String, EdgeFactors>,
    /// Map from population-id string → NodeIndex for fast lookup.
    node_map: HashMap<String, NodeIndex>,
}

impl DagTracer {
    /// Build the graph from a transfers DataFrame.
    ///
    /// Required columns: source_pop, dest_pop, and the four factor columns.
    pub fn from_transfers(df: &DataFrame) -> Result<Self, SdtError> {
        let source = df.column(transfer::SOURCE_POP_ID)?.str()?;
        let dest = df.column(transfer::DEST_POP_ID)?.str()?;

        let factor_series: Vec<&ChunkedArray<Float64Type>> = factors::ALL
            .iter()
            .map(|name| df.column(name).and_then(|s| Ok(s.f64()?)))
            .collect::<Result<Vec<_>, _>>()?;

        let mut graph = DiGraph::new();
        let mut node_map: HashMap<String, NodeIndex> = HashMap::new();

        let get_or_insert = |map: &mut HashMap<String, NodeIndex>,
                                  g: &mut DiGraph<String, EdgeFactors>,
                                  id: &str|
         -> NodeIndex {
            *map.entry(id.to_string())
                .or_insert_with(|| g.add_node(id.to_string()))
        };

        for i in 0..df.height() {
            let src = source.get(i).ok_or_else(|| {
                SdtError::General(format!("Null source_pop at row {i}"))
            })?;
            let dst = dest.get(i).ok_or_else(|| {
                SdtError::General(format!("Null dest_pop at row {i}"))
            })?;

            let mut values = [0.0f64; 4];
            for (j, fs) in factor_series.iter().enumerate() {
                values[j] = fs.get(i).unwrap_or(0.0);
            }

            let src_idx = get_or_insert(&mut node_map, &mut graph, src);
            let dst_idx = get_or_insert(&mut node_map, &mut graph, dst);
            graph.add_edge(src_idx, dst_idx, EdgeFactors { values });
        }

        Ok(Self { graph, node_map })
    }

    /// Trace all reachable populations from a set of origin population ids.
    ///
    /// Returns a DataFrame with columns:
    ///   origin_population, traced_population, direction, + 4 factor columns
    pub fn trace(&self, origin_ids: &[String]) -> Result<DataFrame, SdtError> {
        let mut origins = Vec::new();
        let mut traced = Vec::new();
        let mut directions = Vec::new();
        let mut factor_vecs: [Vec<f64>; 4] = [vec![], vec![], vec![], vec![]];

        for origin_id in origin_ids {
            self.trace_single(
                origin_id,
                &mut origins,
                &mut traced,
                &mut directions,
                &mut factor_vecs,
            );
        }

        let df = DataFrame::new(vec![
            Column::new(traceability::ORIGIN_POPULATION_ID.into(), &origins),
            Column::new(traceability::TRACED_POPULATION_ID.into(), &traced),
            Column::new(traceability::TRACE_DIRECTION.into(), &directions),
            Column::new(factors::ALL[0].into(), &factor_vecs[0]),
            Column::new(factors::ALL[1].into(), &factor_vecs[1]),
            Column::new(factors::ALL[2].into(), &factor_vecs[2]),
            Column::new(factors::ALL[3].into(), &factor_vecs[3]),
        ])?;

        Ok(df)
    }

    fn trace_single(
        &self,
        origin_id: &str,
        origins: &mut Vec<String>,
        traced: &mut Vec<String>,
        directions: &mut Vec<String>,
        factor_vecs: &mut [Vec<f64>; 4],
    ) {
        // Identity row
        origins.push(origin_id.to_string());
        traced.push(origin_id.to_string());
        directions.push("identity".to_string());
        for fv in factor_vecs.iter_mut() {
            fv.push(1.0);
        }

        let Some(&origin_idx) = self.node_map.get(origin_id) else {
            return; // not in graph — only identity row
        };

        // Forward: origin → descendants
        let descendants = self.reachable(origin_idx, Direction::Outgoing);
        for target_idx in &descendants {
            let agg = self.aggregate_path_factors(origin_idx, *target_idx);
            origins.push(origin_id.to_string());
            traced.push(self.graph[*target_idx].clone());
            directions.push("forward".to_string());
            for (j, fv) in factor_vecs.iter_mut().enumerate() {
                fv.push(agg[j]);
            }
        }

        // Backward: ancestors → origin
        let ancestors = self.reachable(origin_idx, Direction::Incoming);
        for source_idx in &ancestors {
            let agg = self.aggregate_path_factors(*source_idx, origin_idx);
            origins.push(origin_id.to_string());
            traced.push(self.graph[*source_idx].clone());
            directions.push("backward".to_string());
            for (j, fv) in factor_vecs.iter_mut().enumerate() {
                fv.push(agg[j]);
            }
        }
    }

    /// Find all nodes reachable from `start` following edges in `direction`.
    fn reachable(&self, start: NodeIndex, direction: Direction) -> Vec<NodeIndex> {
        let mut result = Vec::new();

        // For outgoing we DFS on the graph as-is.
        // For incoming we DFS on the reversed graph.
        // petgraph Dfs only goes forward, so we use neighbors_directed manually.
        let mut stack = Vec::new();
        let mut visited = std::collections::HashSet::new();

        for neighbor in self.graph.neighbors_directed(start, direction) {
            stack.push(neighbor);
        }

        while let Some(node) = stack.pop() {
            if !visited.insert(node) {
                continue;
            }
            result.push(node);
            for neighbor in self.graph.neighbors_directed(node, direction) {
                if !visited.contains(&neighbor) {
                    stack.push(neighbor);
                }
            }
        }

        result
    }

    /// Aggregate factors across all simple paths from `source` to `target`.
    ///
    /// For each path, factors are multiplied along edges.
    /// Across paths, factors are summed (same logic as the Python version).
    fn aggregate_path_factors(&self, source: NodeIndex, target: NodeIndex) -> [f64; 4] {
        let mut totals = [0.0f64; 4];
        let mut path = Vec::new();
        self.enumerate_paths(source, target, &mut path, &mut totals);
        totals
    }

    /// Recursive DFS enumeration of all simple paths, accumulating factor products.
    fn enumerate_paths(
        &self,
        current: NodeIndex,
        target: NodeIndex,
        path: &mut Vec<NodeIndex>,
        totals: &mut [f64; 4],
    ) {
        path.push(current);

        if current == target {
            // Multiply factors along this path
            let mut product = [1.0f64; 4];
            for window in path.windows(2) {
                let edge_idx = self
                    .graph
                    .find_edge(window[0], window[1])
                    .expect("edge must exist on path");
                let factors = &self.graph[edge_idx];
                for i in 0..4 {
                    product[i] *= factors.values[i];
                }
            }
            for i in 0..4 {
                totals[i] += product[i];
            }
        } else {
            for neighbor in self.graph.neighbors_directed(current, Direction::Outgoing) {
                if !path.contains(&neighbor) {
                    self.enumerate_paths(neighbor, target, path, totals);
                }
            }
        }

        path.pop();
    }
}
