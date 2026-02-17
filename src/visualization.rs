/// Visualization module: Gantt-style trace chart with non-linear time axis.
///
/// Produces a self-contained HTML string with inline JS that handles:
/// - Rectangles per population (positioned by container lane + time span)
/// - Arrows between populations indicating transfers
/// - Non-linear time axis that inserts fixed-pixel gaps at transfer times
/// - Zoom, pan, scrolling, and tooltips
///
/// All SVG rendering is done client-side by sdt_chart.js + time_axis.js.
/// This module extracts data from DataFrames, serializes to JSON, and emits
/// the HTML shell.
use std::collections::{BTreeSet, HashMap};
use std::fmt::Write as FmtWrite;

use polars::datatypes::AnyValue;
use polars::prelude::*;

use crate::error::SdtError;
use crate::schema::*;

const TIME_AXIS_JS: &str = include_str!("time_axis.js");
const CHART_JS: &str = include_str!("sdt_chart.js");

// ── Config ──────────────────────────────────────────────────────────────────

/// Configuration for the trace visualization.
pub struct VisualizationConfig {
    /// Column from containers df to use as y-axis label (default: container_id)
    pub container_label_col: Option<String>,
    /// Column from populations df to display on the rectangle
    pub population_label_col: Option<String>,
    /// Columns from populations df to show in tooltip on hover
    pub population_tooltip_cols: Vec<String>,
    /// Columns from transfers df to show in tooltip on transfer arrow hover
    pub transfer_tooltip_cols: Vec<String>,
    /// Fixed pixel width inserted at each unique transfer time
    pub gap_px: u32,
    /// Fixed pixel height per container lane
    pub lane_height_px: u32,
    /// Initial zoom level (pixels per microsecond of real time)
    pub initial_zoom: f64,
}

// ── Intermediate data structures ────────────────────────────────────────────

struct PopulationRect {
    pop_id: String,
    container_id: String,
    start_us: i64,
    end_us: i64,
    label: Option<String>,
    tooltip_fields: Vec<(String, String)>,
}

struct TransferArrow {
    source_pop_id: String,
    dest_pop_id: String,
    transfer_time_us: i64,
    tooltip_fields: Vec<(String, String)>,
}

struct ContainerLane {
    container_id: String,
    label: String,
}

// ── Data extraction ─────────────────────────────────────────────────────────

fn extract_populations(
    populations: &DataFrame,
    config: &VisualizationConfig,
) -> Result<Vec<PopulationRect>, SdtError> {
    let n = populations.height();
    let pop_ids = populations.column(population::POPULATION_ID)?.str()?;
    let container_ids = populations.column(population::CONTAINER_ID)?.str()?;
    let start_times = populations
        .column(population::START_TIME)?
        .as_materialized_series();
    let end_times = populations
        .column(population::END_TIME)?
        .as_materialized_series();

    let label_col = config
        .population_label_col
        .as_deref()
        .and_then(|c| populations.column(c).ok());

    let tooltip_cols: Vec<(&str, &Series)> = config
        .population_tooltip_cols
        .iter()
        .filter_map(|c| {
            populations
                .column(c.as_str())
                .ok()
                .map(|col| (c.as_str(), col.as_materialized_series()))
        })
        .collect();

    let mut rects = Vec::with_capacity(n);
    for i in 0..n {
        let pop_id = pop_ids.get(i).unwrap_or("").to_string();
        let container_id = container_ids.get(i).unwrap_or("").to_string();
        let start_us = match start_times.get(i) {
            Ok(AnyValue::Datetime(us, _, _)) => us,
            _ => 0,
        };
        let end_us = match end_times.get(i) {
            Ok(AnyValue::Datetime(us, _, _)) => us,
            _ => start_us,
        };

        let label = label_col.and_then(|col| {
            let val = col.get(i).ok()?;
            let s = format!("{}", val);
            if s == "null" {
                None
            } else {
                Some(s)
            }
        });

        let tooltip_fields: Vec<(String, String)> = tooltip_cols
            .iter()
            .filter_map(|(name, col)| {
                let val = col.get(i).ok()?;
                let s = format!("{}", val);
                if s == "null" {
                    None
                } else {
                    Some((name.to_string(), s))
                }
            })
            .collect();

        rects.push(PopulationRect {
            pop_id,
            container_id,
            start_us,
            end_us,
            label,
            tooltip_fields,
        });
    }
    Ok(rects)
}

fn extract_transfers(
    transfers: &DataFrame,
    populations: &DataFrame,
    config: &VisualizationConfig,
) -> Result<Vec<TransferArrow>, SdtError> {
    let n = transfers.height();
    let source_ids = transfers.column(transfer::SOURCE_POP_ID)?.str()?;
    let dest_ids = transfers.column(transfer::DEST_POP_ID)?.str()?;

    // Build pop_id -> end_time / start_time lookup from populations
    let pop_ids = populations.column(population::POPULATION_ID)?.str()?;
    let end_times = populations
        .column(population::END_TIME)?
        .as_materialized_series();
    let start_times = populations
        .column(population::START_TIME)?
        .as_materialized_series();

    let mut pop_end_time: HashMap<String, i64> = HashMap::new();
    let mut pop_start_time: HashMap<String, i64> = HashMap::new();
    for i in 0..populations.height() {
        if let Some(pid) = pop_ids.get(i) {
            if let Ok(AnyValue::Datetime(et, _, _)) = end_times.get(i) {
                pop_end_time.insert(pid.to_string(), et);
            }
            if let Ok(AnyValue::Datetime(st, _, _)) = start_times.get(i) {
                pop_start_time.insert(pid.to_string(), st);
            }
        }
    }

    let tooltip_cols: Vec<(&str, &Series)> = config
        .transfer_tooltip_cols
        .iter()
        .filter_map(|c| {
            transfers
                .column(c.as_str())
                .ok()
                .map(|col| (c.as_str(), col.as_materialized_series()))
        })
        .collect();

    let mut arrows = Vec::with_capacity(n);
    for i in 0..n {
        let src = source_ids.get(i).unwrap_or("").to_string();
        let dst = dest_ids.get(i).unwrap_or("").to_string();

        // Transfer time = source pop end_time, fallback to dest pop start_time
        let transfer_time_us = pop_end_time
            .get(&src)
            .or_else(|| pop_start_time.get(&dst))
            .copied()
            .unwrap_or(0);

        let tooltip_fields: Vec<(String, String)> = tooltip_cols
            .iter()
            .filter_map(|(name, col)| {
                let val = col.get(i).ok()?;
                let s = format!("{}", val);
                if s == "null" {
                    None
                } else {
                    Some((name.to_string(), s))
                }
            })
            .collect();

        arrows.push(TransferArrow {
            source_pop_id: src,
            dest_pop_id: dst,
            transfer_time_us,
            tooltip_fields,
        });
    }
    Ok(arrows)
}

fn extract_container_lanes(
    containers: &DataFrame,
    populations: &[PopulationRect],
    config: &VisualizationConfig,
) -> Result<Vec<ContainerLane>, SdtError> {
    let active_ids: BTreeSet<&str> = populations.iter().map(|p| p.container_id.as_str()).collect();

    let cid_col = containers.column(container::CONTAINER_ID)?.str()?;
    let label_col = config
        .container_label_col
        .as_deref()
        .and_then(|c| containers.column(c).ok());

    let mut lanes = Vec::new();
    for i in 0..containers.height() {
        let cid = cid_col.get(i).unwrap_or("");
        if !active_ids.contains(cid) {
            continue;
        }
        let label = label_col
            .and_then(|col| {
                let val = col.get(i).ok()?;
                let s = format!("{}", val);
                if s == "null" {
                    None
                } else {
                    Some(s)
                }
            })
            .unwrap_or_else(|| cid.to_string());

        lanes.push(ContainerLane {
            container_id: cid.to_string(),
            label,
        });
    }
    Ok(lanes)
}

/// Sorted unique transfer times used for gap insertion.
fn collect_transfer_times(arrows: &[TransferArrow]) -> Vec<i64> {
    let mut times: BTreeSet<i64> = BTreeSet::new();
    for a in arrows {
        times.insert(a.transfer_time_us);
    }
    times.into_iter().collect()
}

// ── HTML generation ─────────────────────────────────────────────────────────

/// Main entry point: generates a self-contained HTML string.
///
/// Extracts data from the DataFrames, serializes to JSON, and emits an HTML
/// shell with embedded JS that handles all SVG rendering client-side.
pub fn generate_trace_html(
    populations: &DataFrame,
    containers: &DataFrame,
    transfers: &DataFrame,
    config: &VisualizationConfig,
) -> Result<String, SdtError> {
    // ── Extract data ────────────────────────────────────────────────────
    let rects = extract_populations(populations, config)?;
    let arrows = extract_transfers(transfers, populations, config)?;
    let lanes = extract_container_lanes(containers, &rects, config)?;

    if rects.is_empty() {
        return Ok("<div>No populations to visualize.</div>".to_string());
    }

    // ── Layout parameters (passed to JS) ────────────────────────────────
    let transfer_times = collect_transfer_times(&arrows);

    let t_min = rects.iter().map(|r| r.start_us).min().unwrap_or(0);
    let t_max = rects.iter().map(|r| r.end_us).max().unwrap_or(1);
    let time_range = (t_max - t_min).max(1) as f64;

    // Scale: 1.0 zoom = ~800px for the full time range (before gaps)
    let time_scale = time_range / 800.0;

    // ── Emit HTML ───────────────────────────────────────────────────────
    let html = format!(
        r##"<div style="position:relative; width:100%; border:1px solid #dee2e6; border-radius:4px; background:#fff;">
  <div style="padding:4px 8px; border-bottom:1px solid #dee2e6; font-family:sans-serif; font-size:12px; color:#495057; display:flex; align-items:center; gap:8px;">
    <span style="font-weight:600;">Trace Visualization</span>
    <button onclick="sdtZoom(1.5)" style="cursor:pointer; padding:2px 8px;">Zoom +</button>
    <button onclick="sdtZoom(1/1.5)" style="cursor:pointer; padding:2px 8px;">Zoom −</button>
    <button onclick="sdtResetZoom()" style="cursor:pointer; padding:2px 8px;">Reset</button>
    <span id="sdt-zoom-label" style="color:#868e96; font-size:11px;">1.0x</span>
  </div>
  <div id="sdt-scroll-container" style="overflow:auto; max-height:600px;">
    <svg id="sdt-svg" xmlns="http://www.w3.org/2000/svg" width="100" height="100">
      <style>
        .lane-label {{ font-family: sans-serif; font-size: 12px; fill: #495057; text-anchor: end; }}
        .time-label {{ font-family: sans-serif; font-size: 10px; fill: #868e96; text-anchor: middle; }}
        .pop-rect {{ fill: #4dabf7; stroke: #339af0; stroke-width: 1; cursor: pointer; }}
        .pop-rect:hover {{ fill: #339af0; stroke: #228be6; stroke-width: 2; }}
        .pop-label {{ font-family: sans-serif; font-size: 10px; fill: #fff; pointer-events: none; }}
        .transfer-arrow {{ cursor: pointer; }}
        .transfer-arrow:hover {{ stroke: #c0392b; stroke-width: 2.5; }}
      </style>
      <defs>
        <marker id="arrowhead" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
          <polygon points="0 0, 8 3, 0 6" fill="#e74c3c" />
        </marker>
      </defs>
    </svg>
  </div>
</div>
<script>
{time_axis_js}
{chart_js}
SdtChart.create({{
  zoom: {zoom}, tMin: {t_min}, tMax: {t_max},
  timeScale: {time_scale}, gapPx: {gap_px},
  transferTimes: {transfer_times_json},
  marginLeft: 120, marginTop: 40,
  marginRight: 40, marginBottom: 20,
  laneHeight: {lane_height}, numLanes: {num_lanes},
  rectPadding: 4,
  populations: {populations_json},
  transfers: {transfers_json},
  lanes: {lanes_json}
}});
</script>"##,
        zoom = config.initial_zoom,
        t_min = t_min,
        t_max = t_max,
        time_scale = time_scale,
        gap_px = config.gap_px,
        transfer_times_json = to_json_array_i64(&transfer_times),
        lane_height = config.lane_height_px,
        num_lanes = lanes.len(),
        populations_json = populations_to_json(&rects),
        transfers_json = transfers_to_json(&arrows),
        lanes_json = lanes_to_json(&lanes),
        time_axis_js = TIME_AXIS_JS,
        chart_js = CHART_JS,
    );

    Ok(html)
}

// ── JSON serialization helpers ──────────────────────────────────────────────

fn to_json_array_i64(vals: &[i64]) -> String {
    let mut s = String::from("[");
    for (i, v) in vals.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        write!(s, "{}", v).unwrap();
    }
    s.push(']');
    s
}

fn populations_to_json(rects: &[PopulationRect]) -> String {
    let mut s = String::from("[");
    for (i, r) in rects.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        let tooltip = r
            .tooltip_fields
            .iter()
            .map(|(k, v)| format!("{}: {}", k, v))
            .collect::<Vec<_>>()
            .join("\n");
        write!(
            s,
            r##"{{"pop_id":"{}","container_id":"{}","start_us":{},"end_us":{},"label":{},"tooltip":{}}}"##,
            escape_json(&r.pop_id),
            escape_json(&r.container_id),
            r.start_us,
            r.end_us,
            match &r.label {
                Some(l) => format!(r##""{}""##, escape_json(l)),
                None => "null".to_string(),
            },
            if tooltip.is_empty() {
                "null".to_string()
            } else {
                format!(r##""{}""##, escape_json(&tooltip))
            },
        )
        .unwrap();
    }
    s.push(']');
    s
}

fn transfers_to_json(arrows: &[TransferArrow]) -> String {
    let mut s = String::from("[");
    for (i, a) in arrows.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        let tooltip = a
            .tooltip_fields
            .iter()
            .map(|(k, v)| format!("{}: {}", k, v))
            .collect::<Vec<_>>()
            .join("\n");
        write!(
            s,
            r##"{{"source_pop_id":"{}","dest_pop_id":"{}","transfer_time_us":{},"tooltip":{}}}"##,
            escape_json(&a.source_pop_id),
            escape_json(&a.dest_pop_id),
            a.transfer_time_us,
            if tooltip.is_empty() {
                "null".to_string()
            } else {
                format!(r##""{}""##, escape_json(&tooltip))
            },
        )
        .unwrap();
    }
    s.push(']');
    s
}

fn lanes_to_json(lanes: &[ContainerLane]) -> String {
    let mut s = String::from("[");
    for (i, l) in lanes.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        write!(
            s,
            r##"{{"container_id":"{}","label":"{}"}}"##,
            escape_json(&l.container_id),
            escape_json(&l.label),
        )
        .unwrap();
    }
    s.push(']');
    s
}

fn escape_json(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}