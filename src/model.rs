use std::collections::HashMap;
use std::path::PathBuf;

use polars::datatypes::TimeUnit;
use polars::prelude::StrptimeOptions;
use polars::prelude::*;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDateTime;
use pyo3_polars::PyDataFrame;

use chrono::NaiveDateTime;

use crate::aggregation::Aggregation;
use crate::dag_tracer::DagTracer;
use crate::error::SdtError;
use crate::schema::*;
use crate::visualization::{self, VisualizationConfig};

#[pyclass]
pub struct SdtModel {
    base_path: PathBuf,
    transfers: Option<DataFrame>,
    containers: Option<DataFrame>,
    segments: Option<DataFrame>,
    tracer: Option<DagTracer>,
}

#[pymethods]
impl SdtModel {
    #[new]
    fn new(base_path: String) -> Self {
        Self {
            base_path: PathBuf::from(base_path),
            transfers: None,
            containers: None,
            segments: None,
            tracer: None,
        }
    }

    // ── Data loading ────────────────────────────────────────────────────────

    /// Load any CSV into a Polars DataFrame with all columns as strings.
    /// Optionally rename columns via a map.
    #[pyo3(signature = (filename, rename=None))]
    fn load_csv(
        &self,
        filename: &str,
        rename: Option<HashMap<String, String>>,
    ) -> PyResult<PyDataFrame> {
        let df = self.read_csv_as_strings(filename, rename)?;
        Ok(PyDataFrame(df))
    }

    /// Load transfers CSV.
    ///
    /// Minimum required columns:
    /// segment columns (required)
    ///     source_pop_id, dest_pop_id
    /// stock columns (required if factor columns are missing):
    ///     transfer_count, transfer_biomass_kg
    /// or factor columns (required if stock columna set missing):
    ///     share_count_forward, share_biomass_forward, share_count_backward, share_biomass_backward
    ///
    /// Share factors are calculated automatically but only if they are missing.
    /// Validation happens on row level - so if you want some rows may specify stock while others specify factors.
    /// All other  columns are preserved as strings.
    #[pyo3(signature = (filename=None))]
    fn load_transfers(&mut self, filename: Option<&str>) -> PyResult<PyDataFrame> {
        let fname = filename.unwrap_or("transfers.csv");
        let raw = self.read_csv_as_strings(fname, None)?;

        Self::require_columns(&raw, &[transfer::SOURCE_POP_ID, transfer::DEST_POP_ID])?;

        let schema = raw.schema();
        let has_stock_cols = schema.contains(transfer::TRANSFER_COUNT)
            && schema.contains(transfer::TRANSFER_BIOMASS_KG);
        let has_factor_cols = schema.contains(factors::SHARE_COUNT_FORWARD)
            && schema.contains(factors::SHARE_BIOMASS_FORWARD)
            && schema.contains(factors::SHARE_COUNT_BACKWARD)
            && schema.contains(factors::SHARE_BIOMASS_BACKWARD);

        if !has_stock_cols && !has_factor_cols {
            return Err(SdtError::InvalidData(
                "Transfers CSV must contain either (transfer_count, transfer_biomass_kg) \
             or all share factor columns"
                    .to_string(),
            )
            .into());
        }

        let mut lazy = raw.lazy();

        // Cast stock columns if present, otherwise create null columns
        if has_stock_cols {
            lazy = lazy.with_columns([
                col(transfer::TRANSFER_COUNT).cast(DataType::Float64),
                col(transfer::TRANSFER_BIOMASS_KG).cast(DataType::Float64),
            ]);
        } else {
            lazy = lazy.with_columns([
                lit(NULL)
                    .cast(DataType::Float64)
                    .alias(transfer::TRANSFER_COUNT),
                lit(NULL)
                    .cast(DataType::Float64)
                    .alias(transfer::TRANSFER_BIOMASS_KG),
            ]);
        }

        // Cast or create factor columns
        if has_factor_cols {
            lazy = lazy.with_columns([
                col(factors::SHARE_COUNT_FORWARD).cast(DataType::Float64),
                col(factors::SHARE_BIOMASS_FORWARD).cast(DataType::Float64),
                col(factors::SHARE_COUNT_BACKWARD).cast(DataType::Float64),
                col(factors::SHARE_BIOMASS_BACKWARD).cast(DataType::Float64),
            ]);
        } else {
            lazy = lazy.with_columns([
                lit(NULL)
                    .cast(DataType::Float64)
                    .alias(factors::SHARE_COUNT_FORWARD),
                lit(NULL)
                    .cast(DataType::Float64)
                    .alias(factors::SHARE_BIOMASS_FORWARD),
                lit(NULL)
                    .cast(DataType::Float64)
                    .alias(factors::SHARE_COUNT_BACKWARD),
                lit(NULL)
                    .cast(DataType::Float64)
                    .alias(factors::SHARE_BIOMASS_BACKWARD),
            ]);
        }

        // Calculate factors from stock (for rows that need it)
        let calc_forward_count = col(transfer::TRANSFER_COUNT)
            / col(transfer::TRANSFER_COUNT)
                .sum()
                .over([col(transfer::SOURCE_POP_ID)]);
        let calc_forward_biomass = col(transfer::TRANSFER_BIOMASS_KG)
            / col(transfer::TRANSFER_BIOMASS_KG)
                .sum()
                .over([col(transfer::SOURCE_POP_ID)]);
        let calc_backward_count = col(transfer::TRANSFER_COUNT)
            / col(transfer::TRANSFER_COUNT)
                .sum()
                .over([col(transfer::DEST_POP_ID)]);
        let calc_backward_biomass = col(transfer::TRANSFER_BIOMASS_KG)
            / col(transfer::TRANSFER_BIOMASS_KG)
                .sum()
                .over([col(transfer::DEST_POP_ID)]);

        // For each factor: use file value if present, otherwise calculate from stock
        lazy = lazy.with_columns([
            when(col(factors::SHARE_COUNT_FORWARD).is_not_null())
                .then(col(factors::SHARE_COUNT_FORWARD))
                .otherwise(calc_forward_count)
                .alias(factors::SHARE_COUNT_FORWARD),
            when(col(factors::SHARE_BIOMASS_FORWARD).is_not_null())
                .then(col(factors::SHARE_BIOMASS_FORWARD))
                .otherwise(calc_forward_biomass)
                .alias(factors::SHARE_BIOMASS_FORWARD),
            when(col(factors::SHARE_COUNT_BACKWARD).is_not_null())
                .then(col(factors::SHARE_COUNT_BACKWARD))
                .otherwise(calc_backward_count)
                .alias(factors::SHARE_COUNT_BACKWARD),
            when(col(factors::SHARE_BIOMASS_BACKWARD).is_not_null())
                .then(col(factors::SHARE_BIOMASS_BACKWARD))
                .otherwise(calc_backward_biomass)
                .alias(factors::SHARE_BIOMASS_BACKWARD),
        ]);

        let df = lazy.collect().map_err(SdtError::from)?;

        // Validate that all rows have complete factor data
        let factor_cols = [
            factors::SHARE_COUNT_FORWARD,
            factors::SHARE_BIOMASS_FORWARD,
            factors::SHARE_COUNT_BACKWARD,
            factors::SHARE_BIOMASS_BACKWARD,
        ];

        for factor_col in &factor_cols {
            let null_count = df.column(factor_col).map_err(SdtError::from)?.null_count();
            if null_count > 0 {
                return Err(SdtError::InvalidData(
            format!("All rows must have valid factor values. Column '{}' has {} null values. \
                     Provide either factor values or stock values (transfer_count, transfer_biomass_kg) for all rows.",
                     factor_col, null_count)
        ).into());
            }
        }
        self.transfers = Some(df.clone());
        self.tracer = None;
        Ok(PyDataFrame(df))
    }

    /// Load containers CSV.
    ///
    /// Required columns: container_id
    /// All user columns are preserved (as strings).
    #[pyo3(signature = (filename=None))]
    fn load_containers(&mut self, filename: Option<&str>) -> PyResult<PyDataFrame> {
        let fname = filename.unwrap_or("containers.csv");
        let raw = self.read_csv_as_strings(fname, None)?;

        Self::require_columns(&raw, &[container::CONTAINER_ID])?;

        self.containers = Some(raw.clone());
        Ok(PyDataFrame(raw))
    }

    /// Load segments CSV.
    ///
    /// Required columns: segment_id, container_id, start_time, end_time
    /// start_time and end_time are parsed as datetime (%Y-%m-%d %H:%M:%S).
    /// All user columns are preserved (as strings).
    #[pyo3(signature = (filename=None))]
    fn load_segments(&mut self, filename: Option<&str>) -> PyResult<PyDataFrame> {
        let fname = filename.unwrap_or("segments.csv");
        let raw = self.read_csv_as_strings(fname, None)?;

        Self::require_columns(
            &raw,
            &[
                segment::SEGMENT_ID,
                segment::CONTAINER_ID,
                segment::START_TIME,
                segment::END_TIME,
            ],
        )?;

        // Parse datetime columns
        let df = Self::parse_datetime_column(raw, segment::START_TIME, "%Y-%m-%d %H:%M:%S")?;
        let df = Self::parse_datetime_column(df, segment::END_TIME, "%Y-%m-%d %H:%M:%S")?;

        self.segments = Some(df.clone());
        Ok(PyDataFrame(df))
    }

    /// Load a segment-level timeseries CSV.
    ///
    /// Required columns: segment_id, date_time, + any value columns.
    /// All columns loaded as strings — use parse helpers before passing
    /// to aggregation methods.
    fn load_segment_timeseries(&self, filename: &str) -> PyResult<PyDataFrame> {
        let df = self.read_csv_as_strings(filename, None)?;
        Self::require_columns(&df, &[segment::SEGMENT_ID, timeseries::DATE_TIME])?;
        let df = Self::parse_datetime_column(df, timeseries::DATE_TIME, "%Y-%m-%d %H:%M:%S")?;

        Ok(PyDataFrame(df))
    }

    /// Load a container-level timeseries CSV.
    ///
    /// Required columns: container_id, date_time, + any value columns.
    /// All columns loaded as strings — use parse helpers before passing
    /// to aggregation or mapping methods.
    fn load_container_timeseries(&self, filename: &str) -> PyResult<PyDataFrame> {
        let df = self.read_csv_as_strings(filename, None)?;
        Self::require_columns(&df, &[container::CONTAINER_ID, timeseries::DATE_TIME])?;
        let df = Self::parse_datetime_column(df, timeseries::DATE_TIME, "%Y-%m-%d %H:%M:%S")?;
        Ok(PyDataFrame(df))
    }

    // ── Parse helpers ───────────────────────────────────────────────────────

    /// Parse a string column to Datetime using the given format string.
    ///
    /// Example formats: "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y"
    #[staticmethod]
    fn parse_datetime(df: PyDataFrame, column: &str, format: &str) -> PyResult<PyDataFrame> {
        let result = Self::parse_datetime_column(df.0, column, format)?;
        Ok(PyDataFrame(result))
    }

    /// Parse a string column to Float64.
    #[staticmethod]
    fn parse_float(df: PyDataFrame, column: &str) -> PyResult<PyDataFrame> {
        let result =
            df.0.lazy()
                .with_columns([col(column)
                    .str()
                    .strip_chars(lit(" \t\r\n"))
                    .cast(DataType::Float64)])
                .collect()
                .map_err(SdtError::from)?;
        Ok(PyDataFrame(result))
    }

    /// Parse a string column to Int64.
    #[staticmethod]
    fn parse_int(df: PyDataFrame, column: &str) -> PyResult<PyDataFrame> {
        let result =
            df.0.lazy()
                .with_columns([col(column)
                    .str()
                    .strip_chars(lit(" \t\r\n"))
                    .cast(DataType::Int64)])
                .collect()
                .map_err(SdtError::from)?;
        Ok(PyDataFrame(result))
    }

    // ── Tracing ─────────────────────────────────────────────────────────────

    /// Trace segments from a DataFrame containing a `segment_id` column.
    fn trace_segments(&mut self, origin_df: PyDataFrame) -> PyResult<PyDataFrame> {
        let tracer = self.get_or_build_tracer().map_err(SdtError::from)?;
        let ids: Vec<String> = origin_df
            .0
            .column(segment::SEGMENT_ID)
            .map_err(SdtError::from)?
            .str()
            .map_err(SdtError::from)?
            .into_iter()
            .filter_map(|v| v.map(|s| s.to_string()))
            .collect();

        let result = tracer.trace(&ids).map_err(SdtError::from)?;
        Ok(PyDataFrame(result))
    }

    // ── Filtering ───────────────────────────────────────────────────────────

    fn get_segments_active_at(&self, timestamp: Bound<PyDateTime>) -> PyResult<PyDataFrame> {
        // Reject timezone-aware datetimes
        if !timestamp.getattr("tzinfo")?.is_none() {
            return Err(PyValueError::new_err(
                "aqua-tracekit requires naive datetime objects (no timezone info). \
                 Use datetime(2024, 6, 15, 12, 0, 0) instead of datetime(..., tzinfo=...)",
            ));
        }

        let dt: NaiveDateTime = timestamp.extract()?;
        let timestamp_us = dt.and_utc().timestamp_micros();

        let pops = self
            .segments
            .as_ref()
            .ok_or_else(|| SdtError::NotLoaded("segments".into()))
            .map_err(SdtError::from)?;

        let df = pops
            .clone()
            .lazy()
            .filter(
                col(segment::START_TIME).lt_eq(lit(timestamp_us)).and(
                    col(segment::END_TIME)
                        .gt(lit(timestamp_us))
                        .or(col(segment::END_TIME).is_null()),
                ),
            )
            .collect()
            .map_err(SdtError::from)?;

        Ok(PyDataFrame(df))
    }

    fn get_segments_incoming(&self) -> PyResult<PyDataFrame> {
        let pops = self
            .segments
            .as_ref()
            .ok_or(SdtError::NotLoaded("segments".into()))
            .map_err(SdtError::from)?;
        let transfers = self
            .transfers
            .as_ref()
            .ok_or(SdtError::NotLoaded("transfers".into()))
            .map_err(SdtError::from)?;

        let dest_pops = transfers
            .column(transfer::DEST_POP_ID)
            .map_err(SdtError::from)?
            .as_materialized_series()
            .clone();

        let df = pops
            .clone()
            .lazy()
            .filter(
                col(segment::SEGMENT_ID)
                    .is_in(lit(dest_pops), false)
                    .not(),
            )
            .collect()
            .map_err(SdtError::from)?;

        Ok(PyDataFrame(df))
    }

    fn get_segments_outgoing(&self) -> PyResult<PyDataFrame> {
        let pops = self
            .segments
            .as_ref()
            .ok_or(SdtError::NotLoaded("segments".into()))
            .map_err(SdtError::from)?;
        let transfers = self
            .transfers
            .as_ref()
            .ok_or(SdtError::NotLoaded("transfers".into()))
            .map_err(SdtError::from)?;

        let source_pops = transfers
            .column(transfer::SOURCE_POP_ID)
            .map_err(SdtError::from)?
            .as_materialized_series()
            .clone();

        let df = pops
            .clone()
            .lazy()
            .filter(
                col(segment::SEGMENT_ID)
                    .is_in(lit(source_pops), false)
                    .not(),
            )
            .collect()
            .map_err(SdtError::from)?;

        Ok(PyDataFrame(df))
    }

    // ── Data |ing ────────────────────────────────────────────────────────

    /// Merge traced segment data with time-series or other segment-level data.
    #[staticmethod]
    fn add_data_to_trace(
        pop_data: PyDataFrame,
        traceability_index: PyDataFrame,
    ) -> PyResult<PyDataFrame> {
        let df = traceability_index
            .0
            .lazy()
            .join(
                pop_data.0.lazy(),
                [col(traceability::TRACED_SEGMENT_ID)],
                [col(segment::SEGMENT_ID)],
                JoinArgs::new(JoinType::Left),
            )
            .collect()
            .map_err(SdtError::from)?;

        Ok(PyDataFrame(df))
    }

    /// Map container-level timeseries to segments.
    /// Joins on container_id and filters to each segment's active period.
    ///
    /// A row matches if:
    ///   segment.container_id == container_data.container_id
    ///   AND segment.start_time <= date_time < segment.end_time
    ///   (null end_time means still active)
    ///
    /// The date_time column must be parsed to Datetime before calling this method.
    #[pyo3(signature = (container_data, include_unmatched=true, allow_multiple=true))]
    fn map_container_data_to_segments(
        &self,
        container_data: PyDataFrame,
        include_unmatched: bool,
        allow_multiple: bool,
    ) -> PyResult<PyDataFrame> {
        let pops = self
            .segments
            .as_ref()
            .ok_or(SdtError::NotLoaded("segments".into()))
            .map_err(SdtError::from)?;

        let input_cols: Vec<String> = container_data
            .0
            .get_column_names_str()
            .iter()
            .map(|s| s.to_string())
            .collect();

        // Join and filter to active period
        let join_type = if include_unmatched {
            JoinType::Left
        } else {
            JoinType::Inner
        };

        let mut output_cols: Vec<Expr> = input_cols.iter().map(|c| col(c)).collect();
        output_cols.push(col(segment::SEGMENT_ID));

        let matched = container_data
            .0
            .lazy()
            .join(
                pops.clone().lazy(),
                [col(container::CONTAINER_ID)],
                [col(segment::CONTAINER_ID)],
                JoinArgs::new(join_type),
            )
            .filter(
                // start_time <= date_time
                col(segment::START_TIME)
                    .lt_eq(col(timeseries::DATE_TIME))
                    .and(
                        // date_time < end_time OR end_time is null (still active)
                        col(segment::END_TIME)
                            .is_null()
                            .or(col(timeseries::DATE_TIME).lt(col(segment::END_TIME))),
                    )
                    // Also keep unmatched rows (where segment columns are null)
                    .or(col(segment::SEGMENT_ID).is_null()),
            )
            .select(output_cols)
            .collect()
            .map_err(SdtError::from)?;

        // Check for multiple matches if not allowed
        if !allow_multiple {
            let counts = matched
                .clone()
                .lazy()
                .filter(col(segment::SEGMENT_ID).is_not_null())
                .group_by(input_cols.iter().map(|c| col(c)).collect::<Vec<_>>())
                .agg([col(segment::SEGMENT_ID).count().alias("_match_count")])
                .filter(col("_match_count").gt(lit(1)))
                .collect()
                .map_err(SdtError::from)?;

            if counts.height() > 0 {
                return Err(SdtError::Validation(format!(
                    "{} rows matched multiple segments while allow_multiple=false",
                    counts.height()
                ))
                .into());
            }
        }

        Ok(PyDataFrame(matched))
    }

    // ── Aggregation (built-in) ──────────────────────────────────────────────

    /// Aggregate traced data using built-in Rust aggregations.
    ///
    /// `aggregations`: list of `Aggregation` objects.
    /// `group_by`: column names to group by.
    #[staticmethod]
    #[pyo3(signature = (traced_data, aggregations, group_by=None))]
    fn aggregate_traced_data(
        traced_data: PyDataFrame,
        aggregations: Vec<Aggregation>,
        group_by: Option<Vec<String>>,
    ) -> PyResult<PyDataFrame> {
        use crate::aggregation::apply_builtin_aggregations;

        let group_cols = group_by.unwrap_or_else(|| {
            vec![
                traceability::ORIGIN_SEGMENT_ID.to_string(),
                timeseries::DATE_TIME.to_string(),
            ]
        });

        let df = &traced_data.0;

        // Partition into group DataFrames
        let partitions = df
            .partition_by(group_cols.as_slice(), true)
            .map_err(SdtError::from)?;

        // Determine output column names from first group (or return empty)
        if partitions.is_empty() {
            return Ok(traced_data);
        }

        let sample_results =
            apply_builtin_aggregations(&partitions[0], &aggregations).map_err(SdtError::from)?;
        let agg_names: Vec<String> = sample_results
            .iter()
            .map(|(name, _)| name.clone())
            .collect();

        // Build column vectors: group key columns + aggregation result columns
        // Group keys: take first row of each partition
        let mut key_columns: Vec<Vec<AnyValue>> = vec![vec![]; group_cols.len()];
        let mut agg_columns: Vec<Vec<AnyValue>> = vec![vec![]; agg_names.len()];

        for partition in &partitions {
            // Extract group key values from first row
            for (i, gc) in group_cols.iter().enumerate() {
                let val = partition
                    .column(gc)
                    .map_err(SdtError::from)?
                    .get(0)
                    .map_err(SdtError::from)?;
                key_columns[i].push(val.into_static());
            }

            // Apply aggregations
            let results =
                apply_builtin_aggregations(partition, &aggregations).map_err(SdtError::from)?;
            for (i, (_name, val)) in results.into_iter().enumerate() {
                agg_columns[i].push(val);
            }
        }

        // Build the output DataFrame
        let mut columns: Vec<Column> = Vec::new();

        for (i, gc) in group_cols.iter().enumerate() {
            let series = Series::from_any_values(gc.into(), &key_columns[i], true)
                .map_err(SdtError::from)?;
            columns.push(series.into());
        }

        for (i, name) in agg_names.iter().enumerate() {
            let series = Series::from_any_values(name.into(), &agg_columns[i], true)
                .map_err(SdtError::from)?;
            columns.push(series.into());
        }

        let result = DataFrame::new(columns).map_err(SdtError::from)?;
        Ok(PyDataFrame(result))
    }
    // ── Column mapping utility ──────────────────────────────────────────────

    fn map_column(
        &self,
        df: PyDataFrame,
        source_column: &str,
        lookup_df: PyDataFrame,
        lookup_key: &str,
        lookup_value: &str,
        new_column: Option<&str>,
    ) -> PyResult<PyDataFrame> {
        let target = new_column.unwrap_or(lookup_value);

        let result =
            df.0.lazy()
                .join(
                    lookup_df
                        .0
                        .lazy()
                        .select([col(lookup_key), col(lookup_value)]),
                    [col(source_column)],
                    [col(lookup_key)],
                    JoinArgs::new(JoinType::Left),
                )
                .rename([lookup_value], [target], true)
                .collect()
                .map_err(SdtError::from)?;

        Ok(PyDataFrame(result))
    }

    // ── Properties ──────────────────────────────────────────────────────────

    #[getter]
    fn transfers_df(&self) -> PyResult<Option<PyDataFrame>> {
        Ok(self.transfers.clone().map(PyDataFrame))
    }

    #[getter]
    fn containers_df(&self) -> PyResult<Option<PyDataFrame>> {
        Ok(self.containers.clone().map(PyDataFrame))
    }

    #[getter]
    fn segments_df(&self) -> PyResult<Option<PyDataFrame>> {
        Ok(self.segments.clone().map(PyDataFrame))
    }

    // ── Visualization ───────────────────────────────────────────────────

    /// Visualize the trace as an interactive timeline chart.
    ///
    /// Returns a self-contained HTML string with SVG and JS.
    /// Use with `IPython.display.HTML(model.visualize_trace(...))` in Jupyter.
    ///
    /// Args:
    ///     container_label_col: Column from containers df for y-axis labels
    ///                         (default: "container_id")
    ///     segment_label_col: Column from segments df to display on rectangles
    ///                          (default: "segment_id")
    ///     segment_tooltip_cols: Columns from segments df to show on hover
    ///                             (default: [])
    ///     transfer_tooltip_cols: Columns from transfers df to show on transfer hover
    ///                           (default: ["transfer_count", "transfer_biomass_kg"])
    ///     gap_px: Pixel width of gap inserted at each transfer time (default: 32)
    ///     lane_height_px: Pixel height per container lane (default: 24)
    ///     initial_zoom: Initial zoom level (default: 1.0)
    #[pyo3(signature = (
    container_label_col = None,
    segment_label_col = None,
    segment_tooltip_cols = None,
    transfer_tooltip_cols = None,
    gap_px = 32,
    lane_height_px = 24,
    initial_zoom = 1.0,
))]
    fn visualize_trace(
        &self,
        container_label_col: Option<&str>,
        segment_label_col: Option<&str>,
        segment_tooltip_cols: Option<Vec<String>>,
        transfer_tooltip_cols: Option<Vec<String>>,
        gap_px: u32,
        lane_height_px: u32,
        initial_zoom: f64,
    ) -> PyResult<String> {
        let segments = self
            .segments
            .as_ref()
            .ok_or_else(|| SdtError::NotLoaded("segments".into()))?;
        let containers = self
            .containers
            .as_ref()
            .ok_or_else(|| SdtError::NotLoaded("containers".into()))?;
        let transfers = self
            .transfers
            .as_ref()
            .ok_or_else(|| SdtError::NotLoaded("transfers".into()))?;

        let config = VisualizationConfig {
            container_label_col: container_label_col
                .map(|s| s.to_string())
                .or_else(|| Some(container::CONTAINER_ID.to_string())),
            segment_label_col: segment_label_col
                .map(|s| s.to_string())
                .or_else(|| Some(segment::SEGMENT_ID.to_string())),
            segment_tooltip_cols: segment_tooltip_cols.unwrap_or_default(),
            transfer_tooltip_cols: transfer_tooltip_cols.unwrap_or_else(|| {
                vec![
                    transfer::TRANSFER_COUNT.to_string(),
                    transfer::TRANSFER_BIOMASS_KG.to_string(),
                ]
            }),
            gap_px,
            lane_height_px,
            initial_zoom,
        };

        visualization::generate_trace_html(segments, containers, transfers, &config)
            .map_err(|e| e.into())
    }
}

// ── Private helpers ─────────────────────────────────────────────────────────

impl SdtModel {
    /// Read a CSV file with all columns as String dtype.
    /// Trims whitespace from column names and applies optional rename.
    fn read_csv_as_strings(
        &self,
        filename: &str,
        rename: Option<HashMap<String, String>>,
    ) -> Result<DataFrame, SdtError> {
        let path = self.base_path.join(filename);
        let mut df = CsvReadOptions::default()
            .with_has_header(true)
            .with_infer_schema_length(Some(0)) // all columns as String
            .try_into_reader_with_file_path(Some(path))?
            .finish()?;

        // Trim whitespace from column names
        let trimmed: Vec<String> = df
            .get_column_names_str()
            .iter()
            .map(|c| c.trim().to_string())
            .collect();
        df.set_column_names(trimmed.as_slice())?;

        // Apply optional column rename
        if let Some(map) = rename {
            let old: Vec<&str> = map.keys().map(|s| s.as_str()).collect();
            let new: Vec<&str> = map.values().map(|s| s.as_str()).collect();
            df = df.lazy().rename(old, new, true).collect()?;
        }

        Ok(df)
    }

    fn get_or_build_tracer(&mut self) -> Result<&DagTracer, SdtError> {
        if self.tracer.is_none() {
            let transfers = self
                .transfers
                .as_ref()
                .ok_or_else(|| SdtError::NotLoaded("transfers".into()))?;
            self.tracer = Some(DagTracer::from_transfers(transfers)?);
        }
        Ok(self.tracer.as_ref().unwrap())
    }

    fn require_columns(df: &DataFrame, required: &[&str]) -> PyResult<()> {
        for &col_name in required {
            if df.column(col_name).is_err() {
                return Err(SdtError::MissingColumn(col_name.to_string()).into());
            }
        }
        Ok(())
    }

    /// Parse a string column to Datetime. Handles null values gracefully.
    fn parse_datetime_column(
        df: DataFrame,
        column: &str,
        format: &str,
    ) -> Result<DataFrame, SdtError> {
        if df.column(column).is_ok() {
            let df = df
                .lazy()
                .with_columns([col(column)
                    .str()
                    .strip_chars(lit(" \t\r\n"))
                    .str()
                    .to_datetime(
                        Some(TimeUnit::Microseconds),
                        None,
                        StrptimeOptions {
                            format: Some(format.into()),
                            strict: true,
                            ..Default::default()
                        },
                        lit("raise"),
                    )])
                .collect()?;
            Ok(df)
        } else {
            Ok(df)
        }
    }
}
