use crate::error::SdtError;
use crate::schema::{factors, traceability};
use polars::prelude::*;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_polars::PyDataFrame;

/// Aggregation dimension for direction-aware weighted operations.
#[derive(Debug, Clone, Copy)]
pub enum AggregateBy {
    Count,
    Biomass,
}

/// Declarative aggregation specification.
///
/// Users build these from Python; the Rust engine executes them.
#[derive(Debug, Clone)]
#[pyclass(name = "Aggregation")]
pub struct Aggregation {
    pub(crate) kind: AggKind,
}

impl Clone for AggKind {
    fn clone(&self) -> Self {
        match self {
            Self::Custom { callable } => {
                let cloned = Python::with_gil(|py| callable.clone_ref(py));
                Self::Custom { callable: cloned }
            }
            Self::Min { column, alias } => Self::Min {
                column: column.clone(),
                alias: alias.clone(),
            },
            Self::Max { column, alias } => Self::Max {
                column: column.clone(),
                alias: alias.clone(),
            },
            Self::Sum { columns } => Self::Sum {
                columns: columns.clone(),
            },
            Self::Avg { columns } => Self::Avg {
                columns: columns.clone(),
            },
            Self::WeightedSum {
                columns,
                aggregate_by,
                include_calculation,
            } => Self::WeightedSum {
                columns: columns.clone(),
                aggregate_by: *aggregate_by,
                include_calculation: *include_calculation,
            },
            Self::WeightedAvg {
                column,
                aggregate_by,
            } => Self::WeightedAvg {
                column: column.clone(),
                aggregate_by: *aggregate_by,
            },
            Self::Concat {
                columns,
                separator,
                unique,
            } => Self::Concat {
                columns: columns.clone(),
                separator: separator.clone(),
                unique: *unique,
            },
            Self::ContributionBreakdown {
                columns,
                field_separator,
                row_separator,
                alias,
            } => Self::ContributionBreakdown {
                columns: columns.clone(),
                field_separator: field_separator.clone(),
                row_separator: row_separator.clone(),
                alias: alias.clone(),
            },
        }
    }
}

#[derive(Debug)]
pub enum AggKind {
    Custom {
        callable: PyObject,
    },
    Min {
        column: String,
        alias: Option<String>,
    },
    Max {
        column: String,
        alias: Option<String>,
    },
    Sum {
        columns: Vec<String>,
    },
    Avg {
        columns: Vec<String>,
    },
    WeightedSum {
        columns: Vec<String>,
        aggregate_by: AggregateBy,
        include_calculation: bool,
    },
    WeightedAvg {
        column: String,
        aggregate_by: AggregateBy,
    },
    Concat {
        columns: Vec<String>,
        separator: String,
        unique: bool,
    },
    ContributionBreakdown {
        columns: Vec<String>,
        field_separator: String, // between fields within a row, e.g. ":"
        row_separator: String,   // between rows, e.g. ", "
        alias: Option<String>,
    },
}

#[pymethods]
impl Aggregation {
    #[staticmethod]
    fn custom(callable: PyObject) -> Self {
        Self {
            kind: AggKind::Custom { callable },
        }
    }

    #[staticmethod]
    #[pyo3(signature = (column, alias=None))]
    fn min(column: String, alias: Option<String>) -> Self {
        Self {
            kind: AggKind::Min { column, alias },
        }
    }

    #[staticmethod]
    #[pyo3(signature = (column, alias=None))]
    fn max(column: String, alias: Option<String>) -> Self {
        Self {
            kind: AggKind::Max { column, alias },
        }
    }

    #[staticmethod]
    fn sum(columns: Vec<String>) -> Self {
        Self {
            kind: AggKind::Sum { columns },
        }
    }

    #[staticmethod]
    fn avg(columns: Vec<String>) -> Self {
        Self {
            kind: AggKind::Avg { columns },
        }
    }

    #[staticmethod]
    #[pyo3(signature = (columns, aggregate_by, include_calculation=false))]
    fn weighted_sum(
        columns: Vec<String>,
        aggregate_by: String,
        include_calculation: bool,
    ) -> PyResult<Self> {
        let agg_by = match aggregate_by.as_str() {
            "count" => AggregateBy::Count,
            "biomass" => AggregateBy::Biomass,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Invalid aggregate_by: '{}'. Must be 'count' or 'biomass'",
                    aggregate_by
                )))
            }
        };
        Ok(Self {
            kind: AggKind::WeightedSum {
                columns,
                aggregate_by: agg_by,
                include_calculation,
            },
        })
    }

    #[staticmethod]
    fn weighted_avg(column: String, aggregate_by: String) -> PyResult<Self> {
        let agg_by = match aggregate_by.as_str() {
            "count" => AggregateBy::Count,
            "biomass" => AggregateBy::Biomass,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Invalid aggregate_by: '{}'. Must be 'count' or 'biomass'",
                    aggregate_by
                )))
            }
        };
        Ok(Self {
            kind: AggKind::WeightedAvg {
                column,
                aggregate_by: agg_by,
            },
        })
    }

    #[staticmethod]
    #[pyo3(signature = (columns, separator=", ", unique=false))]
    fn concat(columns: Vec<String>, separator: &str, unique: bool) -> Self {
        Self {
            kind: AggKind::Concat {
                columns,
                separator: separator.to_string(),
                unique,
            },
        }
    }

    #[staticmethod]
    #[pyo3(signature = (columns, field_separator=":", row_separator=", ", alias=None))]
    fn contribution_breakdown(
        columns: Vec<String>,
        field_separator: &str,
        row_separator: &str,
        alias: Option<String>,
    ) -> Self {
        Self {
            kind: AggKind::ContributionBreakdown {
                columns,
                field_separator: field_separator.to_string(),
                row_separator: row_separator.to_string(),
                alias,
            },
        }
    }
}

/// Apply a list of built-in aggregations to a single group DataFrame.
pub fn apply_builtin_aggregations(
    group: &DataFrame,
    aggregations: &[Aggregation],
) -> Result<Vec<(String, AnyValue<'static>)>, SdtError> {
    let mut results: Vec<(String, AnyValue<'static>)> = Vec::new();

    for agg in aggregations {
        match &agg.kind {
            AggKind::Custom { callable } => {
                Python::with_gil(|py| -> PyResult<()> {
                    let py_df = PyDataFrame(group.clone());
                    let result = callable.call1(py, (py_df,))?;
                    let dict = result.downcast_bound::<PyDict>(py).map_err(|_| {
                        PyValueError::new_err("Custom aggregation must return a dict")
                    })?;
                    for (key, value) in dict.iter() {
                        let name: String = key.extract()?;
                        if let Ok(f) = value.extract::<f64>() {
                            results.push((name, AnyValue::Float64(f)));
                        } else if let Ok(i) = value.extract::<i64>() {
                            results.push((name, AnyValue::Int64(i)));
                        } else if let Ok(s) = value.extract::<String>() {
                            results.push((name, AnyValue::StringOwned(s.into())));
                        } else {
                            results
                                .push((name, AnyValue::StringOwned(format!("{}", value).into())));
                        }
                    }
                    Ok(())
                })
                .map_err(SdtError::from)?;
            }
            AggKind::Min { column, alias } => {
                let s = group.column(column)?.as_materialized_series();
                let name = alias.clone().unwrap_or_else(|| format!("{column}_min"));
                let val = s.min_reduce().map_err(SdtError::from)?;
                let f = val.value().try_extract::<f64>().unwrap_or(f64::NAN);
                results.push((name, AnyValue::Float64(f)));
            }
            AggKind::Max { column, alias } => {
                let s = group.column(column)?.as_materialized_series();
                let name = alias.clone().unwrap_or_else(|| format!("{column}_max"));
                let val = s.max_reduce().map_err(SdtError::from)?;
                let f = val.value().try_extract::<f64>().unwrap_or(f64::NAN);
                results.push((name, AnyValue::Float64(f)));
            }
            AggKind::Sum { columns } => {
                for col in columns {
                    let s = group.column(col)?.as_materialized_series();
                    let val = s.sum_reduce().map_err(SdtError::from)?;
                    let f = val.value().try_extract::<f64>().unwrap_or(0.0);
                    results.push((format!("{col}_sum"), AnyValue::Float64(f)));
                }
            }
            AggKind::Avg { columns } => {
                for col in columns {
                    let s = group.column(col)?.as_materialized_series();
                    let mean = s.mean_reduce();
                    let f = mean.value().try_extract::<f64>().unwrap_or(f64::NAN);
                    results.push((format!("{col}_avg"), AnyValue::Float64(f)));
                }
            }
            AggKind::WeightedSum {
                columns,
                aggregate_by,
                include_calculation: _,
            } => {
                // Direction-aware weighted sum
                let direction_col = group
                    .column(traceability::TRACE_DIRECTION)?
                    .as_materialized_series()
                    .str()?;

                // Pre-fetch all factor columns
                let count_fwd = group
                    .column(factors::SHARE_COUNT_FORWARD)?
                    .as_materialized_series()
                    .f64()?;
                let count_bwd = group
                    .column(factors::SHARE_COUNT_BACKWARD)?
                    .as_materialized_series()
                    .f64()?;
                let biomass_fwd = group
                    .column(factors::SHARE_BIOMASS_FORWARD)?
                    .as_materialized_series()
                    .f64()?;
                let biomass_bwd = group
                    .column(factors::SHARE_BIOMASS_BACKWARD)?
                    .as_materialized_series()
                    .f64()?;

                for col in columns {
                    let v = group.column(col)?.as_materialized_series().f64()?;

                    let mut total: f64 = 0.0;
                    for i in 0..group.height() {
                        let dir = direction_col.get(i).ok_or_else(|| {
                            SdtError::General("Null direction in traced data".into())
                        })?;
                        let value = v.get(i).unwrap_or(0.0);

                        // For WeightedSum (scale-then-sum):
                        // - forward direction uses backward factors
                        // - backward direction uses forward factors
                        let weight = match (dir, aggregate_by) {
                            ("forward", AggregateBy::Count) => count_bwd.get(i).unwrap_or(0.0),
                            ("forward", AggregateBy::Biomass) => biomass_bwd.get(i).unwrap_or(0.0),
                            ("backward", AggregateBy::Count) => count_fwd.get(i).unwrap_or(0.0),
                            ("backward", AggregateBy::Biomass) => {
                                biomass_fwd.get(i).unwrap_or(0.0)
                            }
                            ("identity", _) => 1.0,
                            _ => {
                                return Err(SdtError::General(format!(
                                    "Unknown direction: {}",
                                    dir
                                )))
                            }
                        };

                        total += value * weight;
                    }

                    results.push((col.clone(), AnyValue::Float64(total)));
                }
            }
            AggKind::WeightedAvg {
                column,
                aggregate_by,
            } => {
                // Direction-aware weighted average
                let direction_col = group
                    .column(traceability::TRACE_DIRECTION)?
                    .as_materialized_series()
                    .str()?;

                // Pre-fetch all factor columns
                let count_fwd = group
                    .column(factors::SHARE_COUNT_FORWARD)?
                    .as_materialized_series()
                    .f64()?;
                let count_bwd = group
                    .column(factors::SHARE_COUNT_BACKWARD)?
                    .as_materialized_series()
                    .f64()?;
                let biomass_fwd = group
                    .column(factors::SHARE_BIOMASS_FORWARD)?
                    .as_materialized_series()
                    .f64()?;
                let biomass_bwd = group
                    .column(factors::SHARE_BIOMASS_BACKWARD)?
                    .as_materialized_series()
                    .f64()?;

                let v = group.column(column)?.as_materialized_series().f64()?;

                let mut sum_vw: f64 = 0.0;
                let mut sum_w: f64 = 0.0;

                for i in 0..group.height() {
                    let dir = direction_col.get(i).ok_or_else(|| {
                        SdtError::General("Null direction in traced data".into())
                    })?;
                    let value = v.get(i).unwrap_or(0.0);

                    // For WeightedAvg (true weighted average):
                    // - forward direction uses forward factors
                    // - backward direction uses backward factors
                    let weight = match (dir, aggregate_by) {
                        ("forward", AggregateBy::Count) => count_fwd.get(i).unwrap_or(0.0),
                        ("forward", AggregateBy::Biomass) => biomass_fwd.get(i).unwrap_or(0.0),
                        ("backward", AggregateBy::Count) => count_bwd.get(i).unwrap_or(0.0),
                        ("backward", AggregateBy::Biomass) => biomass_bwd.get(i).unwrap_or(0.0),
                        ("identity", _) => 1.0,
                        _ => {
                            return Err(SdtError::General(format!("Unknown direction: {}", dir)))
                        }
                    };

                    sum_vw += value * weight;
                    sum_w += weight;
                }

                let val = if sum_w > 0.0 {
                    sum_vw / sum_w
                } else {
                    f64::NAN
                };

                results.push((format!("{column}_weighted_avg"), AnyValue::Float64(val)));
            }
            AggKind::Concat {
                columns,
                separator,
                unique,
            } => {
                for col in columns {
                    let s = group.column(col)?.as_materialized_series();
                    let vals: Vec<String> = s.iter().map(|v| format!("{v}")).collect();
                    let result = if *unique {
                        let mut seen = std::collections::HashSet::new();
                        vals.into_iter()
                            .filter(|v| seen.insert(v.clone()))
                            .collect::<Vec<_>>()
                            .join(separator)
                    } else {
                        vals.join(separator)
                    };
                    results.push((col.clone(), AnyValue::StringOwned(result.into())));
                }
            }
            AggKind::ContributionBreakdown {
                columns,
                field_separator,
                row_separator,
                alias,
            } => {
                let height = group.height();
                let series: Vec<&Series> = columns
                    .iter()
                    .map(|c| group.column(c).map(|col| col.as_materialized_series()))
                    .collect::<Result<_, _>>()
                    .map_err(SdtError::from)?;

                let parts: Vec<String> = (0..height)
                    .map(|i| {
                        series
                            .iter()
                            .map(|s| {
                                let val = s.get(i).unwrap();
                                match &val {
                                    AnyValue::String(s) => s.to_string(),
                                    AnyValue::StringOwned(s) => s.to_string(),
                                    other => format!("{other}"),
                                }
                            })
                            .collect::<Vec<_>>()
                            .join(field_separator)
                    })
                    .collect();

                let name = alias
                    .clone()
                    .unwrap_or_else(|| "contribution_breakdown".to_string());
                results.push((
                    name,
                    AnyValue::StringOwned(parts.join(row_separator).into()),
                ));
            }
        }
    }

    Ok(results)
}