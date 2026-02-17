use pyo3::exceptions::PyRuntimeError;
use pyo3::PyErr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SdtError {
    #[error("Data not loaded: {0}")]
    NotLoaded(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Polars error: {0}")]
    Polars(#[from] polars::error::PolarsError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    General(String),

    #[error("Missing column: {0}")]
    MissingColumn(String),

    #[error("Validation: {0}")]
    Validation(String),

    #[error("InvalidData: {0}")]
    InvalidData(String),
}

impl From<SdtError> for PyErr {
    fn from(err: SdtError) -> PyErr {
        PyRuntimeError::new_err(err.to_string())
    }
}

impl From<PyErr> for SdtError {
    fn from(err: PyErr) -> Self {
        SdtError::General(err.to_string())
    }
}
