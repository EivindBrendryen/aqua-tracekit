use pyo3::prelude::*;
use pyo3::types::PyModule;

mod aggregation;
mod dag_tracer;
mod error;
mod model;
mod schema;

use model::SdtModel;
mod visualization;

/// Export schema constants as Python submodules
fn add_schema_exports(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Transfer
    let transfer = PyModule::new(m.py(), "transfer")?;
    transfer.add("SOURCE_POP_ID", schema::transfer::SOURCE_POP_ID)?;
    transfer.add("DEST_POP_ID", schema::transfer::DEST_POP_ID)?;
    transfer.add("TRANSFER_COUNT", schema::transfer::TRANSFER_COUNT)?;
    transfer.add("TRANSFER_BIOMASS_KG", schema::transfer::TRANSFER_BIOMASS_KG)?;
    m.add_submodule(&transfer)?;

    // Factors
    let factors = PyModule::new(m.py(), "factors")?;
    factors.add("SHARE_COUNT_FORWARD", schema::factors::SHARE_COUNT_FORWARD)?;
    factors.add(
        "SHARE_BIOMASS_FORWARD",
        schema::factors::SHARE_BIOMASS_FORWARD,
    )?;
    factors.add(
        "SHARE_COUNT_BACKWARD",
        schema::factors::SHARE_COUNT_BACKWARD,
    )?;
    factors.add(
        "SHARE_BIOMASS_BACKWARD",
        schema::factors::SHARE_BIOMASS_BACKWARD,
    )?;
    m.add_submodule(&factors)?;

    // Direction
    let direction = PyModule::new(m.py(), "direction")?;
    direction.add("IDENTITY", schema::direction::IDENTITY)?;
    direction.add("FORWARD", schema::direction::FORWARD)?;
    direction.add("BACKWARD", schema::direction::BACKWARD)?;
    m.add_submodule(&direction)?;

    // AggregateBy
    let aggregate_by = PyModule::new(m.py(), "aggregate_by")?;
    aggregate_by.add("COUNT", schema::aggregate_by::COUNT)?;
    aggregate_by.add("BIOMASS", schema::aggregate_by::BIOMASS)?;
    m.add_submodule(&aggregate_by)?;

    // segment
    let segment = PyModule::new(m.py(), "segment")?;
    segment.add("SEGMENT_ID", schema::segment::SEGMENT_ID)?;
    segment.add("CONTAINER_ID", schema::segment::CONTAINER_ID)?;
    segment.add("START_TIME", schema::segment::START_TIME)?;
    segment.add("END_TIME", schema::segment::END_TIME)?;
    m.add_submodule(&segment)?;

    // Container
    let container = PyModule::new(m.py(), "container")?;
    container.add("CONTAINER_ID", schema::container::CONTAINER_ID)?;
    m.add_submodule(&container)?;

    // Traceability
    let traceability = PyModule::new(m.py(), "traceability")?;
    traceability.add(
        "ORIGIN_SEGMENT_ID",
        schema::traceability::ORIGIN_SEGMENT_ID,
    )?;
    traceability.add(
        "TRACED_SEGMENT_ID",
        schema::traceability::TRACED_SEGMENT_ID,
    )?;
    traceability.add("TRACE_DIRECTION", schema::traceability::TRACE_DIRECTION)?;
    m.add_submodule(&traceability)?;

    // TimeSeries
    let timeseries = PyModule::new(m.py(), "timeseries")?;
    timeseries.add("DATE_TIME", schema::timeseries::DATE_TIME)?;
    m.add_submodule(&timeseries)?;

    Ok(())
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {    
    m.add_class::<SdtModel>()?;
    m.add_class::<crate::aggregation::Aggregation>()?;
    add_schema_exports(m)?;
    Ok(())
}
