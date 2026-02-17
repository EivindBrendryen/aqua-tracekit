"""aqua-tracekit schema - wraps Rust constants for ergonomic Python API."""

from . import _core as _rust


class TraceFactors:
    """Trace factor column names."""
    SHARE_COUNT_FORWARD = _rust.factors.SHARE_COUNT_FORWARD
    SHARE_BIOMASS_FORWARD = _rust.factors.SHARE_BIOMASS_FORWARD
    SHARE_COUNT_BACKWARD = _rust.factors.SHARE_COUNT_BACKWARD
    SHARE_BIOMASS_BACKWARD = _rust.factors.SHARE_BIOMASS_BACKWARD

    ALL = [
        SHARE_COUNT_FORWARD,
        SHARE_BIOMASS_FORWARD,
        SHARE_COUNT_BACKWARD,
        SHARE_BIOMASS_BACKWARD,
    ]


class Direction:
    """Direction values for traceability."""
    IDENTITY = _rust.direction.IDENTITY
    FORWARD = _rust.direction.FORWARD
    BACKWARD = _rust.direction.BACKWARD

    ALL = [IDENTITY, FORWARD, BACKWARD]

class AggregateBy:
    COUNT = _rust.aggregate_by.COUNT
    BIOMASS = _rust.aggregate_by.BIOMASS

class SdtSchema:
    """Schema constants for aqua-tracekit data model."""

    DIRECTION = Direction
    AGGREGATE_BY = AggregateBy

    class Container:
        """Container/cage column names."""
        CONTAINER_ID = _rust.container.CONTAINER_ID

    class Transfer:
        """Transfer/movement column names."""
        SOURCE_POP_ID = _rust.transfer.SOURCE_POP_ID
        DEST_POP_ID = _rust.transfer.DEST_POP_ID
        TRANSFER_COUNT = _rust.transfer.TRANSFER_COUNT
        TRANSFER_BIOMASS_KG = _rust.transfer.TRANSFER_BIOMASS_KG
        FACTORS = TraceFactors

    class Segment:
        """segment column names."""
        SEGMENT_ID = _rust.segment.SEGMENT_ID
        CONTAINER_ID = _rust.segment.CONTAINER_ID
        START_TIME = _rust.segment.START_TIME
        END_TIME = _rust.segment.END_TIME

    class TraceabilityIndex:
        """Traceability index column names."""
        ORIGIN_SEGMENT_ID = _rust.traceability.ORIGIN_SEGMENT_ID
        TRACED_SEGMENT_ID = _rust.traceability.TRACED_SEGMENT_ID
        TRACE_DIRECTION = _rust.traceability.TRACE_DIRECTION
        FACTORS = TraceFactors

    class TimeSeries:
        """Time series column names."""
        DATE_TIME = _rust.timeseries.DATE_TIME