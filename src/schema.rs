/// Column-name constants for aqua-tracekit schema.
/// Single source of truth - exported to Python via PyO3.

// ── Transfer columns ────────────────────────────────────────────────────────
pub mod transfer {
    pub const SOURCE_POP_ID: &str = "source_pop_id";
    pub const DEST_POP_ID: &str = "dest_pop_id";
    pub const TRANSFER_COUNT: &str = "transfer_count";
    pub const TRANSFER_BIOMASS_KG: &str = "transfer_biomass_kg";
}

// ── Trace factor columns ────────────────────────────────────────────────────
pub mod factors {
    pub const SHARE_COUNT_FORWARD: &str = "share_count_forward";
    pub const SHARE_BIOMASS_FORWARD: &str = "share_biomass_forward";
    pub const SHARE_COUNT_BACKWARD: &str = "share_count_backward";
    pub const SHARE_BIOMASS_BACKWARD: &str = "share_biomass_backward";

    pub const ALL: [&str; 4] = [
        SHARE_COUNT_FORWARD,
        SHARE_BIOMASS_FORWARD,
        SHARE_COUNT_BACKWARD,
        SHARE_BIOMASS_BACKWARD,
    ];
}

// ── Direction values ────────────────────────────────────────────────────────
pub mod direction {
    pub const IDENTITY: &str = "identity";
    pub const FORWARD: &str = "forward";
    pub const BACKWARD: &str = "backward";
}

// ── Aggregate by  ───────────────────────────────────────────────────────────
pub mod aggregate_by {
    pub const COUNT: &str = "count";
    pub const BIOMASS: &str = "biomass";
}

// ── Population columns ──────────────────────────────────────────────────────
pub mod population {
    pub const POPULATION_ID: &str = "population_id";
    pub const CONTAINER_ID: &str = "container_id";
    pub const START_TIME: &str = "start_time";
    pub const END_TIME: &str = "end_time";
}

// ── Container columns ───────────────────────────────────────────────────────
pub mod container {
    pub const CONTAINER_ID: &str = "container_id";
}

// ── Traceability index columns ──────────────────────────────────────────────
pub mod traceability {
    pub const ORIGIN_POPULATION_ID: &str = "origin_population_id";
    pub const TRACED_POPULATION_ID: &str = "traced_population_id";
    pub const TRACE_DIRECTION: &str = "direction";
}

// ── Time series columns ─────────────────────────────────────────────────────
pub mod timeseries {
    pub const DATE_TIME: &str = "date_time";
}
