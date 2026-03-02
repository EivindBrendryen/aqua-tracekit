# SalmoDuck Traceability Model

This document is a working specification for the **SalmoDuck traceability model** as implemented in `aqua-tracekit`.

It describes the core entities, how data connects, and the tracing concepts used for analysis.

---

## 1) Purpose

The model captures how fish move through production over time, across containers, while preserving:

- identity of fish groups (`segments`)
- physical location (`containers`)
- movement/split/mix events (`transfers`)
- facts measured over time (`timeseries`)

The goal is a minimal but practical structure that supports tracing, aggregation, and downstream analytics.

## 2) Core entities

### 2.1 Containers

Represents physical units such as cages, tanks, wellboats, waiting cages, etc.

Required field:

- `container_id`

Any additional columns are considered dimensions/metadata and are preserved.

### 2.2 Segments

Represents a fish group in a specific container during a specific period.

Required fields:

- `segment_id`
- `container_id`
- `start_time`
- `end_time`

Semantics:

- one segment belongs to exactly one container
- segment time interval defines when the fish group is present in that container

### 2.3 Transfers

Represents movement relationships between source and destination segments.

Required identity fields:

- `source_segment_id` (source segment)
- `dest_segment_id` (destination segment)

Quantification can be provided in one of two ways:

1. **Stock values**
   - `transfer_count`
   - `transfer_biomass_kg`

2. **Trace factors**
   - `share_count_forward`
   - `share_biomass_forward`
   - `share_count_backward`
   - `share_biomass_backward`

If factors are missing, factors are computed from stock values within source/destination groups.

### 2.4 Timeseries

Represents facts/events over time (sensor values, mortality, treatments, oxygen, temperature, etc.).

Required time field:

- `date_time`

Timeseries can be linked by either:

- `segment_id` (segment-level timeseries)
- `container_id` (container-level timeseries)

## 3) Relationships

At conceptual level:

- `container` 1 --- n `segment`
- `segment` n --- n `segment` via `transfer`
- `timeseries` references either `segment` or `container`

Transfers form a directed graph over segments. This graph is the basis for traceability indexing.

## 4) Traceability concepts

### 4.1 Directions

The model uses trace directions:

- `IDENTITY`
- `FORWARD`
- `BACKWARD`

### 4.2 Traceability index

Tracing produces index rows containing at least:

- `origin_segment_id`
- `traced_segment_id`
- `trace_direction`
- share factors for count/biomass and forward/backward perspectives

This index can then be joined with timeseries/facts for analysis and aggregation.

### 4.3 Aggregate basis

Aggregations can be weighted by:

- `COUNT`
- `BIOMASS`

## 5) Data rules and parsing assumptions

Current implementation assumptions:

- CSV input is loaded as strings first
- required schema columns are validated on load
- datetime fields are parsed with format `%Y-%m-%d %H:%M:%S`
- numeric transfer/factor columns are cast to numeric types
- transfer rows must end up with complete factor values after load/compute

## 6) Typical workflow

1. Load core data (`containers`, `segments`, `transfers`)
2. Attach optional dimensions
3. Load timeseries (container-level or segment-level)
4. Choose trace origin(s)
5. Choose direction/principle (forward/backward/identity)
6. Generate traceability index
7. Join/aggregate facts for reporting

## 7) Scope notes

- Model is intentionally compact; domain-specific dimensions should live in user data columns.
- The same core pattern supports split and mix transfers.
- The model is intended for practical notebook-based analysis using Polars/Pandas workflows.

## 8) Glossary (working)

- **Container**: Physical production unit where fish are held.
- **Segment**: Time-bounded fish group in a container.
- **Transfer**: Directed movement relation between segments.
- **Trace factor**: Weight/share used to propagate count/biomass through the graph.
- **Traceability index**: Computed mapping from origin segment(s) to traced segment(s).

---

## Status

This is a first draft and should be treated as a living document.