# aqua-tracekit

Lightweight toolkit for basic analysis of aquaculture data in Jupyter notebooks, built around the **SalmoDuck traceability model**.

**Source, examples & discussions:** [github.com/EivindBrendryen/aqua-tracekit](https://github.com/EivindBrendryen/aqua-tracekit)

---

`aqua-tracekit` cuts through the complexity of fish movement across containers (cages, tanks, wellboats, etc.) when performing numerical analysis.
 
The goal is to provide a practical tool as well as contributing to a conceptual baseline for modelling traceability data within aquaculture production.

Some problems are out of scope for this package, but if you have ideas or a relevant problem that you would like to see solved, feel free to start a [discussion here](https://github.com/EivindBrendryen/aqua-tracekit/discussions) and it might just be included in the next version :)

## Installation
```bash
pip install aqua-tracekit
```

To run the example notebooks locally:
```bash
pip install aqua-tracekit[examples]
```

## Quick Example
```python
from aqua_tracekit import SdtModel, SdtSchema
from IPython.display import HTML

model = SdtModel(base_path="path/to/data")
# load the core model
model.load_containers("containers.csv")
model.load_segments("segments.csv")
model.load_transfers("transfers.csv")

# Visualize the movement of fish across containers
HTML(model.visualize_trace())
```

## Features

The **SalmoDuck traceability model** is a core data model containing enough structure (but as little as possible) to capture traceability as fish is moved, split or mixed across containers of any kind.

The model's core entities are:

- **Fishgroup segments** — a group of fish in a container over a time period
- **Containers** — cages, tanks, wellboats, waiting cages, etc.
- **Transfers** — splits, mixes, and moves between segments
- **Timeseries** — sensor readings, events (vaccination, stocking, harvest), linked to containers or segments

Dimensional data can be freely attached to these entities, and they will follow the data frames through the processing, ready to be used in the analysis once data is traced according to the requirements of the use-case. Examples: 

- containers: container_name, site_id
- segments: fish_group_name, global_gap_number, species, generation
- transfers: type_of_transfer, transfer_operation_id

Adding refences (id's) is the preferred way to relate to larger structures. This increases performance.

Fact data is modelled as timeseries data (even if the fact is just a single record, like a vaccination). All operations on facts is done using the time series support in the toolkit.

Timeseries comes in 2 flavours; referencing containers or segments. Typically some data stems from production control systems and is already referencing the segments. Data from other systems is typically referencing the containers. 

Key to the analysis is deciding on a tracing principle and selecting the origin of the traces. 
A traceability index is then computed, forming the basis for further analysis and aggregations. 

The model includes a built-in interactive visualization of fish movement.

Most things here apply to both roe/egg and fish, but for simplicity we use the term
fish about the product. The toolkit should be valuable for the whole value chain,
at any resolution.

A typical use case:

- load core data (segments, transfers, containers)
- attach dimensions if needed
- load timeseries data (referencing either containers or segments)
- decide on a tracing principle 
- decide on the trace origin
- generate the trace(s)
- output the timeseries for the trace(s)

The process lends itself to easy data processing, filtering, aggregations and more through the use of Polars data frames. Conversion to Pandas is as easy as .to_pandas()

## Examples

Jupyter notebooks covering common scenarios:

| Example | Description |
|---------|-------------|
| [Basics](https://nbviewer.org/github/EivindBrendryen/aqua-tracekit/blob/main/examples/example_1_basics/example_1_basics.ipynb) | Core data model, loading data, basic operations |
| [Split transfers](https://nbviewer.org/github/EivindBrendryen/aqua-tracekit/blob/main/examples/example_2_split/example_2_split.ipynb) | One segment split across multiple destinations |
| [Mix transfers](https://nbviewer.org/github/EivindBrendryen/aqua-tracekit/blob/main/examples/example_3_mix/example_3_mix.ipynb) | Multiple sources mixed into one container |
| [Aggregations](https://nbviewer.org/github/EivindBrendryen/aqua-tracekit/blob/main/examples/example_4_aggregations/example_4_aggregations.ipynb) | Statistics grouped by origin, time, or custom dimensions |
| [Forward tracing](https://nbviewer.org/github/EivindBrendryen/aqua-tracekit/blob/main/examples/example_6_trace_forward/example_6_trace_forward.ipynb) | Trace forward to see where fish end up |
| [Trace from middle](https://nbviewer.org/github/EivindBrendryen/aqua-tracekit/blob/main/examples/example_20_trace_middle/example_20_trace_middle.ipynb) | Trace both directions from a mid-chain segment |

## License

MIT — see [LICENSE](./LICENSE) for details.

## Author

Eivind Brendryen

## Questions?

- Bug reports and feature requests: [Issues](https://github.com/EivindBrendryen/aqua-tracekit/issues)
- General discussion and advanced topics: [Discussions](https://github.com/EivindBrendryen/aqua-tracekit/discussions)