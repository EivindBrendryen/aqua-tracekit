# Examples

These examples demonstrate how to use aqua-tracekit for aquaculture traceability analysis.

Each example includes sample data and a Jupyter notebook you can run locally or view online.

## Running Examples Locally
```bash
pip install aqua-tracekit[examples]
cd examples
jupyter notebook
```

## Example 1: Basics

**Description:** Introduction to the core data model - fishgroup segments, containers, transfers, and timeseries. Learn how to load data and perform basic operations.

- **View:** [nbviewer](https://nbviewer.org/github/EivindBrendryen/aqua-tracekit/blob/main/examples/example_1_basics/example_1_basics.ipynb)
- **Run:** `examples/example_1_basics/example_1_basics.ipynb`

## Example 2: Split Transfers

**Description:** Track fish fishgroup segments through split transfers where one population is divided between multiple destination containers.

- **View:** [nbviewer](https://nbviewer.org/github/EivindBrendryen/aqua-tracekit/blob/main/examples/example_2_split/example_2_split.ipynb)
- **Run:** `examples/example_2_split/example_2_split.ipynb`

## Example 3: Mix Transfers

**Description:** Handle scenarios where multiple source fishgroup segments are mixed into a single destination container.

- **View:** [nbviewer](https://nbviewer.org/github/EivindBrendryen/aqua-tracekit/blob/main/examples/example_3_mix/example_3_mix.ipynb)
- **Run:** `examples/example_3_mix/example_3_mix.ipynb`

## Example 4: Aggregations

**Description:** Perform aggregations on traced data - compute statistics grouped by origin, time periods, or custom dimensions.

- **View:** [nbviewer](https://nbviewer.org/github/EivindBrendryen/aqua-tracekit/blob/main/examples/example_4_aggregations/example_4_aggregations.ipynb)
- **Run:** `examples/example_4_aggregations/example_4_aggregations.ipynb`

## Example 6: Forward Tracing

**Description:** Trace forward from a starting population to see where fish end up across the production chain.

- **View:** [nbviewer](https://nbviewer.org/github/EivindBrendryen/aqua-tracekit/blob/main/examples/example_6_trace_forward/example_6_trace_forward.ipynb)
- **Run:** `examples/example_6_trace_forward/example_6_trace_forward.ipynb`

## Example 20: Trace from Middle

**Description:** Start tracing from a population in the middle of the production chain - trace both backwards to origins and forwards to destinations.

- **View:** [nbviewer](https://nbviewer.org/github/EivindBrendryen/aqua-tracekit/blob/main/examples/example_20_trace_middle/example_20_trace_middle.ipynb)
- **Run:** `examples/example_20_trace_middle/example_20_trace_middle.ipynb`