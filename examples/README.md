# Examples

This folder contains a linear set of notebooks for learning `aqua-tracekit`.

All notebooks are intended to be runnable in isolation with data stored in each example folder.

## Recommended Learning Path

| Order | Notebook | Focus |
|------:|----------|-------|
| 1 | [example_01_basics](https://nbviewer.org/github/EivindBrendryen/aqua-tracekit/blob/main/examples/example_01_basics/example_01_basics.ipynb) | Core entities, loading data, mapping container data to segments |
| 2 | [example_02_split](https://nbviewer.org/github/EivindBrendryen/aqua-tracekit/blob/main/examples/example_02_split/example_02_split.ipynb) | Split transfers from one source into multiple destinations |
| 3 | [example_03_mix](https://nbviewer.org/github/EivindBrendryen/aqua-tracekit/blob/main/examples/example_03_mix/example_03_mix.ipynb) | Mix transfers from multiple sources into one destination |
| 4 | [example_04_aggregations](https://nbviewer.org/github/EivindBrendryen/aqua-tracekit/blob/main/examples/example_04_aggregations/example_04_aggregations.ipynb) | Aggregation patterns on traced data |
| 5 | [example_06_trace_forward](https://nbviewer.org/github/EivindBrendryen/aqua-tracekit/blob/main/examples/example_06_trace_forward/example_06_trace_forward.ipynb) | Forward tracing from selected origin segments |
| 6 | [example_20_trace_middle](https://nbviewer.org/github/EivindBrendryen/aqua-tracekit/blob/main/examples/example_20_trace_middle/example_20_trace_middle.ipynb) | Bidirectional tracing from a middle segment |

## Run Locally

From the project root:

```bash
pip install -e ".[examples]"
jupyter notebook
```

Then open notebooks under `examples/...`.

## Reliable Notebook Execution

- Open a notebook and run all cells from top to bottom.
- Keep paths relative to the notebook folder (`Path("data")` pattern).
- If launching from command line, start Jupyter from the project root so `examples/...` paths resolve consistently.