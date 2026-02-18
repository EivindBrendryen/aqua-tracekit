# aqua-tracekit

Lightweight traceability toolkit for aquaculture data analysis in Jupyter notebooks.

This repo contains the toolkit, examples and documentation. The goal is to provide tools, but also give a better understanding of traceability and demonstrate how some of the typical problems can be solved quite easily. The hard problems are outside the scope of this project, but feel free to start a [discussion](https://github.com/EivindBrendryen/aqua-tracekit/discussions) about those :)

Most thing here applies to both roe/egg and fish, but for simplicity we use the term fish about the product.

The toolkit should be valuable for the whole value chain, at any resolution, as far as I can tell.

## Features

The toolkit is built around a core data model, containing enough structure (but little as possible) to capture traceability as fish moves across sites, tanks, cages, wellboats, waiting cages etc. (between any container at any place). 

All data loading is based on CSV files.

The model's core entities are
* Fishgroup segments 
* Containers
* Transfers (split, mix and move) of fish 
* TimeSeries
 
Custom metadata can easily be attached to these entities, for example by adding a site id to the container entity, a sensor tag to the timeseries, a fishgroup name to the segmens etc.

The model has features for loading timeseries with:
* time stamp
* relation either to container or fishgroup segment
* data

Timeseries are events, and they can contain events that happens infrequent (like vaccination, stocking, harvest) or frequent (like a sensor reading). The actual timeseries data can be whatever, but built-in features is based on numerical values. 

Timeseries related to containers is easily mapped to fishgroup segments before tracing.

The model has a simple built-in visualization of the movement of fish.

Once a trace origin is decided, a traceability index is generated. This forms the basis for further calculations and analysis. 


## Installation

**Basic installation:**
```bash
pip install aqua-tracekit
```

**To run the examples locally:**
```bash
pip install aqua-tracekit[examples]
jupyter notebook examples/
```

Or open the example notebooks in any notebook environment (VS Code, JupyterLab, Google Colab, Binder).

## Development Setup

If you want to work with the source code, here is [a local development setup guide](docs/DEVELOPMENT.md).

You do **not** have to do this if you just want to work on notebook examples


## Documentation
TODO: Link to docs.
See the [examples](./examples) folder for Jupyter notebooks demonstrating usage.

## License
MIT License - see [LICENSE](./LICENSE) file for details.

## Author
Eivind Brendryen

## Questions?
- Bug reports and feature requests: [Issues](https://github.com/EivindBrendryen/aqua-tracekit/issues)
- General discussion and advanced topics: [Discussions](https://github.com/EivindBrendryen/aqua-tracekit/discussions)
