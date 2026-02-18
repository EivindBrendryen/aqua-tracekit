# Local development

This library has a Rust core compiled to a Python extension using [maturin](https://github.com/PyO3/maturin). To work on the source code you need both Python and Rust installed.

**1. Install Rust**

If you don't have Rust, install it via [rustup](https://rustup.rs) and accept the defaults:
```
https://rustup.rs
```

**2. Clone the repo and create a virtual environment**
```bash
git clone https://github.com/EivindBrendryen/aqua-tracekit.git
cd aqua-tracekit
python -m venv .venv
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # Mac/Linux
```

**3. Install maturin and build**
```bash
pip install maturin
maturin develop
```

This compiles the Rust code and installs the package into your virtual environment. Re-run `maturin develop` after any changes to the Rust source.

**4. Install example dependencies (optional)**
```bash
pip install -e ".[examples]"
jupyter notebook examples/
```

**VS Code tip:** Install the [rust-analyzer](https://marketplace.visualstudio.com/items?itemName=rust-lang.rust-analyzer) extension for Rust code completion and inline errors.
