# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**fluxus** is a Python framework for building complex data processing pipelines (called *flows*). It enables highly concurrent workflows using a functional API inspired by the data stream paradigm.

Key concepts:
- **Flow**: A Directed Acyclic Graph (DAG) where each node is a conduit
- **Conduits**: Building blocks that can be Producers, Transformers, or Consumers
- **Products**: Data elements that move through the flow
- **Lineage**: Complete trace of inputs, intermediate results, and outputs through all paths in a flow

The framework supports both functional API (simple) and object-oriented API (advanced) approaches.

## Development Commands

### Environment Setup
```bash
# Create and activate virtual environment
python -m venv fluxus-env
source fluxus-env/bin/activate  # Mac/Linux
# or: .\fluxus\scripts\Activate  # Windows

# Install in developer mode with dev dependencies
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install
```

### Testing
```bash
# Run all tests
pytest

# Run tests in a specific file
pytest test/fluxus_test/test_flow.py

# Run tests with coverage report
pytest --cov=fluxus --cov-report=html

# Run specific test by name
pytest test/fluxus_test/test_flow.py::test_name -v

# Run tests verbosely
pytest -v -s
```

Test coverage must be at least 90% for the test suite to pass.

### Code Quality
```bash
# Run pre-commit hooks manually
pre-commit run

# Run pre-commit on all files
pre-commit run --all-files

# Format code with black
black src/ test/

# Sort imports with isort
isort src/ test/

# Type checking with mypy
mypy src/ test/

# Lint with flake8
flake8 --config tox.ini src/ test/
```

### Building

The project uses a custom build script (`make.py`) that wraps conda-build and tox:

```bash
# Build with conda (default dependencies)
./make.py fluxus conda default

# Build with tox (default dependencies)
./make.py fluxus tox default

# Build with minimum dependencies (for matrix testing)
./make.py fluxus tox min

# Build with maximum dependencies (for matrix testing)
./make.py fluxus tox max
```

### Documentation

Documentation is built using Sphinx:

```bash
# Build documentation
cd sphinx
./make.py html

# Clean documentation build
./make.py clean
```

Requirements for building docs:
- **Pandoc**: Required to render Jupyter notebooks
- **GraphViz**: Required for flow diagram visualizations

## Architecture

### Core Package Structure

```
src/fluxus/
├── core/           # Base classes and conduit implementations
│   ├── producer/   # Producer conduits (data sources)
│   ├── transformer/ # Transformer conduits (data processing)
│   └── _conduit.py # Base Conduit class
├── functional/     # Functional API (simple, user-facing)
│   ├── conduit/    # Functional conduit wrappers
│   └── product/    # Product handling
├── lineage/        # Lineage tracking and labels
├── viz/            # Visualization (flow diagrams, timelines)
├── simple/         # Simple flow definitions
└── util/           # Utilities
```

### Key Classes and Inheritance

**Conduit Hierarchy** (in `core/`):
- `Conduit` (abstract base): Represents any element of a flow
  - `AtomicConduit`: Single, non-composite conduits
  - `SerialConduit`: Sequential composition of conduits (via `>>` operator)
  - `ConcurrentConduit`: Parallel composition of conduits (via `&` operator)

**Specialized Conduits**:
- `Producer`: Generates or retrieves data (entry points)
- `Transformer`: Processes data from producers/transformers
- `Consumer`: Consumes final data (exactly one per flow)
- `Passthrough`: Special conduit that passes input unchanged

Each has sync and async variants (`AsyncProducer`, `AsyncTransformer`, `AsyncConsumer`).

### Functional API

The functional API (in `functional/`) provides a simpler interface:
- `step(name, function)`: Create a processing step
- `chain()` or `>>` operator: Compose steps sequentially
- `parallel()` or `&` operator: Compose steps in parallel
- `passthrough()`: Pass data unchanged
- `run(flow, input)`: Execute a flow and return `RunResult`

The functional API automatically creates consumers and handles product passing.

### Flow Execution

1. **Product Flow**: Data moves through conduits as products (typically dicts)
2. **Concurrency**: Uses Python's asyncio for concurrent processing
3. **Buffering**: Producers can be buffered to allow multiple concurrent paths
4. **Lineage Tracking**: Complete history preserved through all transformations

### Type System

- Extensive use of `TypeVar` with covariant/contravariant annotations
- Convention: `_ret` suffix for covariant return types, `_arg` for contravariant arguments
- Heavy use of generics for type-safe conduit composition

## Important Implementation Details

### Products and Naming

- Products should be dictionaries to support lineage tracking
- Each step should yield or return `dict[str, Any]` containing results
- Keys in product dicts become part of the lineage trace

### Operator Overloading

- `>>` operator: Sequential composition (creates `SerialConduit`)
- `&` operator: Parallel composition (creates `ConcurrentConduit`)
- Both operators are chainable and associative

### Async Support

- All conduits support both sync and async execution
- Use `run()` for sync, `arun()` for async
- Functions can be sync or async; framework handles both transparently
- Iterator and AsyncIterator both supported for multi-output steps

### Dependency Matrix Testing

The project uses a build matrix (defined in `pyproject.toml`):
- `default`: Standard dependencies from `requires`
- `min`: Minimum versions for compatibility testing
- `max`: Maximum versions for forward compatibility

Environment variables like `FLUXUS_V_PANDAS` are used to inject specific versions during matrix builds.

## Branch Strategy

- **Main branch**: `1.0.x` (use this for PRs)
- **Current development branch**: `2.0.x`
- Version format: `major.minor.patch` or `major.minorrcN` for release candidates

## Testing Guidelines

- Tests are in `test/fluxus_test/`
- Use pytest fixtures defined in `conftest.py`
- Test async code with `pytest-asyncio`
- Minimum 90% code coverage required
- Test naming convention: `test_<feature>.py`

## Pre-commit Hooks

Configured in `.pre-commit-config.yaml`:
- **pyupgrade**: Upgrade Python syntax to 3.10+
- **isort**: Sort imports
- **black**: Format code (line length 88)
- **flake8**: Lint code
- **mypy**: Type checking (strict mode enabled)
- **nbstripout**: Strip notebook outputs
- **check-change-in-file-size**: Custom hook to detect large file changes

## Build System

The `make.py` script:
- Reads dependency specifications from `pyproject.toml`
- Exports versions as environment variables (`FLUXUS_V_*`)
- Supports both conda and tox builds
- Validates release versions against PyPI
- Creates local PyPI index for tox builds

## Dependencies

Core runtime dependencies:
- `matplotlib ~=3.6`
- `pandas ~=2.1`
- `gamma-pytools ~=3.0` (BCG's Python utilities)
- `typing_inspect ~=0.7`

Python version: `>=3.10,<4a`

## Common Patterns

### Creating a New Conduit

When adding new conduit types, inherit from the appropriate base class and implement:
- `_get_init_params()`: For expression representation
- `run()` / `arun()`: For execution
- `__rshift__` and `__and__`: Inherited from `Conduit` for operator support

### Adding Tests

1. Create test file in `test/fluxus_test/test_<feature>.py`
2. Use async test functions with `@pytest.mark.asyncio` decorator
3. Leverage fixtures from `conftest.py`
4. Ensure coverage of both sync and async paths
5. Test edge cases and error handling

### Documentation

- Docstrings use reStructuredText format
- API docs auto-generated via Sphinx autodoc
- User guide and tutorials in `sphinx/source/`
- Use type hints in function signatures (included in docs via sphinx-autodoc-typehints)