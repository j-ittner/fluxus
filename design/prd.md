termmd. # DSL for Asynchronous Flow Composition in Python

This is the new design for **fluxus 2.0** with **full backwards compatibility** with fluxus 1.0.

## 1. Purpose and Scope

### 1.1 What is Fluxus?

Fluxus is a small, composable DSL in Python for building **asynchronous, lazily evaluated dataflow pipelines**. Think of it as a way to chain together data processing steps where:
- Each step receives a dictionary and produces a dictionary
- Steps run concurrently when possible
- The framework tracks complete lineage of data through the pipeline

### 1.2 Core Building Blocks (in order of complexity)

#### Records
A **record** is simply a dictionary representing data at a point in your pipeline:
```python
record = {"name": "Alice", "age": 30}
```

**Type Safety and Validation (Recommended):**
For simplicity, fluxus does not bake type validation into the engine—records are just `dict[str, object]`. However, **we recommend using Pydantic at runtime** to validate record schemas:

```python
from pydantic import BaseModel, ValidationError

class UserRecord(BaseModel):
    name: str
    age: int

def validate_user(name: str, age: int) -> dict[str, UserRecord]:
    # Pydantic validates at runtime
    return UserRecord(name=name, age=age)

validated_step = step("validate", validate_user)
```

This keeps the fluxus core lightweight while giving you production-grade validation where you need it.

#### Steps
A **step** is a single processing unit—a Python function wrapped to work in the pipeline:
```python
from fluxus.functional import step

# Define a step: takes a record, returns a record
def add_greeting(name: str) -> dict[str, object]:
    return {"greeting": f"Hello, {name}!"}

greet_step = step("greet", add_greeting)
```

#### Conduits
A **conduit** is anything that can process records—individual steps or compositions of steps. Steps are conduits, and when you combine steps using operators, you create new conduits.

### 1.3 Building Pipelines: Two Equivalent Styles

Fluxus supports two styles for building pipelines that are completely interchangeable:

#### Style 1: Operator-Based (Concise)
Use `>>` for sequence, `&` for parallel, `~` for merge:

```python
from fluxus.functional import step, passthrough

# Define steps
load = step("load", lambda: {"data": [1, 2, 3]})
double = step("double", lambda data: {"doubled": [x * 2 for x in data]})
triple = step("triple", lambda data: {"tripled": [x * 3 for x in data]})
summarize = step("summarize", lambda doubled, tripled: {
    "summary": f"Doubled: {doubled}, Tripled: {tripled}"
})

# Sequential: load, then double
pipeline1 = load >> double

# Parallel: double and triple in parallel, then summarize both results
pipeline2 = load >> ~(double & triple) >> summarize

# Parallel with passthrough: process in parallel, keep original data
pipeline3 = load >> (double & passthrough()) >> summarize
```

#### Style 2: Functional API (Explicit)
Use `chain()`, `parallel()`, `merge()` for the same logic:

```python
from fluxus.functional import step, chain, parallel, merge, passthrough

# Same steps as above
load = step("load", lambda: {"data": [1, 2, 3]})
double = step("double", lambda data: {"doubled": [x * 2 for x in data]})
triple = step("triple", lambda data: {"tripled": [x * 3 for x in data]})
summarize = step("summarize", lambda doubled, tripled: {
    "summary": f"Doubled: {doubled}, Tripled: {tripled}"
})

# Sequential: load, then double
pipeline1 = chain(load, double)

# Parallel: double and triple in parallel, then summarize both results
pipeline2 = chain(
    load,
    merge(parallel(double, triple)),
    summarize
)

# Parallel with passthrough: process in parallel, keep original data
pipeline3 = chain(
    load,
    parallel(double, passthrough()),
    summarize
)
```

Both styles are **exactly equivalent**—use whichever feels more natural for your use case.

### 1.4 Executing Pipelines

Once you've built a conduit, execute it:

```python
from fluxus.functional import run

# Synchronous execution (1.0 compatibility)
result = run(pipeline2)

# Or asynchronous execution
result = await pipeline2.arun()

# Access outputs
for output in result.get_outputs():
    print(output["summary"])

# Or as DataFrame
df = result.to_frame()
```

### 1.5 Design Principles (inspired by Haskell)

- **Compositionality**: Conduits compose from smaller conduits using a small set of combinators (`>>`, `&`, `~` or their functional equivalents)
- **Referential transparency**: Conduit definitions are pure descriptions; execution happens only when you call `run()` or `arun()`
- **Lazy evaluation**: Merged results and intermediate keys are computed on demand
- **Predictable behaviour**: Type hints and clear runtime semantics make behavior obvious
- **Backwards compatibility**: Full compatibility with fluxus 1.0 API and naming—existing code continues to work

### 1.6 What Fluxus 2.0 Adds

New features in 2.0 (all backwards-compatible):
- **Merge operator** (`~` / `merge()`): Combine parallel branches into a single record
- **Loop combinator** (`loop()`): Controlled iteration until a condition is met
- **Enhanced lineage**: Track complete data provenance through complex pipelines
- **Simplified type system**: All records are dictionaries (no more generic types)

### 1.7 Non-Goals (for initial version)

- Full streaming transport (backpressure, networked streams, etc.)
- Arbitrary cyclic graphs (focus on DAG-like structures; `loop` combinator provides controlled cycles)
- Sophisticated scheduling or cluster execution
- Breaking changes from fluxus 1.0 (all existing code must continue to work)

---

## 2. Core Concepts and Terminology

- **Record**: a `dict[str, object]` representing the state at a given point in the pipeline.
- **Conduit**:
  - A compositional description of how steps and subconduits are combined.
  - Can be constructed via operators (`>>`, `&`, `~`) or functional combinators (`chain`, `parallel`, `merge`, `loop`).
  - Maintains backwards compatibility with fluxus 1.0 naming.
- **Step**:
  - An atomic `Conduit` wrapping a Python callable.
  - Callable signature is inspected to resolve inputs from the record.
  - Callable must return a `dict[str, object]`.
- **Producer/Transformer/Consumer**:
  - Facade classes for backwards compatibility with fluxus 1.0.
  - Advanced users can subclass these for custom conduits.
  - Most users interact via the functional API (`step()`, `chain()`, `parallel()`).
- **RunResult**:
  - Encapsulates all outputs and complete lineage for each parallel result.
  - Provides introspection APIs.

---

## 3. Type Model and Public API

Target: Python 3.12+ typing style (builtins as generics, no capitalised type names for builtins).

### 3.1 Core Type Aliases

```python
from collections.abc import Awaitable, Callable, Coroutine, AsyncIterator
from dataclasses import dataclass
from typing import Protocol, TypedDict, runtime_checkable

Record = dict[str, object]
Records = list[Record]

@runtime_checkable
class StepFn(Protocol):
    def __call__(self, *args, **kwargs) -> Record | Awaitable[Record]: ...
```

### 3.2 Conduit and Step base classes

```python
from abc import ABC, abstractmethod
import asyncio


class Conduit(ABC):
    """Abstract base for all conduits (steps and composites).

    Maintains backwards compatibility with fluxus 1.0 naming and API.
    """

    name: str

    def run(self) -> "RunResult":
        """Execute this conduit synchronously (fluxus 1.0 compatibility).

        Wraps arun() for backwards compatibility with existing sync code.
        """
        return asyncio.run(self.arun())

    @abstractmethod
    async def arun(
        self,
        inputs: list[Record] | None = None,
        *,
        concurrency: int | None = None,
    ) -> "RunResult":
        """Execute this conduit asynchronously over a list of input records.

        Parameters:
            inputs: Optional list of input records. If None, the conduit
                    generates its own inputs (pull model for Producer facades).
                    If provided, inputs are pushed to the conduit.
            concurrency: Maximum concurrent step invocations, or None for default.

        Returns:
            RunResult with all outputs and lineage.
        """
        ...

    # Operator DSL
    def __rshift__(self, other: "Conduit") -> "Conduit": ...  # sequence
    def __and__(self, other: "Conduit") -> "Conduit": ...     # parallel branch
    def __invert__(self) -> "Conduit": ...                     # merge scope

    def draw(self, style: str = "graph") -> Any:
        """Visualize the conduit composition graph.

        Backwards compatibility with fluxus 1.0 visualization.
        """
        ...


@dataclass(slots=True)
class Step(Conduit):
    """Atomic conduit wrapping a Python callable.

    A Step *is a* Conduit and participates in all Conduit compositions.
    """

    fn: StepFn
    name: str | None = None

    def __post_init__(self) -> None:
        if self.name is None:
            self.name = getattr(self.fn, "__name__", f"step_{id(self):x}")

    async def arun(
        self,
        inputs: list[Record] | None = None,
        *,
        concurrency: int | None = None,
    ) -> "RunResult":
        ...
```

### 3.3 Step construction helper

```python
def step(name: str | None, fn: Callable, **kwargs) -> Conduit:
    """Construct a Step from a callable.

    Maintains fluxus 1.0 signature: step(name, fn, **kwargs)

    Parameters:
        name: Optional step name. If not provided, defaults to fn.__name__
              or a generated identifier.
        fn: The callable to wrap. May be synchronous (returns Record) or
            asynchronous (returns Awaitable[Record]).
        **kwargs: Additional arguments for backwards compatibility.

    Returns:
        A Conduit (Step) wrapping the callable.

    Notes:
        - The return value of fn must be a dict[str, object]; otherwise TypeError is raised.
        - Supports both sync and async callables.
    """
    return Step(fn=fn, name=name)
```

### 3.4 Producer/Transformer/Consumer Facade Classes (Backwards Compatibility)

For backwards compatibility with fluxus 1.0, the framework provides facade classes that advanced users can subclass:

```python
class Producer(Conduit):
    """Facade for conduits that generate data (entry points).

    Backwards compatibility with fluxus 1.0 Producer API.
    """

    def produce(self) -> Iterator[Record]:
        """Generate records synchronously."""
        ...

    async def aproduce(self) -> AsyncIterator[Record]:
        """Generate records asynchronously."""
        ...

    # Delegates to internal Conduit.arun() implementation


class Transformer(Conduit):
    """Facade for conduits that process data.

    Backwards compatibility with fluxus 1.0 Transformer API.
    """

    def transform(self, input: Record) -> Iterator[Record]:
        """Transform a record synchronously."""
        ...

    async def atransform(self, input: Record) -> AsyncIterator[Record]:
        """Transform a record asynchronously."""
        ...

    # Delegates to internal Conduit.arun() implementation


class Consumer(Conduit):
    """Facade for terminal conduits.

    Backwards compatibility with fluxus 1.0 Consumer API.
    """

    def consume(self, products: Iterable[tuple[int, Record]]) -> Any:
        """Consume final products synchronously."""
        ...

    async def aconsume(self, products: AsyncIterable[tuple[int, Record]]) -> Any:
        """Consume final products asynchronously."""
        ...

    # Delegates to internal Conduit.arun() implementation
```

**Note:** Most users interact via the functional API (`step()`, `chain()`, `parallel()`) and never need to use these classes directly. These facades exist for advanced users who need custom conduit implementations.

---

## 4. Conduit Construction – Operator DSL

Conduits are built from `Conduit` (including `Step`) objects using operators.

### 4.1 Sequence: `>>`

```python
Conduit.__rshift__(self, other: Conduit) -> Conduit
```

- `a >> b` means: run `a` first, then use its outputs as inputs to `b`.
- Semantics:
  - For each incoming record:
    - Apply `a` (may produce `0..n` records).
    - For each resulting record, apply `b`.
- This defines sequential composition.
- **Backwards compatible**: Same semantics as fluxus 1.0 `>>` operator.

### 4.2 Parallel Branching: `&`

```python
Conduit.__and__(self, other: Conduit) -> Conduit
```

- `a & b` means: run `a` and `b` in parallel for each incoming record.
- Semantics:
  - For each incoming record `r`:
    - Feed `r` into `a` and `b`, each in their own branch.
    - Each branch produces `0..n` records independently.
  - The conduit now represents multiple concurrent branches per original record.
- **Backwards compatible**: Same semantics as fluxus 1.0 `&` operator.

### 4.3 Merge / Join Scope: unary `~`

```python
Conduit.__invert__(self) -> Conduit
```

- `~f` means: merge all concurrent results created within `f`'s branching structure into a single record per original input.
- **New feature in 2.0**: Not present in fluxus 1.0.

Example:

```python
s1 = step("add", lambda x, y=2: {"y": x + y})
s2 = step("times", lambda x: {"y": x * 5})

conduit = s1 & ~((s1 & s2) >> s1)

inputs = [{"x": 1}, {"x": 2}, {"x": 3}]
result = await conduit.arun(inputs)
# Or synchronously: result = conduit.run()
```

Interpretation:

- Start with the input records.
- `s1 & (...)` creates two branches per record.
- Inside the `~(...)` scope the branches `(s1 & s2) >> s1` run in parallel.
- `~` merges all concurrent results within its scope into a single merged record per original input.

---

## 5. Conduit Construction – Functional DSL

To provide a clear, functional alternative for any operator expression, the DSL exposes explicit combinators.

**Backwards compatibility**: Maintains fluxus 1.0 functional API naming.

### 5.1 `chain(...)`

```python
def chain(*conduits: Conduit) -> Conduit:
    """Sequentially compose all arguments.

    chain(a, b, c) is equivalent to ((a >> b) >> c).

    Backwards compatible with fluxus 1.0.
    """
```

### 5.2 `parallel(...)`

```python
def parallel(*conduits: Conduit) -> Conduit:
    """Parallel branch composition.

    parallel(a, b, c) is equivalent to (a & b & c).

    Backwards compatible with fluxus 1.0 (was called 'parallel' in 1.0, not 'branch').
    """
```

### 5.3 `passthrough()`

```python
def passthrough() -> Conduit:
    """Return a transparent conduit that passes input unchanged.

    Backwards compatibility with fluxus 1.0.

    Useful in parallel compositions where one branch should pass through:
        parallel(transform1, transform2, passthrough())
    """
```

### 5.4 `merge(...)`

```python
def merge(conduit: Conduit) -> Conduit:
    """Merge all concurrent results created inside `conduit`'s scope.

    Equivalent to `~conduit`.

    New feature in 2.0.
    """
```

### 5.5 `loop(...)`

```python
ConditionFn = Callable[[Record], bool]


def loop(*, until: ConditionFn, conduit: Conduit) -> Conduit:
    """Repeatedly apply `conduit` to each record until `until(record)` is true.

    New feature in 2.0.

    Semantics (per record):
    - Start from the incoming record.
    - While `not until(current_record)`:
        - Apply `conduit` to `[current_record]`.
        - If `conduit` yields no records, terminate the loop for this record.
        - If `conduit` yields `>1` records, this creates branches *within the loop*.
          All branches continue looping independently until `until` holds
          for their current record.
    - Records for which `until` eventually returns `True` are emitted.

    Behaviour:
    - `loop` introduces controlled cycles in an otherwise DAG-like graph.
    - Internally, the implementation must prevent infinite loops, e.g. by
      allowing an optional `max_iterations` parameter in the future.
    """
```

### 5.6 `run(...)` - Standalone Execution Function

```python
def run(conduit: Conduit, input: Any = None, timestamps: bool = False) -> RunResult:
    """Execute a conduit and return results.

    Backwards compatibility with fluxus 1.0 functional API.

    Parameters:
        conduit: The conduit to execute
        input: Optional input data (for push model). If None, conduit generates its own inputs.
        timestamps: Whether to track execution timestamps

    Returns:
        RunResult with outputs and lineage
    """
```

### 5.7 Example mapping between styles

Given the operator-style example:

```python
conduit = s1 & ~((s1 & s2) >> s1)
```

A functional equivalent is:

```python
conduit = parallel(
    s1,
    merge(
        chain(
            parallel(s1, s2),
            s1,
        ),
    ),
)
```

Requirements:

- Every operator-based conduit must have a functional equivalent via `parallel`, `chain`, `merge`, and optionally `loop`.
- The library should document that equivalence and keep the semantics aligned.

---

## 6. Execution API and RunResult

### 6.1 Execution API on Conduit

```python
class Conduit:
    name: str

    def run(self) -> "RunResult":
        """Synchronous execution (fluxus 1.0 compatibility).

        Wraps arun() for backwards compatibility.
        """
        return asyncio.run(self.arun())

    async def arun(
        self,
        inputs: list[Record] | None = None,
        *,
        concurrency: int | None = None,
    ) -> "RunResult":
        ...
```

- `inputs` is an optional list of initial records:
  - If `None`, the conduit generates its own inputs (pull model, for Producer facades)
  - If provided, inputs are pushed to the conduit (push model, for Step-based flows)
- `concurrency` controls the maximum number of concurrent step invocations (or `None` for default behaviour).
- The return value is a single `RunResult` (fully materialised outputs).

**Backwards compatibility notes:**
- Both `run()` (sync) and `arun()` (async) are provided
- `inputs` parameter is optional to support both pull and push models

### 6.2 RunResult and Lineage

```python
from typing import NamedTuple, Iterator
import pandas as pd


class StepOutput(NamedTuple):
    step: Step
    output: Record


class Lineage:
    """Lineage for a single final record."""

    def as_list(self) -> list[StepOutput]:
        """Return an ordered list of (step, output) tuples."""
        ...

    def as_dict_by_step(self) -> dict[Step, list[Record]]:
        """Group outputs by `Step` object, returning a mapping from each `Step` to the list of its output records."""
        ...


class RunResult:
    """Execution result for a conduit.

    Contains final outputs and full lineage for each final record.

    Provides both fluxus 2.0 API (lineage_for, all_lineages) and
    fluxus 1.0 backwards compatible API (get_outputs, to_frame, draw_timeline).
    """

    final: list[Record]

    # === New 2.0 API ===

    def lineage_for(self, index: int) -> Lineage:
        """Return lineage for `final[index]`.

        New in fluxus 2.0.
        """
        ...

    def all_lineages(self) -> list[Lineage]:
        """Return lineages for all final records.

        New in fluxus 2.0.
        """
        ...

    # === Backwards Compatible 1.0 API ===

    def get_outputs(self) -> Iterator[Record]:
        """Iterate over all final outputs.

        Backwards compatible with fluxus 1.0.

        Equivalent to: iter(self.final)
        """
        return iter(self.final)

    def get_outputs_per_path(self) -> list[Iterator[Record]]:
        """Get outputs grouped by parallel path.

        Backwards compatible with fluxus 1.0.

        Returns a list of iterators, one per parallel branch in the conduit.
        """
        ...

    def to_frame(self, path: int | None = None, simplify: bool = False) -> pd.DataFrame:
        """Convert results to pandas DataFrame.

        Backwards compatible with fluxus 1.0.

        Parameters:
            path: Optional path index to convert. If None, converts all paths.
            simplify: Whether to simplify the DataFrame structure.

        Returns:
            pandas DataFrame with results and lineage.
        """
        ...

    def draw_timeline(self, style: str = "matplot", out: Any = None):
        """Visualize execution timeline.

        Backwards compatible with fluxus 1.0.

        Parameters:
            style: Visualization style ("matplot" for matplotlib output)
            out: Optional output target (file path or stream)
        """
        ...
```

Requirements:

- `RunResult.final` is a list of final output records after all merges and loops.
- `lineage_for(i)` returns a `Lineage` object that can reconstruct the ordered sequence of `(step, output)` that contributed to `final[i]`.
- Lineage includes branch and loop iterations as separate entries.
- All fluxus 1.0 methods are preserved for backwards compatibility.

---

## 7. Argument Resolution for Steps

Each `Step` inspects its `fn` signature to determine required arguments.

### 7.1 Input resolution from records

For each parameter of `fn`:

1. Let `param_name` be the argument name.
2. To find a value for `param_name`:
   - Look at the **current record** (latest dict for this branch / loop iteration).
   - If not present, scan previous records in reverse chronological order along that branch’s lineage and pick the first record containing `param_name`.
3. If still not found:
   - If the parameter has a default value, use that default.
   - Otherwise, raise `MissingInputError`.

### 7.2 Merged concurrent results

When inputs are merged (via `~` or `merge`):

- The merged record is a dictionary of lists:

  ```python
  {
      "x": [x_from_branch1, x_from_branch2, ...],
      "y": [y_from_branch1, None, ...],
      # ...
  }
  ```

- For each step argument `param_name` immediately after a merge:

  - The value associated with `param_name` is a list of values, one per branch within the merge scope.
  - Branches that did not produce `param_name` contribute `None`.

Initial behaviour:

- Steps after a merge receive lists directly as argument values.
- The library may later introduce configurable collapsing strategies (e.g. first non-`None`, aggregation functions), but the default semantics are list-of-values.

---

## 8. Execution Semantics and Concurrency

- Each `Conduit` operates over a multiset of records.
- **Note**: All products must be `dict[str, object]` in fluxus 2.0 (simplified from 1.0's generic types).

### 8.1 Granularity

- **Step application / sequence (**`>>`**)**:

  - Input: list of records.
  - Output: list of records from the next step.

- **Parallel Branch (**`&`**)**:

  - Input: list of records.
  - Output: concatenation of results from each branch, with branch identity tracked in lineage for later merging.

- **Merge (**`~`** / **`merge()`**)**:

  - Input: list of records with branch identifiers.
  - For each original input record, gather all branch results in that merge scope and build a single merged record where each key maps to a list of values, one per branch, with missing values represented as `None`.

- **Loop (**`loop()`**)**:

  - Applied per record as described above; may generate multiple records if the inner `conduit` branches.
  - Each loop iteration is part of the lineage and can be distinguished via metadata (e.g. iteration counter) in the implementation.

### 8.2 Concurrency

- All step calls are asynchronous:
  - Within a step application, each record is processed concurrently using `asyncio` primitives.
  - Concurrency is bounded by `concurrency` if provided.

---

## 9. Lazy Evaluation and On-demand Key Computation

- Merged records behave like dictionaries where values are lists but are computed lazily per key when accessed.

Implementation sketch (non-normative):

```python
class LazyMergedRecord(dict[str, object]):
    """Dictionary-like object that lazily materialises merged key values."""

    def __init__(self, branches: list[Record]):
        super().__init__()
        self._branches = branches
        self._cache: dict[str, list[object | None]] = {}

    def __getitem__(self, key: str) -> list[object | None]:
        if key not in self._cache:
            self._cache[key] = [b.get(key) for b in self._branches]
        return self._cache[key]

    def keys(self):
        # union of keys across branches, computed lazily if desired
        ...
```

Requirements:

- Public semantics: accessing any key performs the minimal work required to compute that key’s list of values across branches.
- Implementations should cache computed keys to avoid repeated work.

---

## 10. Error Handling

### 10.1 Step errors

- If a step’s function raises an exception for a record:
  - Initial behaviour: propagate error and fail the entire `run`, wrapping it in a `FlowExecutionError` that includes step and record context.
  - Future extension: configurable error strategies (drop record, attach error to record, etc.).

### 10.2 Argument resolution errors

- Missing required argument:
  - Raise `MissingInputError(param_name, step)`.
- Non-dict step return value:
  - Raise `TypeError`.

---

## 11. Lineage Tracking Details

Requirements for lineage:

- For every final record in `RunResult.final` it must be possible to reconstruct the ordered sequence of step applications that contributed to it.
- Each step application is represented as `StepOutput(step, output)`.
- Lineage must include:
  - Branch information (which branch of a `&` a step belonged to).
  - Merge points.
  - Loop iteration information (iteration count, if available).

APIs:

- `Lineage.as_list()`:

  - Returns a list ordered by execution order within that record’s path (including branches and loops).

- `Lineage.as_dict_by_step()`:

  - Groups outputs by step name (or id) to facilitate inspection of per-step behaviour.

---

## 12. Operator Precedence and Associativity

To avoid surprises, define explicit precedence:

- Precedence (high to low):

  1. Unary `~`
  2. `&`
  3. `>>`

- Associativity:

  - `>>` is left-associative: `a >> b >> c == (a >> b) >> c`.
  - `&` is left-associative: `a & b & c == (a & b) & c`.
  - `~` binds to the nearest flow; users are encouraged to write explicit parentheses or use `merge(...)` to clarify the intended scope.

---

## 13. Extensibility and Future Considerations

Potential extensions (not required for the initial version):

- Additional combinators:
  - `map`, `filter`, `reduce`-style helpers on `Record`s.
  - Windowing or batching.
- Static analysis:
  - Validate that all required keys for each step are produced somewhere upstream.
- Stronger typing:
  - Protocols or typed dicts for specific `Record` schemas.
- Integrations:
  - Adapters to and from `asyncio` streams or message queues.
- Loop safeguards:
  - Optional `max_iterations` in `loop` to prevent unbounded cycles.

---

## 14. Backwards Compatibility Summary

This design maintains full backwards compatibility with fluxus 1.0:

### Preserved from 1.0:
- ✅ `Conduit` class name (not `Flow`)
- ✅ `Producer`, `Transformer`, `Consumer` facade classes for advanced users
- ✅ Both `run()` (sync) and `arun()` (async) execution methods
- ✅ Operators: `>>` (sequence) and `&` (parallel) with same semantics
- ✅ Functional API: `step()`, `chain()`, `parallel()`, `passthrough()`, `run()`
- ✅ `step()` signature: `step(name, fn, **kwargs)` (not `step(fn, name)`)
- ✅ RunResult methods: `get_outputs()`, `get_outputs_per_path()`, `to_frame()`, `draw_timeline()`
- ✅ Visualization: `conduit.draw(style="graph")`
- ✅ Pull model: Producers can generate their own inputs

### New in 2.0:
- ✨ `~` operator and `merge()` for merging parallel branches
- ✨ `loop()` combinator for controlled iteration
- ✨ Enhanced lineage: `lineage_for()`, `all_lineages()`
- ✨ Simplified type system: dict-only products (no generic types)
- ✨ Optional push model: `arun(inputs=[...])`

### Migration Path:
- **Existing 1.0 code works unchanged**: All functional API and OOP API code continues to work
- **New 2.0 features available alongside**: Users can adopt merge/loop incrementally
- **No breaking changes**: Deprecated patterns can be supported for several major versions
