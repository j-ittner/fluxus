# DSL for Asynchronous Flow Composition in Python

This is the new design for fluxus 2.0.

## 1. Purpose and Scope

- Provide a small, composable DSL in Python for building **asynchronous, lazily evaluated dataflow pipelines**.
- Pipelines are constructed from **steps** that:
  - Consume and produce dictionaries
  - Are executed concurrently over collections of such dictionaries
- Design principles (inspired by Haskell):
  - **Compositionality**: flows can be composed from smaller flows using a small set of combinators.
  - **Referential transparency**: flow definitions are pure descriptions; `run` executes them.
  - **Lazy evaluation**: merged results and intermediate keys are computed on demand.
  - **Predictable behaviour** via type hints and clear runtime semantics.

Non-goals (for initial version):

- Full streaming transport (backpressure, networked streams, etc.).
- Arbitrary cyclic graphs (focus on DAG-like or well-structured branching/merging semantics; an explicit `loop` combinator provides controlled cycles).
- Sophisticated scheduling or cluster execution.

---

## 2. Core Concepts and Terminology

- **Record**: a `dict[str, object]` representing the state at a given point in the pipeline.
- **Flow**:
  - A compositional description of how steps and subflows are combined.
  - Can be constructed via operators (`>>`, `&`, `~`) or functional combinators (`chain`, `branch`, `merge`, `loop`).
- **Step**:
  - An atomic `Flow` wrapping a Python callable.
  - Callable signature is inspected to resolve inputs from the record.
  - Callable must return a `dict[str, object]`.
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

### 3.2 Flow and Step base classes

```python
from abc import ABC, abstractmethod


class Flow(ABC):
    """Abstract base for all flow nodes (steps and composites)."""

    name: str

    @abstractmethod
    async def run(
        self,
        inputs: Records,
        *,
        concurrency: int | None = None,
    ) -> "RunResult | AsyncIterator[RunResult]":
        """Execute this flow over a list of input records.

        Implementations may either:
        - collect all outputs and return a single `RunResult`, or
        - return an `AsyncIterator[RunResult]` that streams results as they
          become available (e.g. when steps spawn multiple concurrent
          executions downstream).
        """
        ...

    # Operator DSL
    def __rshift__(self, other: "Flow") -> "Flow": ...  # sequence
    def __and__(self, other: "Flow") -> "Flow": ...     # parallel branch
    def __invert__(self) -> "Flow": ...                  # merge scope



@dataclass(slots=True)
class Step(Flow):
    """Atomic flow node wrapping a Python callable.

    A Step *is a* Flow and participates in all Flow compositions.
    """

    fn: StepFn
    name: str | None = None

    def __post_init__(self) -> None:
        if self.name is None:
            self.name = getattr(self.fn, "__name__", f"step_{id(self):x}")

    async def run(
        self,
        inputs: Records,
        *,
        concurrency: int | None = None,
    ) -> "RunResult | AsyncIterator[RunResult]":
        ...
```

### 3.3 Step construction helper

```python
def step(fn: StepFn, name: str | None = None) -> Step:
    """Construct a Step from a callable.

    - If `name` is not provided, the step name defaults to `fn.__name__` if present,
      otherwise a generated identifier.
    - `fn` may be synchronous (returns `Record`) or asynchronous
      (returns `Awaitable[Record]`).
    - The return value must be a `dict[str, object]`; otherwise a `TypeError` is raised.
    """
    return Step(fn=fn, name=name)
```

---

## 4. Flow Construction – Operator DSL

Flows are built from `Flow` (including `Step`) objects using operators.

### 4.1 Sequence: `>>`

```python
Flow.__rshift__(self, other: Flow) -> Flow
```

- `a >> b` means: run `a` first, then use its outputs as inputs to `b`.
- Semantics:
  - For each incoming record:
    - Apply `a` (may produce `0..n` records).
    - For each resulting record, apply `b`.
- This defines sequential composition.

### 4.2 Parallel Branching: `&`

```python
Flow.__and__(self, other: Flow) -> Flow
```

- `a & b` means: run `a` and `b` in parallel for each incoming record.
- Semantics:
  - For each incoming record `r`:
    - Feed `r` into `a` and `b`, each in their own branch.
    - Each branch produces `0..n` records independently.
  - The flow now represents multiple concurrent branches per original record.

### 4.3 Merge / Join Scope: unary `~`

```python
Flow.__invert__(self) -> Flow
```

- `~f` means: merge all concurrent results created within `f`’s branching structure into a single record per original input.

Example:

```python
s1 = step(fn=lambda x, y=2: {"y": x + y})
s2 = step(name="times", fn=lambda x: {"y": x * 5})

flow = s1 & ~((s1 & s2) >> s1)

inputs = [{"x": 1}, {"x": 2}, {"x": 3}]
result = await flow.run(inputs)
```

Interpretation:

- Start with the input records.
- `s1 & (...)` creates two branches per record.
- Inside the `~(...)` scope the branches `(s1 & s2) >> s1` run in parallel.
- `~` merges all concurrent results within its scope into a single merged record per original input.

---

## 5. Flow Construction – Functional DSL

To provide a clear, functional alternative for any operator expression, the DSL exposes explicit combinators.

### 5.1 `chain(...)`

```python
def chain(*flows: Flow) -> Flow:
    """Sequentially compose all arguments.

    chain(a, b, c) is equivalent to ((a >> b) >> c).
    """
```

### 5.2 `branch(...)`

```python
def branch(*flows: Flow) -> Flow:
    """Parallel branch composition.

    branch(a, b, c) is equivalent to (a & b & c).
    """
```

### 5.3 `merge(...)`

```python
def merge(flow: Flow) -> Flow:
    """Merge all concurrent results created inside `flow`'s scope.

    Equivalent to `~flow`.
    """
```

### 5.4 `loop(...)`

```python
ConditionFn = Callable[[Record], bool]


def loop(*, until: ConditionFn, flow: Flow) -> Flow:
    """Repeatedly apply `flow` to each record until `until(record)` is true.

    Semantics (per record):
    - Start from the incoming record.
    - While `not until(current_record)`:
        - Apply `flow` to `[current_record]`.
        - If `flow` yields no records, terminate the loop for this record.
        - If `flow` yields `>1` records, this creates branches *within the loop*.
          All branches continue looping independently until `until` holds
          for their current record.
    - Records for which `until` eventually returns `True` are emitted.

    Behaviour:
    - `loop` introduces controlled cycles in an otherwise DAG-like graph.
    - Internally, the implementation must prevent infinite loops, e.g. by
      allowing an optional `max_iterations` parameter in the future.
    """
```

### 5.5 Example mapping between styles

Given the operator-style example:

```python
flow = s1 & ~((s1 & s2) >> s1)
```

A functional equivalent is:

```python
flow = branch(
    s1,
    merge(
        chain(
            branch(s1, s2),
            s1,
        ),
    ),
)
```

Requirements:

- Every operator-based flow must have a functional equivalent via `branch`, `chain`, `merge`, and optionally `loop`.
- The library should document that equivalence and keep the semantics aligned.

---

## 6. Execution API and RunResult

### 6.1 Execution API on Flow

```python
class Flow:
    name: str

    async def run(
        self,
        inputs: Records,
        *,
        concurrency: int | None = None,
    ) -> "RunResult | AsyncIterator[RunResult]":
        ...
```

- `inputs` is a list of initial records.
- `concurrency` controls the maximum number of concurrent step invocations (or `None` for default behaviour).
- The return value of `run` may be either:
  - a single `RunResult` (fully materialised outputs), or
  - an `AsyncIterator[RunResult]` that streams results as they become available when steps spawn multiple concurrent executions downstream.

### 6.2 RunResult and Lineage

```python
from typing import NamedTuple


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
    """Execution result for a flow.

    - Contains final outputs and full lineage for each final record.
    """

    final: list[Record]

    def lineage_for(self, index: int) -> Lineage:
        """Return lineage for `final[index]`."""
        ...

    def all_lineages(self) -> list[Lineage]:
        """Return lineages for all final records."""
        ...
```

Requirements:

- `RunResult.final` is a list of final output records after all merges and loops.
- `lineage_for(i)` returns a `Lineage` object that can reconstruct the ordered sequence of `(step, output)` that contributed to `final[i]`.
- Lineage includes branch and loop iterations as separate entries.

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

- Each `Flow` operates over a multiset of records.

### 8.1 Granularity

- **Step application / sequence (**``**)**:

  - Input: list of records.
  - Output: list of records from the next step.

- **Branch (**``**)**:

  - Input: list of records.
  - Output: concatenation of results from each branch, with branch identity tracked in lineage for later merging.

- **Merge (**``** / **``**)**:

  - Input: list of records with branch identifiers.
  - For each original input record, gather all branch results in that merge scope and build a single merged record where each key maps to a list of values, one per branch, with missing values represented as `None`.

- **Loop (**``**)**:

  - Applied per record as described above; may generate multiple records if the inner `flow` branches.
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

```}
```
