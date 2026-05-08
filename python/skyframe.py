# ============================================================================
# Core Framework
# ============================================================================

class SkyKey:
    """Base class for all SkyKeys. Each subclass must implement __hash__ and __eq__."""

    def function_name(self):
        return type(self).__name__

    def argument(self):
        return self


class SkyValue:
    """Marker base class for all computed values."""
    pass


class Node:
    """A node in the Skyframe graph."""

    READY = "READY"
    WAITING = "WAITING"
    DONE = "DONE"

    def __init__(self, key: SkyKey):
        self.key = key
        self.value = None
        self.reverse_deps = set()
        self.direct_deps = []
        self.waiting_on = 0
        self.state = Node.WAITING

    def is_done(self):
        return self.state == Node.DONE

    def add_reverse_dep(self, parent_key: SkyKey):
        self.reverse_deps.add(parent_key)


class Graph:
    """Backed by ConcurrentHashMap<SkyKey, Node> in real Bazel."""

    def __init__(self):
        self.nodes = {}
        self.in_flight = set()

    def get_or_create(self, key: SkyKey) -> Node:
        if key not in self.nodes:
            node = Node(key)
            node.state = Node.READY
            self.nodes[key] = node
        return self.nodes[key]

    def is_done(self, key: SkyKey) -> bool:
        return key in self.nodes and self.nodes[key].is_done()


class Environment:
    """Bridge between SkyFunction and the evaluator."""

    def __init__(self, graph: Graph, queue: list, current_key: SkyKey):
        self._graph = graph
        self._queue = queue
        self._key = current_key
        self._missing = False
        self._new_deps = []

    def get_value(self, dep_key: SkyKey) -> SkyValue | None:
        dep_node = self._graph.get_or_create(dep_key)

        if dep_node.is_done():
            return dep_node.value

        if self._key not in dep_node.reverse_deps:
            dep_node.add_reverse_dep(self._key)
            self._new_deps.append(dep_key)
            if dep_key not in self._graph.in_flight:
                self._graph.in_flight.add(dep_key)
                self._queue.append(dep_key)
                print(f"      enqueued {dep_key}")

        self._missing = True
        return None

    def values_missing(self):
        return self._missing


class Executor:
    """Just runs SkyFunction.compute(). Mirrors ThreadPoolExecutor.

    In real Bazel this wraps a thread pool and runs evaluations in parallel.
    In this simplified demo it runs single-threaded.
    """

    def __init__(self, functions: dict):
        self.functions = functions

    def run(self, key: SkyKey, env: Environment) -> SkyValue | None:
        func = self.functions[type(key).__name__]
        return func.compute(key, env)


class Evaluator:
    """Orchestrates evaluation lifecycle. Mirrors AbstractParallelEvaluator"""

    def __init__(self, functions: dict):
        self.graph = Graph()
        self.executor = Executor(functions)
        self.queue = []

    def evaluate(self, key: SkyKey) -> SkyValue | None:
        self.graph.get_or_create(key)
        self.graph.in_flight.add(key)
        self.queue.append(key)
        step = 0
        print(f"      enqueued {key}")

        while self.queue:
            key = self.queue.pop(0)
            step += 1

            if self.graph.is_done(key):
                print(f"[{step}] {key} -> cached")
                continue

            print(f"[{step}] {key}  [dequeued]")

            env = Environment(self.graph, self.queue, key)
            result = self.executor.run(key, env)

            node = self.graph.nodes[key]

            if result is not None:
                node.value = result
                node.state = Node.DONE
                self.graph.in_flight.discard(key)
                print(f"      done: {result}")
                for parent in node.reverse_deps:
                    parent_node = self.graph.nodes[parent]
                    parent_node.waiting_on -= 1
                    remaining = parent_node.waiting_on
                    print(f"      signal {parent} ({remaining} remaining)")
                    if remaining == 0:
                        parent_node.state = Node.READY
                        self.queue.append(parent)
                        print(f"      -> {parent} is READY (enqueued)")
            else:
                node.waiting_on = len(env._new_deps)
                node.direct_deps = list(env._new_deps)
                node.state = Node.WAITING
                print(f"      -> {key} is WAITING")
                print(f"      waiting on {len(env._new_deps)} deps: {env._new_deps}")

        return self.graph.nodes[key].value if key in self.graph.nodes else None


# ============================================================================
# SkyKeys
# ============================================================================

class FileStateKey(SkyKey):
    def __init__(self, path: str):
        self.path = path

    def __hash__(self):
        return hash(("FileState", self.path))

    def __eq__(self, other):
        return isinstance(other, FileStateKey) and self.path == other.path

    def __repr__(self):
        return f"FILE_STATE:{self.path}"


class FileKey(SkyKey):
    def __init__(self, path: str):
        self.path = path

    def __hash__(self):
        return hash(("File", self.path))

    def __eq__(self, other):
        return isinstance(other, FileKey) and self.path == other.path

    def __repr__(self):
        return f"FILE:{self.path}"


class ArtifactKey(SkyKey):
    def __init__(self, path: str):
        self.path = path

    def __hash__(self):
        return hash(("Artifact", self.path))

    def __eq__(self, other):
        return isinstance(other, ArtifactKey) and self.path == other.path

    def __repr__(self):
        return f"ARTIFACT:{self.path}"


class ArtifactNestedSetKey(SkyKey):
    def __init__(self, n: int, artifact_paths: list[str]):
        self.n = n
        self.artifact_paths = artifact_paths

    def __hash__(self):
        return hash(("ArtifactNestedSet", self.n))

    def __eq__(self, other):
        return isinstance(other, ArtifactNestedSetKey) and self.n == other.n

    def __repr__(self):
        return f"ARTIFACT_NESTED_SET:{self.n}"


# ============================================================================
# SkyValues
# ============================================================================

class FileStateValue(SkyValue):
    def __init__(self, path: str, content: str):
        self.path = path
        self.content = content

    def __repr__(self):
        return f"FileStateValue({self.path!r}, content={self.content!r})"


class FileValue(SkyValue):
    def __init__(self, path: str, state: FileStateValue):
        self.path = path
        self.state = state

    def __repr__(self):
        return f"FileValue({self.path!r})"


class ArtifactValue(SkyValue):
    def __init__(self, path: str, file_value: FileValue):
        self.path = path
        self.file_value = file_value

    def __repr__(self):
        return f"ArtifactValue({self.path!r})"


class ArtifactNestedSetValue(SkyValue):
    def __init__(self, artifacts: list):
        self.artifacts = artifacts

    def __repr__(self):
        paths = [a.path for a in self.artifacts]
        return f"ArtifactNestedSetValue({paths})"


# ============================================================================
# SkyFunctions
# ============================================================================

class FileStateFunction:
    def compute(self, key: FileStateKey, env: Environment) -> FileStateValue:
        return FileStateValue(path=key.path, content=f"bytes({key.path})")


class FileFunction:
    def compute(self, key: FileKey, env: Environment) -> FileValue | None:
        if not key.path.endswith("/"):
            parent_path = "/".join(key.path.split("/")[:-1]) + "/"
            env.get_value(FileKey(parent_path))

        state: FileStateValue = env.get_value(FileStateKey(key.path))

        if env.values_missing():
            return None

        return FileValue(path=key.path, state=state)


class ArtifactFunction:
    def compute(self, key: ArtifactKey, env: Environment) -> ArtifactValue | None:
        file_val: FileValue = env.get_value(FileKey(f"/workspace/{key.path}"))

        if env.values_missing():
            return None

        return ArtifactValue(path=key.path, file_value=file_val)


class ArtifactNestedSetFunction:
    def compute(self, key: ArtifactNestedSetKey, env: Environment) -> ArtifactNestedSetValue | None:
        artifacts = [env.get_value(ArtifactKey(p)) for p in key.artifact_paths]

        if env.values_missing():
            return None

        return ArtifactNestedSetValue(artifacts=artifacts)


# ============================================================================
# Demo
# ============================================================================

if __name__ == "__main__":
    functions = {
        "FileStateKey": FileStateFunction(),
        "FileKey": FileFunction(),
        "ArtifactKey": ArtifactFunction(),
        "ArtifactNestedSetKey": ArtifactNestedSetFunction(),
    }

    print("=" * 60)
    print("Evaluating ArtifactNestedSetKey(2)")
    print("=" * 60)

    evaluator = Evaluator(functions)
    result = evaluator.evaluate(
        ArtifactNestedSetKey(2, artifact_paths=["hello.py", "lib.py"])
    )

    print("\n" + "=" * 60)
    print("Final result:", result)
    if isinstance(result, ArtifactNestedSetValue):
        for artifact in result.artifacts:
            print(f"  {artifact} -> {artifact.file_value.state.content}")
    print("=" * 60)

    print("\nGraph contents:")
    for key, node in evaluator.graph.nodes.items():
        rdeps = ", ".join(str(k) for k in node.reverse_deps) if node.reverse_deps else "(none)"
        print(f"  {key}: value={node.value}, reverse_deps=[{rdeps}]")
