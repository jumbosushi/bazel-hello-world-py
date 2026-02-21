"""
Minimal Skyframe: restart-based evaluation with signaling
"""

# ============================================================================
# Core Framework
# ============================================================================

class Graph:
    def __init__(self):
        self.nodes = {}        # key -> value (None if not done)
        self.reverse_deps = {}  # key -> set of parent keys
        self.waiting_on = {}    # key -> count of unsignaled deps
        self.in_flight = set()  # keys currently being evaluated

    def get_or_create(self, key):
        if key not in self.nodes:
            self.nodes[key] = None
            self.reverse_deps[key] = set()
            self.waiting_on[key] = 0
        return self.nodes.get(key)

    def is_done(self, key):
        return key in self.nodes and self.nodes[key] is not None


class Environment:
    """Provided to compute() - tracks missing deps"""
    def __init__(self, graph, queue, current_key):
        self._graph = graph
        self._queue = queue
        self._key = current_key
        self._missing = False
        self._new_deps = []

    def get_value(self, dep_key):
        self._graph.get_or_create(dep_key)

        # If done, return the value
        if self._graph.is_done(dep_key):
            return self._graph.nodes[dep_key]

        # Dep not ready - register reverse edge (parent tracking)
        if self._key not in self._graph.reverse_deps[dep_key]:
            self._graph.reverse_deps[dep_key].add(self._key)
            self._new_deps.append(dep_key)

            # Only enqueue if not already in-flight
            if dep_key not in self._graph.in_flight:
                self._graph.in_flight.add(dep_key)
                self._queue.append(dep_key)

        self._missing = True
        return None

    def nodes_missing(self):
        return self._missing


class Executor:
    def __init__(self, functions):
        self.graph = Graph()
        self.functions = functions  # key class name -> SkyFunction instance
        self.queue = []

    def evaluate(self, key):
        self.graph.get_or_create(key)  # Initialize root key
        self.graph.in_flight.add(key)
        self.queue.append(key)
        step = 0

        while self.queue:
            key = self.queue.pop(0)
            step += 1

            if self.graph.is_done(key):
                print(f"[{step}] {key} -> cached")
                continue

            print(f"[{step}] {key}")

            env = Environment(self.graph, self.queue, key)
            func = self.functions[type(key).__name__]
            result = func.compute(key, env)

            if result is not None:
                self.graph.nodes[key] = result
                self.graph.in_flight.discard(key)
                print(f"      done: {result}")
                # Signal parents
                for parent in self.graph.reverse_deps[key]:
                    self.graph.waiting_on[parent] -= 1
                    remaining = self.graph.waiting_on[parent]
                    print(f"      signal {parent} ({remaining} remaining)")
                    if remaining == 0:
                        print(f"      -> re-enqueue {parent}")
                        self.queue.append(parent)
            else:
                self.graph.waiting_on[key] = len(env._new_deps)
                print(f"      waiting on {len(env._new_deps)} deps: {env._new_deps}")

        return self.graph.nodes.get(key)

# ============================================================================
# SkyKeys - just identifiers (immutable, hashable)
# ============================================================================

class FileStateKey:
    def __init__(self, path):
        self.path = path

    def __hash__(self):
        return hash(("FileState", self.path))

    def __eq__(self, other):
        return isinstance(other, FileStateKey) and self.path == other.path

    def __repr__(self):
        return f"FILE_STATE:{self.path}"


class FileKey:
    def __init__(self, path):
        self.path = path

    def __hash__(self):
        return hash(("File", self.path))

    def __eq__(self, other):
        return isinstance(other, FileKey) and self.path == other.path

    def __repr__(self):
        return f"FILE:{self.path}"


class ArtifactKey:
    def __init__(self, path):
        self.path = path

    def __hash__(self):
        return hash(("Artifact", self.path))

    def __eq__(self, other):
        return isinstance(other, ArtifactKey) and self.path == other.path

    def __repr__(self):
        return f"ARTIFACT:{self.path}"


class ArtifactNestedSetKey:
    def __init__(self, n):
        self.n = n

    def __hash__(self):
        return hash(("ArtifactNestedSet", self.n))

    def __eq__(self, other):
        return isinstance(other, ArtifactNestedSetKey) and self.n == other.n

    def __repr__(self):
        return f"ARTIFACT_NESTED_SET:{self.n}"


# ============================================================================
# SkyFunctions - stateless singletons that compute nodes for keys
# ============================================================================

class FileStateFunction:
    """Reads raw file content from disk (leaf node)"""
    def compute(self, key, env):
        # Leaf node - no deps, just return the value
        return f"content({key.path})"


class FileFunction:
    """Resolves a file, checking parent directory first"""
    def compute(self, key, env):
        # If not a directory, depend on parent dir first
        if not key.path.endswith("/"):
            parent_path = "/".join(key.path.split("/")[:-1]) + "/"
            env.get_value(FileKey(parent_path))

        # Depend on file state
        state = env.get_value(FileStateKey(key.path))

        if env.nodes_missing():
            return None

        return f"File[{state}]"


class ArtifactFunction:
    """Wraps a file as a build artifact"""
    def compute(self, key, env):
        file_val = env.get_value(FileKey(f"/workspace/{key.path}"))

        if env.nodes_missing():
            return None

        return f"Artifact({file_val})"


class ArtifactNestedSetFunction:
    """Collects multiple artifacts into a set"""
    def compute(self, key, env):
        # Request both artifacts (Skyframe can parallelize these)
        a1 = env.get_value(ArtifactKey("hello.py"))
        a2 = env.get_value(ArtifactKey("lib.py"))

        if env.nodes_missing():
            return None

        return f"NestedSet{{{a1}, {a2}}}"


# ============================================================================
# Demo
# ============================================================================

if __name__ == "__main__":
    # Register functions (singleton per key type)
    functions = {
        "FileStateKey": FileStateFunction(),
        "FileKey": FileFunction(),
        "ArtifactKey": ArtifactFunction(),
        "ArtifactNestedSetKey": ArtifactNestedSetFunction(),
    }

    print("=" * 60)
    print("Evaluating ArtifactNestedSetKey(2)")
    print("=" * 60)

    executor = Executor(functions)
    result = executor.evaluate(ArtifactNestedSetKey(2))

    print("\nGraph contents:")
    for key, value in executor.graph.nodes.items():
        print(f"  {key}: {value}")
