"""
Simple SkyFrame with signalDep mechanism
"""

# ============================================================================
# Node - tracks state and dependency signals
# ============================================================================

class Node:
    def __init__(self, sky_function):
        self.sky_function = sky_function
        self.state = "NOT_STARTED"
        self.value = None
        self.direct_deps = []
        self.signaled_deps = 0
        self.reverse_deps = []  # list of parent SkyFunctions

    def get_value(self):
        """Returns the computed value, or None if not done"""
        if self.state == "DONE":
            return self.value
        return None

    def add_dep(self, dep_key):
        self.direct_deps.append(dep_key)

    def signal_dep(self):
        """Returns True if node is now ready"""
        self.signaled_deps += 1
        return self.signaled_deps == len(self.direct_deps)

    def mark_done(self, value):
        self.state = "DONE"
        self.value = value


# ============================================================================
# Graph - memoized node storage
# ============================================================================

class Graph:
    def __init__(self):
        self.nodes = {}  # key string -> Node

    def get_or_create(self, sky_key):
        if sky_key.key not in self.nodes:
            self.nodes[sky_key.key] = Node(sky_key)
        return self.nodes[sky_key.key]

    def get(self, key):
        return self.nodes.get(key)

    def items(self):
        return self.nodes.items()


# ============================================================================
# SkyFunctions - stateless computation logic
# ============================================================================

class FileStateFunction:
    def __init__(self, path):
        self.key = f"FILE_STATE:{path}"
        self.path = path

    def compute(self, env):
        return f"content({self.path})"


class FileFunction:
    def __init__(self, path):
        self.key = f"FILE:{path}"
        self.path = path

    def compute(self, env):
        # If this is a file (not directory), depend on parent directory first
        if not self.path.endswith("/"):
            parent_dir = "/".join(self.path.split("/")[:-1]) + "/"
            env.get_value(FileFunction(parent_dir))

        state = env.get_value(FileStateFunction(self.path))
        if state is None:
            return None
        return f"File[{state}]"


class ArtifactFunction:
    def __init__(self, path):
        self.key = f"ARTIFACT:{path}"
        self.path = path

    def compute(self, env):
        file_val = env.get_value(FileFunction(f"/workspace/{self.path}"))
        if file_val is None:
            return None
        return f"Artifact({file_val})"


class ArtifactNestedSetFunction:
    def __init__(self, n):
        self.key = f"ARTIFACT_NESTED_SET[{n}]"
        self.n = n

    def compute(self, env):
        a1 = env.get_value(ArtifactFunction("hello.py"))
        a2 = env.get_value(ArtifactFunction("lib.py"))
        if a1 is None or a2 is None:
            return None
        return f"NestedSet{{{a1}, {a2}}}"


from collections import OrderedDict


# ============================================================================
# Executor - evaluates SkyFunctions using a work queue
# ============================================================================

class Executor:
    def __init__(self):
        self.graph = Graph()
        self.queue = OrderedDict()  # key -> sky_function (acts as ordered set)

    def evaluate(self, sky_function):
        self._enqueue(sky_function)
        self._run()
        return self.graph.get(sky_function.key).value

    def _enqueue(self, sky_function):
        if sky_function.key not in self.queue:
            self.queue[sky_function.key] = sky_function

    def _create_environment(self, node):
        """Creates an Environment for a SkyFunction to request dependencies"""
        return _Environment(self, node)

    def _run(self):
        step = 0
        while self.queue:
            step += 1
            key, sky_function = self.queue.popitem(last=False)  # FIFO
            node = self.graph.get_or_create(sky_function)

            print(f"\n[{step}] {sky_function.key}")

            if node.state == "DONE":
                print(f"    cached: {node.value}")
                continue

            env = self._create_environment(node)
            result = sky_function.compute(env)

            if result is not None:
                node.mark_done(result)
                print(f"    done: {result}")

                # Signal all parents (reverse deps)
                for parent_sky_function in node.reverse_deps:
                    parent = self.graph.get(parent_sky_function.key)
                    print(f"    signal {parent_sky_function.key} ({parent.signaled_deps+1}/{len(parent.direct_deps)})")
                    if parent.signal_dep():
                        print(f"      -> ready, re-queue")
                        self._enqueue(parent_sky_function)
            else:
                print("    waiting on deps")


# ============================================================================
# Environment - provided to SkyFunction.compute() to request dependencies
# ============================================================================

class _Environment:
    """
    The interface provided to SkyFunction.compute() by the Executor.

    Mirrors Bazel's SkyFunction.Environment - allows requesting dependency
    values via get_value(). Returns None if dep is not ready.
    """
    def __init__(self, executor, current_node):
        self._executor = executor
        self._current = current_node

    def get_value(self, dep_sky_function):
        """
        Request a dependency value.
        Returns the value if available, or None if the dep needs to be computed first.
        """
        dep_node = self._executor.graph.get_or_create(dep_sky_function)

        # Record dependency edge
        self._current.add_dep(dep_sky_function.key)
        dep_node.reverse_deps.append(self._current.sky_function)

        # Return cached value if done
        value = dep_node.get_value()
        if value is not None:
            return value

        # Not done - enqueue for evaluation
        self._executor._enqueue(dep_sky_function)
        return None


# ============================================================================
# Demo
# ============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Evaluating ARTIFACT_NESTED_SET[2]")
    print("=" * 60)

    executor = Executor()
    result = executor.evaluate(ArtifactNestedSetFunction(2))

    print("\n" + "=" * 60)
    print("Final graph:")
    print("=" * 60)
    for key, node in executor.graph.items():
        print(f"  {key}: {node.value}")
