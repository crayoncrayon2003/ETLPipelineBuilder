from collections import Counter, deque
from typing import Any, Iterable


def validate_pipeline_graph(nodes: Iterable[Any], edges: Iterable[Any]) -> None:
    """
    Validate the framework's single-input DAG contract.

    The function intentionally accepts node/edge objects by attribute rather
    than depending on the API schema layer, so every execution entry point can
    share the same validation without introducing a core -> API dependency.
    """
    node_list = list(nodes)
    edge_list = list(edges)

    if not node_list:
        raise ValueError("Pipeline must contain at least one node.")

    node_ids = [node.id for node in node_list]
    duplicate_ids = sorted(
        node_id for node_id, count in Counter(node_ids).items() if count > 1
    )
    if duplicate_ids:
        raise ValueError(f"Duplicate node IDs are not allowed: {duplicate_ids}")

    node_id_set = set(node_ids)
    indegree = {node_id: 0 for node_id in node_ids}
    adjacency = {node_id: [] for node_id in node_ids}
    incoming_sources = {node_id: [] for node_id in node_ids}

    for edge in edge_list:
        source = edge.source_node_id
        target = edge.target_node_id

        if source not in node_id_set:
            raise ValueError(
                f"Edge references unknown source node '{source}'."
            )
        if target not in node_id_set:
            raise ValueError(
                f"Edge references unknown target node '{target}'."
            )

        incoming_sources[target].append(source)
        if len(incoming_sources[target]) > 1:
            raise ValueError(
                f"Node '{target}' has {len(incoming_sources[target])} incoming "
                f"edges (from {incoming_sources[target]}), but the plugin "
                f"interface supports only one upstream input_data."
            )

        adjacency[source].append(target)
        indegree[target] += 1

    ready = deque(node_id for node_id in node_ids if indegree[node_id] == 0)
    visited_count = 0
    while ready:
        node_id = ready.popleft()
        visited_count += 1
        for target in adjacency[node_id]:
            indegree[target] -= 1
            if indegree[target] == 0:
                ready.append(target)

    if visited_count != len(node_ids):
        cyclic_nodes = sorted(
            node_id for node_id, degree in indegree.items() if degree > 0
        )
        raise ValueError(
            f"Pipeline contains a circular dependency involving nodes: "
            f"{cyclic_nodes}"
        )
