from typing import List, Tuple, Dict, Optional, Set, Type

import networkx


class Graph:
    def __init__(
        self,
        edges: List[Tuple],
        nodes_hash: Optional[Dict] = None,
        edges_weight: Optional[Dict] = None,
    ):
        self.undirected_graph = networkx.Graph(edges)
        self.directed_graph = networkx.DiGraph(edges)

        if nodes_hash is not None:
            self._add_node_hash_to_graph(nodes_hash=nodes_hash)

        if edges_weight is not None:
            self._add_edge_weight_to_graph(edges_weight=edges_weight)

    def _add_node_hash_to_graph(self, nodes_hash: Dict):
        for node, node_hash in nodes_hash.items():
            self.undirected_graph.nodes[node]["hash"] = node_hash

    def _add_edge_weight_to_graph(self, edges_weight: Dict):
        for edge, edge_weight in edges_weight.items():
            self.directed_graph.edges[edge]["weight"] = edge_weight

    def get_connected_components(self) -> List[Set]:
        return [
            connected_subgraph_nodes for connected_subgraph_nodes in
            networkx.connected_components(self.undirected_graph)
        ]

    def get_strongly_connected_components(self) -> List[Set]:
        return [
            strongly_connected_subgraph_nodes for
            strongly_connected_subgraph_nodes in
            networkx.strongly_connected_components(self.directed_graph)
        ]

    def get_wl_subgraph_hashes_per_nodes(
        self,
        iterations: int,
    ) -> Dict[Type, List[str]]:
        return networkx.weisfeiler_lehman_subgraph_hashes(
            self.undirected_graph,
            iterations=iterations,
            node_attr="hash",
        )

    def get_graph_communities(self) -> List[set]:
        return list(
            networkx.community
            .fast_label_propagation_communities(self.directed_graph)
        )
