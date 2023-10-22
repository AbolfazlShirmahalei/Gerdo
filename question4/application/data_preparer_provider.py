from typing import Optional, Dict, List, Set, Union

from pyspark.sql import DataFrame

from question4.application.graph_provider import Graph


class Graph2VecDataPreparer:
    def __init__(
        self,
        wl_sub_graph_iterations: int,
        source_column_name: str = "source",
        destination_column_name: str = "destination",
        node_column_name: str = "node",
        node_hash_column_name: str = "nodeHash",
    ):
        self.wl_sub_graph_iterations = wl_sub_graph_iterations

        self.source_column_name = source_column_name
        self.destination_column_name = destination_column_name
        self.node_column_name = node_column_name
        self.node_hash_column_name = node_hash_column_name

    @staticmethod
    def _create_sub_graph_index_to_sub_graph_nodes(
        connected_sub_graphs: List[Set],
    ) -> Dict[int, Set]:
        return {
            index + 1: sub_graph_nodes
            for index, sub_graph_nodes
            in enumerate(connected_sub_graphs)
        }

    @staticmethod
    def _get_wl_sub_graph_vocab(
        sub_graph_index_to_wl_sab_graphs_hash: Dict[int, Set[str]],
    ) -> Dict[str, Union[Dict[int, str], Dict[str, int], int]]:
        wl_sub_graphs = set()

        for sub_graph_index in sub_graph_index_to_wl_sab_graphs_hash.keys():
            wl_sub_graphs.update(
                sub_graph_index_to_wl_sab_graphs_hash[sub_graph_index]
            )

        number_of_wl_sub_graphs = len(wl_sub_graphs)

        wl_sub_graph_to_wl_index = {}

        for wl_index_minus_one, wl_subgraph in enumerate(wl_sub_graphs):
            wl_index = wl_index_minus_one + 1
            wl_sub_graph_to_wl_index[wl_subgraph] = wl_index

        return {
            "hash_to_int": wl_sub_graph_to_wl_index,
            "number_of_wl_sub_graphs": number_of_wl_sub_graphs,
        }

    @staticmethod
    def _get_sub_graph_index_to_wl_indices(
        sub_graph_index_to_wl_sab_graphs_hash: Dict[int, Set[str]],
        wl_hash_to_wl_index: Dict[str, int],
    ) -> Dict[int, Set[int]]:
        return {
            sub_graph_index: {
                wl_hash_to_wl_index[wl_sub_graph]
                for wl_sub_graph in sub_graph_wl_sub_graphs
            }
            for sub_graph_index, sub_graph_wl_sub_graphs
            in sub_graph_index_to_wl_sab_graphs_hash.items()
        }

    @staticmethod
    def _create_positive_training_sample(
        sub_graph_index_to_wl_indices: Dict[int, Set[int]],
    ) -> Dict[str, List[int]]:
        positive_training_input = []
        positive_training_target = []

        for sub_graph_index in sub_graph_index_to_wl_indices.keys():
            positive_training_input.extend(
                [sub_graph_index]
                * len(sub_graph_index_to_wl_indices[sub_graph_index])
            )

            positive_training_target.extend(
                list(sub_graph_index_to_wl_indices[sub_graph_index])
            )

        return {
            "positive_training_input": positive_training_input,
            "positive_training_target": positive_training_target,
        }

    def _create_graph(
        self,
        graph_df: DataFrame,
        hash_df: Optional[DataFrame] = None,
    ) -> Graph:
        if hash_df is not None:
            nodes_hash = {
                node[self.node_column_name]: node[self.node_hash_column_name]
                for node in hash_df.collect()
            }
        else:
            nodes_hash = None

        return Graph(
            edges=[
                (
                    edge[self.source_column_name],
                    edge[self.destination_column_name],
                )
                for edge in graph_df.collect()
            ],
            nodes_hash=nodes_hash,
        )

    def _create_sub_graph_index_to_wl_sab_graphs_hash(
        self,
        graph: Graph,
        sub_graph_index_to_sub_graph_nodes: Dict[int, Set],
    ) -> Dict[int, Set[str]]:
        wl_sub_graphs_per_nodes = graph.get_wl_subgraph_hashes_per_nodes(
            iterations=self.wl_sub_graph_iterations,
        )

        sub_graph_index_to_wl_sab_graphs_hash = {}

        for sub_graph_index in (
                sub_graph_index_to_sub_graph_nodes.keys()
        ):
            wl_sub_graphs = set()
            for node in (
                    sub_graph_index_to_sub_graph_nodes[sub_graph_index]
            ):
                wl_sub_graphs.update(
                    set(wl_sub_graphs_per_nodes[node])
                )
            sub_graph_index_to_wl_sab_graphs_hash[sub_graph_index] = (
                wl_sub_graphs
            )

        return sub_graph_index_to_wl_sab_graphs_hash

    def transform(
        self,
        graph_df: DataFrame,
        hash_df: Optional[DataFrame] = None,
    ) -> Dict[str, Union[List[int], Dict[int, Set[int]], int, Dict[int, Set]]]:
        graph = self._create_graph(
            graph_df=graph_df,
            hash_df=hash_df,
        )

        connected_sub_graphs = graph.get_connected_components()

        sub_graph_index_to_sub_graph_nodes = (
            self._create_sub_graph_index_to_sub_graph_nodes(
                connected_sub_graphs=connected_sub_graphs,
            )
        )

        sub_graph_index_to_wl_sab_graphs_hash = (
            self._create_sub_graph_index_to_wl_sab_graphs_hash(
                graph=graph,
                sub_graph_index_to_sub_graph_nodes=(
                    sub_graph_index_to_sub_graph_nodes
                ),
            )
        )

        wl_sub_graph_vocab = self._get_wl_sub_graph_vocab(
            sub_graph_index_to_wl_sab_graphs_hash=(
                sub_graph_index_to_wl_sab_graphs_hash
            )
        )
        number_of_wl_sub_graphs = wl_sub_graph_vocab["number_of_wl_sub_graphs"]
        wl_hash_to_wl_index = wl_sub_graph_vocab["hash_to_int"]

        sub_graph_index_to_wl_indices = self._get_sub_graph_index_to_wl_indices(
            sub_graph_index_to_wl_sab_graphs_hash=(
                sub_graph_index_to_wl_sab_graphs_hash
            ),
            wl_hash_to_wl_index=wl_hash_to_wl_index,
        )

        positive_training_samples = self._create_positive_training_sample(
            sub_graph_index_to_wl_indices=sub_graph_index_to_wl_indices,
        )

        return {
            "positive_training_input": (
                positive_training_samples["positive_training_input"]
            ),
            "positive_training_target": (
                positive_training_samples["positive_training_target"]
            ),
            "sub_graph_index_to_wl_indices": sub_graph_index_to_wl_indices,
            "number_of_wl_sub_graphs": number_of_wl_sub_graphs,
            "sub_graph_index_to_sub_graph_nodes": (
                sub_graph_index_to_sub_graph_nodes
            ),
        }
