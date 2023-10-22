from typing import Dict, Set, List

import numpy as np

from question4.question4_config.negative_training_sample_provider import (
    CONFIDENCE_COEFFICIENT,
    NUMBER_OF_NEGATIVE_SAMPLES,
)


class NegativeSampleGenerator:
    def __init__(
        self,
        sub_graph_index_to_wl_indices: Dict[int, Set[int]],
        number_of_wl_sub_graphs: int,
        number_of_negative_samples: int = NUMBER_OF_NEGATIVE_SAMPLES,
        confidence_coefficient: int = CONFIDENCE_COEFFICIENT,
    ):
        self.sub_graph_index_to_wl_indices = sub_graph_index_to_wl_indices
        self.number_of_wl_sub_graphs = number_of_wl_sub_graphs
        self.number_of_negative_samples = number_of_negative_samples
        self.confidence_coefficient = confidence_coefficient

    def get_negative_training_sample(self) -> Dict[str, List[int]]:
        random_candidates = np.random.randint(
            low=1,
            high=self.number_of_wl_sub_graphs + 1,
            size=(
                len(self.sub_graph_index_to_wl_indices),
                self.confidence_coefficient * self.number_of_negative_samples,
            ),
        )

        negative_targets = np.array([
            np.array(list(
                set(training_sample_random_candidates)
                - set(self.sub_graph_index_to_graph_nodes[graph_index])
            )[:self.number_of_negative_samples])
            for graph_index, training_sample_random_candidates
            in zip(
                self.sub_graph_index_to_wl_indices.keys(),
                random_candidates,
            )
        ])

        negative_training_input = []
        for graph_index, negative_targets_for_graph in zip(
            self.sub_graph_index_to_wl_indices.keys(), negative_targets
        ):
            negative_training_input.extend([graph_index] * len(negative_targets_for_graph))

        return {
            "negative_training_input": negative_training_input,
            "negative_training_target": list(negative_targets.reshape(-1,)),
        }
