from typing import List, Generator

import numpy as np
from torch import optim, nn, Tensor
from tqdm import tqdm

from question4.application.embedder_model_provider import (
    Graph2VecModel,
    Graph2VecLoss,
)
from question4.application.negative_training_sample_provider import (
    NegativeSampleGenerator,
)
from question4.config.trainer_provider import DEVICE


class BatchGenerator:
    def __init__(
        self,
        batch_size: int,
    ):
        self.batch_size = batch_size

    def get_batches(
        self,
        input_vector: List[int],
        target_vector: List[int],
        signs: List[int],
    ) -> Generator:
        number_of_samples = len(input_vector)
        permutation = np.random.permutation(number_of_samples)

        permutate_input_vector = np.array(input_vector)[permutation]
        permutate_target_vector = np.array(target_vector)[permutation]
        permutate_signs = np.array(signs)[permutation]

        number_of_batches = (
            int(number_of_samples / self.batch_size)
            if number_of_samples % self.batch_size == 0
            else int(number_of_samples / self.batch_size) + 1
        )
        for idx in range(number_of_batches):
            yield (
                permutate_input_vector[idx * self.batch_size: (idx + 1) * self.batch_size],
                permutate_target_vector[idx * self.batch_size: (idx + 1) * self.batch_size],
                permutate_signs[idx * self.batch_size: (idx + 1) * self.batch_size],
            )


class OptimizerGenerator:
    def __init__(
        self,
        initial_learning_rate: float,
        model: nn.Module,
        c: int = 3,
    ):
        self.step = 0
        self.initial_learning_rate = initial_learning_rate
        self.model = model
        self.c = c

    def __iter__(self):
        return self

    def __next__(self) -> optim:
        return_object = optim.Adam(
            self.model.parameters(),
            lr=self.initial_learning_rate / (1 + self.step/self.c),
        )
        self.step += 1

        return return_object


def train(
    model: Graph2VecModel,
    positive_training_input: List[int],
    positive_training_target: List[int],
    loss_calculator: Graph2VecLoss,
    batch_generator: BatchGenerator,
    optimizer_generator: OptimizerGenerator,
    epochs: int,
    batch_size: int,
    negative_sample_generator: NegativeSampleGenerator,
    device: str = DEVICE,
) -> Graph2VecModel:
    model.to(device)
    model.train()
    for epoch in range(1, epochs + 1):
        negative_samples = (
            negative_sample_generator.get_negative_training_sample()
        )
        negative_training_input, negative_training_target = (
            negative_samples["negative_training_input"],
            negative_samples["negative_training_target"],
        )

        optimizer = next(optimizer_generator)

        training_input = positive_training_input + negative_training_input
        training_target = positive_training_target + negative_training_target
        signs = (
            [1] * len(positive_training_input)
            + [-1] * len(negative_training_input)
        )

        epoch_losses = []

        total = (
            int(len(training_target) / batch_size)
            if len(training_target) % batch_size == 0
            else int(len(training_target) / batch_size) + 1
        )
        t = tqdm(
            batch_generator.get_batches(
                input_vector=training_input,
                target_vector=training_target,
                signs=signs,
            ),
            desc="Epoch:",
            leave=True,
            total=total,
        )

        for (batched_input, batched_target, batched_signs) in t:
            batched_input = Tensor(batched_input).int().to(device)
            batched_target = Tensor(batched_target).int().to(device)
            batched_signs = Tensor(batched_signs).int().to(device)

            input_output = model.input_forward(batched_input)
            target_output = model.target_forward(batched_target)

            loss = loss_calculator(
                embedded_input=input_output,
                embedded_target=target_output,
                samples_sign=batched_signs,
            )
            epoch_losses.append(loss.item())
            t.set_description(f"Epoch: {epoch}, Loss: {np.mean(epoch_losses)}")
            t.refresh()

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            del (
                batched_input,
                batched_target,
                batched_signs,
                input_output,
                target_output,
            )
    model.eval()
    model.to("cpu")

    return model
