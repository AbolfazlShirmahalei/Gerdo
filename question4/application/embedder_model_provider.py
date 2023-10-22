from torch import nn, Tensor, bmm


class GraphEmbedder(nn.Module):
    def __init__(
        self,
        graph_embedder: nn.Embedding,
    ):
        super().__init__()
        self.graph_embedder = graph_embedder

    def forward(self, input_vector: Tensor) -> Tensor:
        return self.graph_embedder(input_vector)


class Graph2VecModel(nn.Module):
    def __init__(
        self,
        input_dim: int,
        target_dim: int,
        embedding_dim: int,
    ):
        super().__init__()

        self.input_dim = input_dim
        self.target_dim = target_dim

        self.input_embedding_layer = nn.Embedding(
            input_dim + 1,
            embedding_dim,
        )
        self.input_embedding_layer.weight.data.uniform_(-1, 1)

        self.target_embedding_layer = nn.Embedding(
            target_dim + 1,
            embedding_dim,
        )
        self.target_embedding_layer.weight.data.uniform_(-1, 1)

    def input_forward(
        self,
        input_vector: Tensor,
    ) -> Tensor:
        return self.input_embedding_layer(input_vector)

    def target_forward(
        self,
        target_vector: Tensor,
    ) -> Tensor:
        return self.target_embedding_layer(target_vector)

    def get_graph_embedder(self) -> GraphEmbedder:
        return GraphEmbedder(self.input_embedding_layer)


class Graph2VecLoss(nn.Module):
    def __init__(self, embedding_dim: int):
        super().__init__()

        self.embedding_dim = embedding_dim

    def forward(
        self,
        embedded_input: Tensor,
        embedded_target: Tensor,
        samples_sign: Tensor
    ):
        loss = -(
            bmm(
                embedded_input.view(-1, 1, self.embedding_dim),
                embedded_target.view(-1, self.embedding_dim, 1)
                * samples_sign,
            ).sigmoid().log().view(-1,).mean()
        )

        return loss
