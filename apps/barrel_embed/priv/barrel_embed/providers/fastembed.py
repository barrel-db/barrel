"""FastEmbed embedding provider."""

from ..server import AsyncEmbedServer, logger

DEFAULT_MODEL = "BAAI/bge-small-en-v1.5"


class FastEmbedServer(AsyncEmbedServer):
    """Async server for FastEmbed models."""

    def __init__(self, model_name: str = None, max_workers: int = 4):
        super().__init__(max_workers=max_workers)
        self.model_name = model_name or DEFAULT_MODEL
        self.model = None
        self.dimension = None

    def load_model(self) -> bool:
        """Load the FastEmbed model."""
        try:
            from fastembed import TextEmbedding

            logger.info(f"Loading FastEmbed model: {self.model_name}")
            self.model = TextEmbedding(model_name=self.model_name)
            # Get dimension by embedding a test string
            test_embedding = list(self.model.embed(["test"]))[0]
            self.dimension = len(test_embedding)
            logger.info(f"Model loaded. Dimension: {self.dimension}")
            return True
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            return False

    def handle_info(self) -> dict:
        """Return model info."""
        return {
            "ok": True,
            "dimensions": self.dimension,
            "model": self.model_name,
            "backend": "fastembed",
        }

    def embed_sync(self, texts: list) -> dict:
        """Synchronous embedding using FastEmbed."""
        try:
            # FastEmbed returns a generator, convert to list
            embeddings = list(self.model.embed(texts))
            # Convert numpy arrays to lists
            embeddings_list = [emb.tolist() for emb in embeddings]
            return {"ok": True, "embeddings": embeddings_list}
        except Exception as e:
            return {"ok": False, "error": str(e)}
