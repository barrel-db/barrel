"""Sentence-transformers embedding provider."""

from ..server import AsyncEmbedServer, logger

DEFAULT_MODEL = "BAAI/bge-base-en-v1.5"


class SentenceTransformerServer(AsyncEmbedServer):
    """Async server for sentence-transformers models."""

    def __init__(self, model_name: str = None, max_workers: int = 4):
        super().__init__(max_workers=max_workers)
        self.model_name = model_name or DEFAULT_MODEL
        self.model = None

    def load_model(self) -> bool:
        """Load the sentence transformer model."""
        try:
            from sentence_transformers import SentenceTransformer

            logger.info(f"Loading model: {self.model_name}")
            self.model = SentenceTransformer(self.model_name)
            logger.info(
                f"Model loaded. Dimension: {self.model.get_sentence_embedding_dimension()}"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            return False

    def handle_info(self) -> dict:
        """Return model info."""
        return {
            "ok": True,
            "dimensions": self.model.get_sentence_embedding_dimension(),
            "model": self.model_name,
            "backend": "sentence_transformers",
        }

    def embed_sync(self, texts: list) -> dict:
        """Synchronous embedding using sentence-transformers."""
        try:
            embeddings = self.model.encode(
                texts, normalize_embeddings=True, show_progress_bar=False
            )
            return {"ok": True, "embeddings": embeddings.tolist()}
        except Exception as e:
            return {"ok": False, "error": str(e)}
