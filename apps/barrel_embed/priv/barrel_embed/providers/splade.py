"""SPLADE sparse embedding provider."""

from ..server import AsyncEmbedServer, logger

DEFAULT_MODEL = "prithivida/Splade_PP_en_v1"


class SpladeServer(AsyncEmbedServer):
    """Async server for SPLADE sparse embedding models."""

    def __init__(self, model_name: str = None, max_workers: int = 4):
        super().__init__(max_workers=max_workers)
        self.model_name = model_name or DEFAULT_MODEL
        self.model = None
        self.tokenizer = None
        self.vocab_size = None

    def load_model(self) -> bool:
        """Load the SPLADE model and tokenizer."""
        try:
            import torch
            from transformers import AutoModelForMaskedLM, AutoTokenizer

            logger.info(f"Loading SPLADE model: {self.model_name}")

            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            self.model = AutoModelForMaskedLM.from_pretrained(self.model_name)
            self.model.eval()

            self.vocab_size = self.tokenizer.vocab_size
            logger.info(f"Model loaded. Vocab size: {self.vocab_size}")
            return True
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            return False

    def handle_info(self) -> dict:
        """Return model info."""
        return {
            "ok": True,
            "vocab_size": self.vocab_size,
            "model": self.model_name,
            "type": "sparse",
            "backend": "splade",
        }

    def embed_sync(self, texts: list) -> dict:
        """Synchronous sparse embedding using SPLADE."""
        try:
            import torch

            # Tokenize
            inputs = self.tokenizer(
                texts,
                padding=True,
                truncation=True,
                max_length=512,
                return_tensors="pt"
            )

            # Get model output
            with torch.no_grad():
                output = self.model(**inputs)

            # SPLADE: log(1 + ReLU(logits)) * attention_mask, then max pool
            logits = output.logits
            relu_log = torch.log1p(torch.relu(logits))

            # Apply attention mask
            attention_mask = inputs["attention_mask"].unsqueeze(-1)
            weighted = relu_log * attention_mask

            # Max pooling over sequence length
            sparse_vecs, _ = torch.max(weighted, dim=1)

            # Convert to sparse format (indices and values)
            results = []
            for vec in sparse_vecs:
                # Get non-zero indices and values
                non_zero_mask = vec > 0
                indices = torch.where(non_zero_mask)[0].tolist()
                values = vec[non_zero_mask].tolist()

                results.append({
                    "indices": indices,
                    "values": values
                })

            return {"ok": True, "embeddings": results}
        except Exception as e:
            return {"ok": False, "error": str(e)}
