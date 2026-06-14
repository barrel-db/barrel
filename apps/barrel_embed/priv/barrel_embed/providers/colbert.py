"""ColBERT multi-vector embedding provider."""

from ..server import AsyncEmbedServer, logger

DEFAULT_MODEL = "colbert-ir/colbertv2.0"


class ColBERTServer(AsyncEmbedServer):
    """Async server for ColBERT multi-vector models."""

    def __init__(self, model_name: str = None, max_workers: int = 4):
        super().__init__(max_workers=max_workers)
        self.model_name = model_name or DEFAULT_MODEL
        self.model = None
        self.tokenizer = None
        self.dimension = None
        self.linear = None  # ColBERT projection layer

    def load_model(self) -> bool:
        """Load the ColBERT model and tokenizer."""
        try:
            import torch
            from transformers import AutoModel, AutoTokenizer

            logger.info(f"Loading ColBERT model: {self.model_name}")

            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            self.model = AutoModel.from_pretrained(self.model_name)
            self.model.eval()

            # Get dimension from model config
            hidden_size = self.model.config.hidden_size

            # ColBERT typically projects to 128 dimensions
            # Check if model has a linear projection layer
            if hasattr(self.model, 'linear'):
                self.linear = self.model.linear
                self.dimension = self.linear.out_features
            else:
                # Default ColBERT dimension
                self.dimension = min(128, hidden_size)
                # Create projection if needed
                if hidden_size != self.dimension:
                    self.linear = torch.nn.Linear(hidden_size, self.dimension, bias=False)
                    torch.nn.init.xavier_uniform_(self.linear.weight)

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
            "type": "multi_vector",
            "backend": "colbert",
        }

    def embed_sync(self, texts: list) -> dict:
        """Synchronous multi-vector embedding using ColBERT."""
        try:
            import torch
            import torch.nn.functional as F

            results = []

            for text in texts:
                # Tokenize single text
                inputs = self.tokenizer(
                    text,
                    padding=True,
                    truncation=True,
                    max_length=512,
                    return_tensors="pt"
                )

                # Get model output
                with torch.no_grad():
                    output = self.model(**inputs)

                # Get token embeddings (last hidden state)
                token_embeddings = output.last_hidden_state[0]  # [seq_len, hidden_size]

                # Apply projection if needed
                if self.linear is not None:
                    token_embeddings = self.linear(token_embeddings)

                # Normalize embeddings (ColBERT uses L2 normalization)
                token_embeddings = F.normalize(token_embeddings, p=2, dim=-1)

                # Get attention mask to filter padding tokens
                attention_mask = inputs["attention_mask"][0]

                # Filter out padding tokens and special tokens ([CLS], [SEP])
                # Keep only actual content tokens
                valid_tokens = []
                for i, (emb, mask) in enumerate(zip(token_embeddings, attention_mask)):
                    if mask == 1:  # Not padding
                        valid_tokens.append(emb.tolist())

                results.append(valid_tokens)

            return {"ok": True, "embeddings": results}
        except Exception as e:
            return {"ok": False, "error": str(e)}
