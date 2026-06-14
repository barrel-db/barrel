"""CLIP image/text embedding provider."""

import base64
from io import BytesIO

from ..server import AsyncEmbedServer, logger

DEFAULT_MODEL = "openai/clip-vit-base-patch32"


class CLIPServer(AsyncEmbedServer):
    """Async server for CLIP image/text models."""

    def __init__(self, model_name: str = None, max_workers: int = 4):
        super().__init__(max_workers=max_workers)
        self.model_name = model_name or DEFAULT_MODEL
        self.model = None
        self.processor = None
        self.dimension = None

    def load_model(self) -> bool:
        """Load the CLIP model and processor."""
        try:
            import torch
            from transformers import CLIPModel, CLIPProcessor

            logger.info(f"Loading CLIP model: {self.model_name}")

            self.processor = CLIPProcessor.from_pretrained(self.model_name)
            self.model = CLIPModel.from_pretrained(self.model_name)
            self.model.eval()

            # Get dimension from model config
            self.dimension = self.model.config.projection_dim

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
            "type": "image",
            "backend": "clip",
        }

    async def dispatch(self, request: dict) -> dict:
        """Dispatch request to appropriate handler."""
        import asyncio

        action = request.get("action")

        if action == "info":
            return self.handle_info()
        elif action == "embed" or action == "embed_text":
            texts = request.get("texts", [])
            return await self.handle_embed(texts)
        elif action == "embed_image":
            images = request.get("images", [])
            return await self.handle_embed_image(images)
        else:
            return {"ok": False, "error": f"Unknown action: {action}"}

    async def handle_embed_image(self, images: list) -> dict:
        """Handle image embed request - run in thread pool."""
        if not isinstance(images, list):
            return {"ok": False, "error": "images must be a list"}
        if len(images) == 0:
            return {"ok": True, "embeddings": []}

        # Validate all images are strings (base64)
        for i, img in enumerate(images):
            if not isinstance(img, str):
                return {"ok": False, "error": f"images[{i}] must be a base64 string"}

        import asyncio
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(self.executor, self.embed_images_sync, images)
        return result

    def embed_sync(self, texts: list) -> dict:
        """Synchronous text embedding using CLIP."""
        try:
            import torch

            results = []
            for text in texts:
                # Process text
                inputs = self.processor(
                    text=text, return_tensors="pt", padding=True, truncation=True
                )

                # Get text embedding
                with torch.no_grad():
                    text_features = self.model.get_text_features(**inputs)
                    # Normalize
                    text_features = text_features / text_features.norm(p=2, dim=-1, keepdim=True)

                results.append(text_features[0].tolist())

            return {"ok": True, "embeddings": results}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def embed_images_sync(self, images: list) -> dict:
        """Synchronous image embedding using CLIP."""
        try:
            import torch
            from PIL import Image

            results = []
            for image_data in images:
                # Decode base64 image
                image_bytes = base64.b64decode(image_data)
                image = Image.open(BytesIO(image_bytes)).convert("RGB")

                # Process image
                inputs = self.processor(images=image, return_tensors="pt")

                # Get image embedding
                with torch.no_grad():
                    image_features = self.model.get_image_features(**inputs)
                    # Normalize
                    image_features = image_features / image_features.norm(p=2, dim=-1, keepdim=True)

                results.append(image_features[0].tolist())

            return {"ok": True, "embeddings": results}
        except Exception as e:
            return {"ok": False, "error": str(e)}
