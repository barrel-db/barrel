#!/usr/bin/env python3
"""
Async cross-encoder reranking server for barrel_vectordb.

Uses the same async pattern as barrel_embed for handling multiple concurrent
requests with fire-and-forget task dispatch.

Cross-encoders score query-document pairs directly, providing more accurate
relevance scores than bi-encoder similarity for reranking candidate results.

Communicates via stdin/stdout using JSON lines (one JSON object per line).

Requirements:
    pip install transformers torch

Optional for better performance:
    pip install uvloop

Usage:
    python rerank_server.py [model_name]

Protocol:
    Request (stdin):
        {"id": 1, "action": "info"}
        {"id": 2, "action": "rerank", "query": "search query", "documents": ["doc1", "doc2", ...]}
        {"id": 3, "action": "rerank", "query": "search query", "documents": ["doc1", ...], "top_k": 5}

    Response (stdout):
        {"id": 1, "ok": true, "model": "cross-encoder/ms-marco-MiniLM-L-6-v2", "type": "rerank"}
        {"id": 2, "ok": true, "results": [{"index": 0, "score": 0.95}, {"index": 2, "score": 0.82}, ...]}
        {"id": 3, "ok": false, "error": "error message"}

Supported models:
    - cross-encoder/ms-marco-MiniLM-L-6-v2 (default, fast, good quality)
    - cross-encoder/ms-marco-MiniLM-L-12-v2 (better quality, slower)
    - BAAI/bge-reranker-base (good quality)
    - BAAI/bge-reranker-large (best quality, slowest)
"""

import sys
import json
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

# Try uvloop for better performance
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    _USING_UVLOOP = True
except ImportError:
    _USING_UVLOOP = False

# Configure logging to stderr so it doesn't interfere with JSON output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

# Default model
DEFAULT_MODEL = "cross-encoder/ms-marco-MiniLM-L-6-v2"


class RerankServer:
    """Async cross-encoder reranking server.

    Handles concurrent rerank requests using asyncio with CPU-bound model
    inference offloaded to a thread pool.
    """

    def __init__(self, model_name: str, max_workers: int = 4):
        self.model_name = model_name
        self.model = None
        self.tokenizer = None
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.write_lock = asyncio.Lock()
        self._max_workers = max_workers

    def load_model(self) -> bool:
        """Load the cross-encoder model and tokenizer synchronously at startup."""
        try:
            import torch
            from transformers import AutoModelForSequenceClassification, AutoTokenizer

            logger.info(f"Loading cross-encoder model: {self.model_name}")

            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
            self.model.eval()

            logger.info("Model loaded successfully")
            return True

        except ImportError as e:
            logger.error(f"Missing dependency: {e}. Run: pip install transformers torch")
            return False
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            return False

    async def run(self):
        """Main async event loop - read requests, dispatch, write responses."""
        loop = asyncio.get_event_loop()

        # Setup async stdin reader
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)

        # Setup async stdout writer
        writer_transport, writer_protocol = await loop.connect_write_pipe(
            asyncio.streams.FlowControlMixin, sys.stdout
        )
        writer = asyncio.StreamWriter(
            writer_transport, writer_protocol, reader, loop
        )

        logger.info(f"Async rerank server ready (uvloop={_USING_UVLOOP}, workers={self._max_workers})")

        # Process requests
        while True:
            line = await reader.readline()
            if not line:
                break
            # Fire and forget - handle_request manages its own response
            asyncio.create_task(self.handle_request(line, writer))

    async def handle_request(self, line: bytes, writer):
        """Handle a single request asynchronously."""
        request_id = None
        try:
            request = json.loads(line.decode())
            request_id = request.get("id")
            response = await self.dispatch(request)
        except Exception as e:
            logger.error(f"Error handling request: {e}")
            response = {"ok": False, "error": str(e)}

        # Always include request ID in response
        if request_id is not None:
            response["id"] = request_id

        # Serialize writes to prevent interleaving
        async with self.write_lock:
            output = json.dumps(response) + "\n"
            writer.write(output.encode())
            await writer.drain()

    async def dispatch(self, request: dict) -> dict:
        """Dispatch request to appropriate handler."""
        action = request.get("action")

        if action == "info":
            return self.handle_info()
        elif action == "rerank":
            return await self.handle_rerank(request)
        else:
            return {"ok": False, "error": f"Unknown action: {action}"}

    def handle_info(self) -> dict:
        """Handle info request."""
        return {
            "ok": True,
            "model": self.model_name,
            "type": "rerank"
        }

    async def handle_rerank(self, request: dict) -> dict:
        """Handle rerank request - run CPU-bound work in thread pool."""
        query = request.get("query", "")
        documents = request.get("documents", [])
        top_k = request.get("top_k")

        # Validate inputs
        if not isinstance(query, str):
            return {"ok": False, "error": "query must be a string"}

        if not isinstance(documents, list):
            return {"ok": False, "error": "documents must be a list"}

        if len(documents) == 0:
            return {"ok": True, "results": []}

        # Validate all documents are strings
        for i, doc in enumerate(documents):
            if not isinstance(doc, str):
                return {"ok": False, "error": f"documents[{i}] must be a string"}

        # Run CPU-bound reranking in thread pool
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor,
            self.rerank_sync,
            query, documents, top_k
        )

    def rerank_sync(self, query: str, documents: list, top_k: int = None) -> dict:
        """Synchronous reranking (runs in thread pool)."""
        import torch

        try:
            # Create query-document pairs
            pairs = [[query, doc] for doc in documents]

            # Tokenize all pairs
            inputs = self.tokenizer(
                pairs,
                padding=True,
                truncation=True,
                max_length=512,
                return_tensors="pt"
            )

            # Get scores
            with torch.no_grad():
                outputs = self.model(**inputs)
                # For sequence classification, logits shape is [batch_size, num_labels]
                # For reranking models, typically num_labels=1, so we squeeze
                if outputs.logits.shape[-1] == 1:
                    scores = outputs.logits.squeeze(-1)
                else:
                    # Some models output 2 classes (not relevant, relevant)
                    # Use the "relevant" class score
                    scores = outputs.logits[:, 1] if outputs.logits.shape[-1] == 2 else outputs.logits[:, 0]

            # Convert to list and pair with indices
            scores_list = scores.tolist()
            results = [{"index": i, "score": score} for i, score in enumerate(scores_list)]

            # Sort by score descending
            results.sort(key=lambda x: x["score"], reverse=True)

            # Apply top_k limit if specified
            if top_k is not None and top_k > 0:
                results = results[:top_k]

            return {"ok": True, "results": results}

        except Exception as e:
            return {"ok": False, "error": str(e)}


def main():
    """Main entry point."""
    # Get model name from command line or use default
    model_name = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_MODEL

    # Create server and load model
    server = RerankServer(model_name)
    if not server.load_model():
        print(json.dumps({"ok": False, "error": "Failed to load model"}), flush=True)
        sys.exit(1)

    # Run async event loop
    asyncio.run(server.run())


if __name__ == "__main__":
    main()
