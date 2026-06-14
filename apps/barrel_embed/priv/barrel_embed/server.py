"""Async embedding server base class."""

import asyncio
import json
import logging
import sys
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor

# Try uvloop for better performance
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    _USING_UVLOOP = True
except ImportError:
    _USING_UVLOOP = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)


class AsyncEmbedServer(ABC):
    """Base class for async embedding servers.

    Subclasses must implement:
        - load_model() -> bool
        - handle_info() -> dict
        - embed_sync(texts: list[str]) -> dict
    """

    def __init__(self, max_workers: int = 4):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.write_lock = asyncio.Lock()
        self._max_workers = max_workers

    async def run(self):
        """Main event loop - read requests, dispatch, write responses."""
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

        logger.info(f"Async server ready (uvloop={_USING_UVLOOP}, workers={self._max_workers})")

        # Process requests
        while True:
            line = await reader.readline()
            if not line:
                break
            # Fire and forget - handle_request manages its own response
            asyncio.create_task(self.handle_request(line, writer))

    async def handle_request(self, line: bytes, writer):
        """Handle a single request."""
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
        elif action == "embed":
            texts = request.get("texts", [])
            return await self.handle_embed(texts)
        else:
            return {"ok": False, "error": f"Unknown action: {action}"}

    async def handle_embed(self, texts: list) -> dict:
        """Handle embed request - run in thread pool."""
        if not isinstance(texts, list):
            return {"ok": False, "error": "texts must be a list"}
        if len(texts) == 0:
            return {"ok": True, "embeddings": []}

        # Validate all texts are strings
        for i, text in enumerate(texts):
            if not isinstance(text, str):
                return {"ok": False, "error": f"texts[{i}] must be a string"}

        loop = asyncio.get_event_loop()
        # Run CPU-bound embedding in thread pool
        result = await loop.run_in_executor(self.executor, self.embed_sync, texts)
        return result

    @abstractmethod
    def load_model(self) -> bool:
        """Load the embedding model. Return True on success."""
        pass

    @abstractmethod
    def handle_info(self) -> dict:
        """Return model info as dict with 'ok': True."""
        pass

    @abstractmethod
    def embed_sync(self, texts: list) -> dict:
        """Synchronous embedding. Return dict with 'ok' and 'embeddings'."""
        pass
