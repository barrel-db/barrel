"""barrel_embed - Async embedding server for Erlang."""

__version__ = "0.2.0"

from .server import AsyncEmbedServer

__all__ = ["AsyncEmbedServer", "__version__"]
