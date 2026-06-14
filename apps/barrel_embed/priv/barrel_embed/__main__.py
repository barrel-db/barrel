#!/usr/bin/env python3
"""Entry point for barrel_embed server.

Usage:
    python -m barrel_embed --provider sentence_transformers --model BAAI/bge-base-en-v1.5
    python -m barrel_embed --provider fastembed
    python -m barrel_embed --provider splade
"""

import argparse
import asyncio
import sys


def main():
    parser = argparse.ArgumentParser(description="Embedding server for barrel_embed")
    parser.add_argument(
        "--provider",
        choices=["sentence_transformers", "fastembed", "splade", "colbert", "clip"],
        default="sentence_transformers",
        help="Embedding provider to use"
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Model name (provider-specific default if not specified)"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Thread pool size for CPU-bound work"
    )

    args = parser.parse_args()

    # Import and instantiate the appropriate provider
    if args.provider == "sentence_transformers":
        from .providers.sentence_transformers import SentenceTransformerServer
        server = SentenceTransformerServer(
            model_name=args.model,
            max_workers=args.max_workers
        )
    elif args.provider == "fastembed":
        from .providers.fastembed import FastEmbedServer
        server = FastEmbedServer(
            model_name=args.model,
            max_workers=args.max_workers
        )
    elif args.provider == "splade":
        from .providers.splade import SpladeServer
        server = SpladeServer(
            model_name=args.model,
            max_workers=args.max_workers
        )
    elif args.provider == "colbert":
        from .providers.colbert import ColBERTServer
        server = ColBERTServer(
            model_name=args.model,
            max_workers=args.max_workers
        )
    elif args.provider == "clip":
        from .providers.clip import CLIPServer
        server = CLIPServer(
            model_name=args.model,
            max_workers=args.max_workers
        )
    else:
        print(f'{{"ok": false, "error": "Unknown provider: {args.provider}"}}', flush=True)
        sys.exit(1)

    # Load model and run
    if not server.load_model():
        print('{"ok": false, "error": "Failed to load model"}', flush=True)
        sys.exit(1)

    asyncio.run(server.run())


if __name__ == "__main__":
    main()
