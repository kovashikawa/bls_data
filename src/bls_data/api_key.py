"""BLS API key rotation — loads keys from .env with BLS_API_KEY_ prefix."""

import os
import random

from dotenv import load_dotenv

load_dotenv()


def get_random_bls_key() -> str:
    """Return a random BLS API key from environment variables prefixed BLS_API_KEY_."""
    bls_keys = [
        value
        for key, value in os.environ.items()
        if key.startswith("BLS_API_KEY_") and value
    ]
    if not bls_keys:
        raise ValueError("No BLS API keys found in environment (prefix: BLS_API_KEY_)")
    return random.choice(bls_keys)  # noqa: S311