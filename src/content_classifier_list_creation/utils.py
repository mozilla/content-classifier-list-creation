"""Utility functions."""

import os
import uuid
from pathlib import Path
from urllib.parse import urlparse

import requests


def download_file(url, dest_dir):
    """Download a file from url to dest_dir. Returns the local file path."""
    parsed = urlparse(url)
    filename = Path(parsed.path).name or str(uuid.uuid4())
    dest_path = os.path.join(dest_dir, filename)

    print(f"  Downloading {url} ...")
    resp = requests.get(url, stream=True, timeout=30)
    resp.raise_for_status()

    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

    size = os.path.getsize(dest_path)
    print(f"  Downloaded {filename} ({size} bytes)")
    return dest_path
