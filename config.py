"""Persist user settings between sessions."""
import json
from pathlib import Path

_CONFIG_DIR  = Path.home() / ".config" / "findethedox"
_CONFIG_FILE = _CONFIG_DIR / "config.json"


def load() -> dict:
    try:
        return json.loads(_CONFIG_FILE.read_text())
    except Exception:
        return {}


def save(data: dict) -> None:
    _CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    _CONFIG_FILE.write_text(json.dumps(data, indent=2))
