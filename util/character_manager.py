import json
import random
from pathlib import Path

_characters: list[dict] | None = None
_id_index: dict[int, dict] | None = None


def load_characters() -> list[dict]:
    """Load character pool from disk once."""
    global _characters, _id_index
    if _characters is None:
        data_path = Path(__file__).resolve().parent.parent / "data" / "characters.json"
        with data_path.open("r", encoding="utf-8") as f:
            _characters = json.load(f)
    if _id_index is None:
        _id_index = {c.get("id"): c for c in _characters if isinstance(c, dict) and c.get("id") is not None}
    return _characters


def get_random_character(limit=None):
    """Return a random character dict, or None if pool empty."""
    chars = load_characters()
    if not chars:
        return None
    if limit:
        chars = chars[:limit]
    return random.choice(chars)


def get_character_by_id(id):
    """O(1) lookup via cached id index; builds index on first use."""
    global _id_index
    try:
        cid = int(id)
    except Exception:
        return None
    if _id_index is None:
        load_characters()
    return _id_index.get(cid)


def search_characters_by_name(keyword: str) -> list[dict]:
    """Return characters whose name contains the keyword (case-insensitive)."""
    if not keyword:
        return []
    key_lower = str(keyword).lower()
    chars = load_characters()
    if not chars:
        return []
    return [c for c in chars if key_lower in str(c.get("name", "")).lower()]
