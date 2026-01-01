import json
import random
from pathlib import Path


class CharacterManager:
    """Encapsulates cached character data to avoid module-level globals."""

    def __init__(self) -> None:
        self._characters: list[dict] | None = None
        self._id_index: dict[int, dict] | None = None

    def load_characters(self) -> list[dict]:
        """Load character pool from disk once."""
        if self._characters is None:
            data_path = Path(__file__).resolve().parent / "characters.json"
            with data_path.open("r", encoding="utf-8") as f:
                self._characters = json.load(f)
        if self._id_index is None:
            self._id_index = {
                c.get("id"): c
                for c in self._characters
                if isinstance(c, dict) and c.get("id") is not None
            }
        return self._characters

    def get_random_character(self, limit=None):
        """Return a random character dict, or None if pool empty."""
        chars = self.load_characters()
        if not chars:
            return None
        if limit:
            chars = chars[:limit]
        return random.choice(chars)

    def get_character_by_id(self, id):
        """O(1) lookup via cached id index; builds index on first use."""
        try:
            cid = int(id)
        except Exception:
            return None
        if self._id_index is None:
            self.load_characters()
        return self._id_index.get(cid)

    def search_characters_by_name(self, keyword: str) -> list[dict]:
        """Return characters whose name contains the keyword (case-insensitive)."""
        if not keyword:
            return []
        key_lower = str(keyword).lower()
        chars = self.load_characters()
        if not chars:
            return []
        return [c for c in chars if key_lower in str(c.get("name", "")).lower()]

