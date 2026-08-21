# utils.py
import re
from typing import Tuple

ROMAN_MAP = {
    "i": 1, "ii": 2, "iii": 3, "iv": 4, "v": 5,
    "vi": 6, "vii": 7, "viii": 8, "ix": 9, "x": 10
}

def detect_season_from_title(title: str) -> Tuple[str, int]:
    """
    Extracts base title and season number from anime titles.
    Examples:
      - "Attack on Titan Season 3" -> ("Attack on Titan", 3)
      - "Jujutsu Kaisen 2nd Season" -> ("Jujutsu Kaisen", 2)
      - "Overlord IV" -> ("Overlord", 4)
      - "Sousou no Frieren" -> ("Sousou no Frieren", 1)
    """
    if not title:
        return "", 1

    clean = title.strip()

    # Match: "Title Season 2", "Title S2"
    m = re.search(r'[\s:_-]+(?:season|s)[\s:_-]*(\d+)', clean, flags=re.IGNORECASE)
    if m:
        return clean[:m.start()].strip() or clean, int(m.group(1))

    # Match: "Title 2nd Season", "Title 3rd Season"
    m = re.search(r'[\s:_-]+(\d+)(?:st|nd|rd|th)\s+season', clean, flags=re.IGNORECASE)
    if m:
        return clean[:m.start()].strip() or clean, int(m.group(1))

    # Match: "Title Season II"
    m = re.search(r'[\s:_-]+season[\s:_-]+([ivx]+)\b', clean, flags=re.IGNORECASE)
    if m:
        roman = m.group(1).lower()
        if roman in ROMAN_MAP:
            return clean[:m.start()].strip() or clean, ROMAN_MAP[roman]

    # Match: "Title Part 2" or "Title Cour 2"
    m = re.search(r'[\s:_-]+(?:part|cour)[\s:_-]*(\d+)', clean, flags=re.IGNORECASE)
    if m:
        return clean[:m.start()].strip() or clean, int(m.group(1))

    # Match trailing Roman numerals (e.g. "Mob Psycho 100 II")
    m = re.search(r'[\s:_-]+([ivx]+)$', clean, flags=re.IGNORECASE)
    if m:
        roman = m.group(1).lower()
        if roman in ROMAN_MAP and roman != "i":
            return clean[:m.start()].strip() or clean, ROMAN_MAP[roman]

    return clean, 1
