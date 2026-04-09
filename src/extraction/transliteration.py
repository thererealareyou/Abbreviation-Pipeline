from itertools import product
from typing import List


ENG_TO_RU: dict[str, list[str]] = {
    "A": ["эй", "а", "э", "ей", "эи", "еи", "ай"],
    "B": ["би", "б"],
    "C": ["си", "с", "к", "ц"],
    "D": ["ди", "д"],
    "E": ["и", "е", "э"],
    "F": ["эф", "ф"],
    "G": ["джи", "жи", "г"],
    "H": ["эйч", "эйтч", "аш", "х", "г"],
    "I": ["ай", "и"],
    "J": ["джей", "ж", "дж", "я"],
    "K": ["кей", "к"],
    "L": ["эл", "эль", "л"],
    "M": ["эм", "м"],
    "N": ["эн", "н"],
    "O": ["оу", "о", "а"],
    "P": ["пи", "п"],
    "Q": ["кью", "кю", "ку", "к"],
    "R": ["ар", "эр", "р"],
    "S": ["эс", "с", "з"],
    "T": ["ти", "т"],
    "U": ["ю", "у", "а"],
    "V": ["ви", "вэ", "в"],
    "W": ["ву", "в", "у"],
    "X": ["экс", "икс", "кс", "х"],
    "Y": ["уай", "вай", "й", "и"],
    "Z": ["зед", "зэд", "зи", "з"],
    "1": ["уан", "ван", "1"],
    "2": ["ту", "2"],
    "3": ["фри", "три", "3"],
    "4": ["фор", "фоур", "4"],
    "5": ["файв", "файф", "5"],
    "6": ["сикс", "6"],
    "7": ["сэвн", "7"],
    "8": ["эйт", "8"],
    "9": ["найн", "9"],
    "0": ["зиро", "зеро", "0"],
    "-": ["-", ""],
    " ": [" ", ""],
    "/": ["/", " ", ""],
}


def build_transliteration_map(
    abbreviations: List[str],
    max_abbr_len: int,
) -> dict[str, str]:
    """
    Принимает словарь аббревиатур {abbr: definition},
    возвращает словарь {ru_variant: eng_abbr} для всех заглавных
    латинских аббревиатур длиной <= max_abbr_len.
    """
    results: dict[str, str] = {}

    for abbr in sorted(abbreviations):
        if not abbr.isupper():
            continue
        if any("\u0400" <= ch <= "\u04ff" for ch in abbr):
            continue
        if len(abbr) > max_abbr_len:
            continue
        if not all(ch in ENG_TO_RU for ch in abbr):
            continue

        letters = [ENG_TO_RU[ch] for ch in abbr]
        for combo in product(*letters):
            ru_variant = "".join(combo)
            results[ru_variant] = abbr

    return results
