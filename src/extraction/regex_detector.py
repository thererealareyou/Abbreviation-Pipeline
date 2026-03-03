import re
import json
from typing import List

def abbr_in_text(abbr: str, text: str) -> bool:
    """
    Проверяем, что аббревиатура реально встречается в тексте как отдельная единица.
    Работает и для вариантов с пробелами, как 'ЗО КИИ'.
    """
    pattern = r"(?<![0-9A-Za-zА-Яа-яЁё])" + re.escape(abbr) + r"(?![0-9A-Za-zА-Яа-яЁё])"
    return re.search(pattern, text) is not None


def clean_abbr_list(text: str, raw_response: str) -> List[str]:
    ABBR_RE = re.compile(r"[A-ZА-ЯЁ0-9][A-ZА-ЯЁ0-9/\- ]{1,19}")

    m = re.search(r'\[.*?\]', raw_response, re.DOTALL)
    if not m:
        return []
    try:
        data = json.loads(m.group())
    except json.JSONDecodeError:
        return []

    result = set()
    for item in data:
        if not isinstance(item, str):
            continue
        abbr = item.strip()
        if not abbr:
            continue

        if not ABBR_RE.fullmatch(abbr):
            continue

        if not abbr_in_text(abbr, text):
            continue

        result.add(abbr)

    return sorted(result)

def is_pure_abbreviation(candidate: str, threshold: float = 0.6) -> bool:
    """
    Возвращает True, если кандидат — аббревиатура.
    Логика: если доля заглавных букв >= threshold, считаем аббревиатурой.

    Примеры:
    - "ТД ИТ"         → 4/4  = 100% → аббревиатура
    - "YouTube"        → 1/7  = 14%  → термин
    - "Владелец ТД ИТ" → 3/12 = 25%  → термин
    - "BSS"            → 3/3  = 100% → аббревиатура
    - "КИПиА"          → 4/5  = 80%  → аббревиатура
    """
    letters = [c for c in candidate if c.isalpha()]
    if not letters:
        return False
    upper_ratio = sum(1 for c in letters if c.isupper()) / len(letters)
    return upper_ratio >= threshold


def term_in_text(term: str, text: str) -> bool:
    """
    Проверяем, что термин реально встречается в тексте как отдельная единица.
    """
    pattern = r"(?<![0-9A-Za-zА-Яа-яЁё])" + re.escape(term) + r"(?![0-9A-Za-zА-Яа-яЁё])"
    return re.search(pattern, text) is not None


def clean_terms_list(text: str, raw_response: str, threshold: float = 0.6) -> List[str]:
    # Термин: начинается с заглавной, содержит буквы и пробелы, до 60 символов
    TERM_RE = re.compile(r"[А-ЯЁA-Z][А-Яа-яЁёA-Za-z0-9/\- ]{1,59}")

    m = re.search(r'\[.*?\]', raw_response, re.DOTALL)
    if not m:
        return []
    try:
        data = json.loads(m.group())
    except json.JSONDecodeError:
        return []

    result = set()
    for item in data:
        if not isinstance(item, str):
            continue
        term = item.strip()
        if not term:
            continue

        # Соответствует шаблону термина
        if not TERM_RE.fullmatch(term):
            continue

        # Не является чистой аббревиатурой
        if is_pure_abbreviation(term, threshold):
            continue

        # Реально встречается в тексте
        if not term_in_text(term, text):
            continue

        result.add(term)

    return sorted(result)
