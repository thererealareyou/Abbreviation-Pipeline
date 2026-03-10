import pandas as pd
from itertools import product
import json


eng_to_ru = {
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
    "/": ["/", " ", ""]
}

def form_corresponging_table(input_file: str,
                             max_abbr_len: int,
                             output_file: str) -> None:
    """
    Сохраняет таблицу с потенциальным написанием английский аббревиатур русскими символами (литерализация)
    :param input_file: Путь к файлу с аббревиатурами.
    :param max_abbr_len: Максимальная длина аббревиатуры.
    :param output_file: Путь к выходному json-файлу.
    """

    abbr_file = pd.read_json(input_file)
    keys_series = pd.Series(abbr_file["abbreviations"].keys())
    russian_abbrs = keys_series[keys_series.str.contains(r'[а-яА-ЯёЁ]', regex=True)]
    english_abbrs = pd.Series(list(set(keys_series) - set(russian_abbrs)))
    english_capitalized_abbrs = english_abbrs[english_abbrs.str.isupper()]

    results = {}

    for abbr in sorted(english_capitalized_abbrs):
        if len(abbr) <= max_abbr_len:
            letters = [eng_to_ru[char] for char in abbr]
            combinations = product(*letters)
            result = ["".join(combo) for combo in combinations]

            for res in result:
                results[res] = abbr

    results_df = pd.DataFrame(list(results.items()), columns=['ru_abbr', 'eng_abbr'])

    with open(output_file+'.json', 'w', encoding='utf-8') as f:
        f.write(results_df.to_json(orient='records', force_ascii=False, indent=4))

    with open(output_file+'_dict.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=4)