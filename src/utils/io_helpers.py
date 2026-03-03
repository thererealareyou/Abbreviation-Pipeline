import json
import pandas as pd
from collections import defaultdict


def parse_stringified_list(val: str) -> list[str]:
    """Превращает строку вида "['A', 'B']" в список и очищает от мусора.

    Args:
        val (str): Исходная строка, содержащая строковое представление списка.

    Returns:
        list[str]: Список очищенных строковых элементов, извлеченных из исходной строки.
    """

    if not isinstance(val, str):
        return []

    # Убираем скобки и кавычки
    cleaned_str = val.replace('[', '').replace(']', '').replace("'", "").replace('"', '')
    # Разбиваем по запятой, удаляем пробелы и оставляем только непустые строки
    return [item.strip() for item in cleaned_str.split(',') if len(item.strip()) > 1]


def export_df_to_json(df, mapping, output_filename: str = "output.json") -> dict:
    """Универсальная функция для сбора словарей из DataFrame в единый JSON.

    Считывает словари (с терминами и списками определений) из указанных колонок
    DataFrame, объединяет определения для одинаковых терминов (без дубликатов)
    и сохраняет результат в JSON-файл.

    Args:
        df (pd.DataFrame): DataFrame с результатами извлечения и определений.
        mapping (dict[str, str]): Словарь сопоставления, где ключ — желаемое имя корневого
            элемента в итоговом JSON, а значение — название соответствующей колонки
            в DataFrame (например, `{"terms": "term_definitions"}`).
        output_filename (str, optional): Имя выходного JSON-файла для сохранения.
            По умолчанию "output.json".

    Returns:
        dict: Итоговый словарь, сохраненный в JSON-файл. Содержит структуру вида:
            `{"Имя_ключа_в_JSON": {"термин": ["опр1", "опр2"], ...}}`.
    """

    final_data = {}

    for json_key, column_name in mapping.items():
        final_data[json_key] = defaultdict(list)

        if column_name not in df.columns:
            continue

        for row_dict in df[column_name].dropna():
            if not isinstance(row_dict, dict):
                continue

            for term, definitions in row_dict.items():
                for d in definitions:
                    if d not in final_data[json_key][term]:
                        final_data[json_key][term].append(d)

        final_data[json_key] = dict(final_data[json_key])

    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(final_data, f, ensure_ascii=False, indent=4)

    return final_data
