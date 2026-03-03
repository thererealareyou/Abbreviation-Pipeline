import aiohttp
import json
import pandas as pd
from tqdm.asyncio import tqdm_asyncio
from src.extraction.model_client import AsyncAPIModelClient, parse_llm_definition_response
from src.extraction.regex_detector import clean_abbr_list, clean_terms_list
from src.dataset_builder.dataset_examiner import verify_expansion
from src.utils.io_helpers import parse_stringified_list


async def build_initial_dataframe_async(chunks: list, model: AsyncAPIModelClient, config: dict) -> pd.DataFrame:
    """Первый этап: Извлекаем списки аббревиатур и терминов.

    Args:
        chunks (list): Список текстовых фрагментов (чанков) для обработки.
        model (AsyncAPIModelClient): Асинхронный клиент для взаимодействия с LLM API.
        config (dict): Словарь с конфигурацией, содержащий инструкции для LLM
            (например, `config["llm"]["abbr"]["instructions"]`).

    Returns:
        pd.DataFrame: Датафрейм с результатами, где каждая строка содержит
            исходный текст (`chunk`), список аббревиатур (`abbrs`) и
            список терминов (`terms`).
    """
    print("\nПоиск аббревиатур и терминов (Пакетная обработка)...")
    rows = []

    async with aiohttp.ClientSession() as session:
        abbr_instr = config["llm"]["abbr"]["instructions"]
        abbr_tasks = [model.generate_async(session, f"{abbr_instr}\n{chunk}", stage="abbr") for chunk in chunks]

        term_instr = config["llm"]["term"]["instructions"]
        term_tasks = [model.generate_async(session, f"{term_instr}\n{chunk}", stage="term") for chunk in chunks]

        abbr_results = await tqdm_asyncio.gather(*abbr_tasks, desc="Abbrs")
        term_results = await tqdm_asyncio.gather(*term_tasks, desc="Terms")

    print("\nСборка базового DataFrame...")
    for chunk, raw_abbrs, raw_terms in zip(chunks, abbr_results, term_results):
        cleaned_abbr = clean_abbr_list(chunk, raw_abbrs)
        cleaned_term = clean_terms_list(chunk, raw_terms)
        if cleaned_abbr or cleaned_term:
            rows.append({"chunk": chunk, "abbrs": cleaned_abbr, "terms": cleaned_term})

    return pd.DataFrame(rows)


async def enrich_with_definitions_async(df: pd.DataFrame, model: AsyncAPIModelClient, config: dict) -> pd.DataFrame:
    """Второй этап: Поиск точных определений для извлеченных аббревиатур и терминов.

    Args:
        df (pd.DataFrame): Исходный датафрейм, содержащий текстовые фрагменты ('chunk')
            и списки извлеченных аббревиатур ('abbrs') и терминов ('terms').
        model (AsyncAPIModelClient): Асинхронный клиент для взаимодействия с LLM API.
        config (dict): Словарь с конфигурацией, содержащий инструкции и промпты для
            генерации определений (например, `config['llm']['def_abbr']['instructions']`).

    Returns:
        pd.DataFrame: Обогащенный датафрейм с добавленными столбцами:
            'abbr_definitions' (валидные определения аббревиатур),
            'term_definitions' (валидные определения терминов),
            'dropped_abbr' (отклоненные определения аббревиатур),
            'dropped_terms' (отклоненные определения терминов).
    """

    print("\nНачинаем строгое извлечение определений...")

    tasks = {'abbr': [], 'term': []}
    meta = {'abbr': [], 'term': []}

    async with aiohttp.ClientSession() as session:
        # 1. Формирование пула задач
        for idx, row in df.iterrows():
            chunk_text = row['chunk']
            abbrs = parse_stringified_list(row['abbrs'])
            terms = parse_stringified_list(row['terms'])

            for abbr in abbrs:
                prompt = config['llm']['def_abbr']['instructions'].format(ABBR=abbr, TEXT=chunk_text)
                tasks['abbr'].append(model.generate_async(session, prompt, stage="def_abbr"))
                meta['abbr'].append((idx, abbr, chunk_text))

            for term in terms:
                prompt = config['llm']['def_term']['instructions'].format(TERM=term, TEXT=chunk_text)
                tasks['term'].append(model.generate_async(session, prompt, stage="def_term"))
                meta['term'].append((idx, term, chunk_text))

        # 2. Выполнение задач
        print(f"\nЗапуск {len(tasks['abbr'])} задач для аббревиатур и {len(tasks['term'])} для терминов...")
        raw_results = {
            'abbr': await tqdm_asyncio.gather(*tasks['abbr'], desc="Def Abbrs") if tasks['abbr'] else [],
            'term': await tqdm_asyncio.gather(*tasks['term'], desc="Def Terms") if tasks['term'] else []
        }

    # 3. Словари для результатов
    results_by_row = {
        'abbr': {idx: {} for idx in df.index},
        'term': {idx: {} for idx in df.index},
        'dropped_abbr': {idx: {} for idx in df.index},
        'dropped_terms': {idx: {} for idx in df.index}
    }

    # Вспомогательная функция для распределения ответов
    def process_results(category, is_abbr=False):
        for (idx, item, chunk_text), res in zip(meta[category], raw_results[category]):
            valid_def = parse_llm_definition_response(res)

            if is_abbr and valid_def and not verify_expansion(abbr=item, expansion=valid_def, chunk_text=chunk_text):
                valid_def = None

            if valid_def:
                # Проверка на галлюцинации / дублирование
                if item in valid_def or (is_abbr and " — " in item):
                    drop_key = 'dropped_abbr' if is_abbr else 'dropped_terms'
                    results_by_row[drop_key].setdefault(idx, {}).setdefault(item, []).append(valid_def)
                    continue

                # Добавление валидного определения
                results_by_row[category].setdefault(idx, {}).setdefault(item, [])
                if valid_def not in results_by_row[category][idx][item]:
                    results_by_row[category][idx][item].append(valid_def)

    # 4. Обработка
    process_results('abbr', is_abbr=True)
    process_results('term', is_abbr=False)

    # 5. Запись в DataFrame
    df['abbr_definitions'] = df.index.map(results_by_row['abbr'])
    df['term_definitions'] = df.index.map(results_by_row['term'])
    df['dropped_abbr'] = df.index.map(results_by_row['dropped_abbr'])
    df['dropped_terms'] = df.index.map(results_by_row['dropped_terms'])

    return df


async def resolve_conflicts_async(conflict_abbrs: dict, conflict_terms: dict, model: AsyncAPIModelClient,
                                  config: dict) -> tuple[dict, dict]:
    """Третий этап: Разрешение конфликтов для аббревиатур и терминов параллельно.

    Args:
        conflict_abbrs (dict): Словарь с конфликтующими определениями аббревиатур,
            где ключ — аббревиатура, а значение — список вариантов ее определений.
        conflict_terms (dict): Словарь с конфликтующими определениями терминов,
            где ключ — термин, а значение — список вариантов его определений.
        model (AsyncAPIModelClient): Асинхронный клиент для взаимодействия с LLM API.
        config (dict): Словарь с конфигурацией, содержащий инструкции для LLM
            по разрешению конфликтов (например, `config["llm"]["resolve_abbr"]["instructions"]`).

    Returns:
        tuple[dict, dict]: Кортеж из двух словарей `(resolved_abbrs, resolved_terms)`:

            - `resolved_abbrs` (dict): словарь с итоговыми (разрешенными) определениями аббревиатур.
            - `resolved_terms` (dict): словарь с итоговыми (разрешенными) определениями терминов.
    """

    print(
        f"\nЗапуск 3 этапа: Разрешение конфликтов ({len(conflict_abbrs)} аббревиатур, {len(conflict_terms)} терминов)...")

    resolved_abbrs = {}
    resolved_terms = {}

    # Если конфликтов нет вообще, возвращаем пустые словари
    if not conflict_abbrs and not conflict_terms:
        return resolved_abbrs, resolved_terms

    tasks = {'abbr': [], 'term': []}
    meta = {'abbr': [], 'term': []}  # Храним ключи (сущности), чтобы потом сопоставить результаты

    async with aiohttp.ClientSession() as session:
        # 1. Формирование пула задач для АББРЕВИАТУР
        abbr_instructions = config["llm"]["resolve_abbr"]["instructions"]
        for entity, variants in conflict_abbrs.items():
            variants_str = json.dumps(variants, ensure_ascii=False)
            prompt = abbr_instructions.replace("{ENTITY}", entity).replace("{VARIANTS}", variants_str)
            tasks['abbr'].append(model.generate_async(session, prompt, stage="resolve_abbr"))
            meta['abbr'].append(entity)

        # 2. Формирование пула задач для ТЕРМИНОВ
        term_instructions = config["llm"]["resolve_term"]["instructions"]
        for entity, variants in conflict_terms.items():
            variants_str = json.dumps(variants, ensure_ascii=False)
            prompt = term_instructions.replace("{ENTITY}", entity).replace("{VARIANTS}", variants_str)
            tasks['term'].append(model.generate_async(session, prompt, stage="resolve_term"))
            meta['term'].append(entity)

        # 3. Выполнение задач (параллельно для обеих категорий)
        raw_results = {
            'abbr': await tqdm_asyncio.gather(*tasks['abbr'], desc="Resolve Abbrs") if tasks['abbr'] else [],
            'term': await tqdm_asyncio.gather(*tasks['term'], desc="Resolve Terms") if tasks['term'] else []
        }

    # 4. Парсинг результатов для АББРЕВИАТУР
    for entity, res in zip(meta['abbr'], raw_results['abbr']):
        try:
            clean_res = res.strip().replace('```json', '').replace('```', '')
            data = json.loads(clean_res)
            final_val = data.get("resolved_definition", "").strip()

            # Фолбэк, если LLM вернула пустоту
            if not final_val:
                final_val = conflict_abbrs[entity][0]

            resolved_abbrs[entity] = final_val
        except Exception as e:
            print(f"Ошибка парсинга аббревиатуры {entity}: {e}. Беру первый вариант.")
            resolved_abbrs[entity] = conflict_abbrs[entity][0]

    # 5. Парсинг результатов для ТЕРМИНОВ
    for entity, res in zip(meta['term'], raw_results['term']):
        try:
            clean_res = res.strip().replace('```json', '').replace('```', '')
            data = json.loads(clean_res)
            final_val = data.get("resolved_definition", "").strip()

            # Фолбэк, если LLM вернула пустоту
            if not final_val:
                final_val = conflict_terms[entity][0]

            resolved_terms[entity] = final_val
        except Exception as e:
            print(f"Ошибка парсинга термина {entity}: {e}. Беру первый вариант.")
            resolved_terms[entity] = conflict_terms[entity][0]

    return resolved_abbrs, resolved_terms




