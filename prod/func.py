import json
from datetime import datetime
from re import search
from typing import List, Dict, Any, Union



LEVEL_ORDER = {
    "trace": 1,
    "info": 2,
    "debug": 3,
    "warn": 4,
    "error": 5,
    "off": 6
}

def keyword(path):

    with open(path, "r", encoding="utf-8") as f:
        return f.readline().strip()

def choose(path: str):
    """
    Читает первую строку из .txt файла формата "a, b, c, d"
    и возвращает список элементов ['a', 'b', 'c', 'd'].
    """
    with open(path, "r", encoding="utf-8") as f:
        line = f.readline().strip()
    return [item.strip() for item in line.split(" ")]

def choose_time(path: str):
    with open(path, "r", encoding="utf-8") as f:
        line = f.readline().strip()
    time1, time2 = line.split(" ")
    if time2 == "0":
        time2="9998-11-27T23:59:58.99998+0300"
    if time1 == "0":
        time1="2001-09-11T15:31:32.842105+03:00"
    return [time1, time2]

def read_json_logs(path: str) -> List[Dict]:
    """
    Читает файл построчно и возвращает список словарей.
    Никакой обработки, только чтение.
    """
    logs = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                log_entry = json.loads(line)
                logs.append(log_entry)
            except json.JSONDecodeError:
                continue  # пропускаем некорректные строки
    return logs

def process_logs(logs: List[Dict]) -> List[Dict]:
    """
    Обрабатывает список логов:
    - добавляет @id (номер строки/лога);
    - определяет section (plan / apply / None).
    """
    logs_with_sections = []
    current_section = None

    for idx, log_entry in enumerate(logs, start=1):
        message = log_entry.get("@message", "")

        # Определяем начало новой секции
        if "CLI args" in message:
            if "plan" in message:
                current_section = "plan"
            elif "apply" in message:
                current_section = "apply"
            else:
                current_section = None

        # Добавляем секцию внутрь самого лога
        log_entry["section"] = current_section

        # Добавляем @id
        log_entry["@id"] = str(idx)

        logs_with_sections.append(log_entry)

    return logs_with_sections

def save_list_to_file(data, filename):

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def save_list_to_file_one_line(data, filename):
    """
    Сохраняет каждый элемент списка логов в отдельную строку в файле.
    """
    with open(filename, "w", encoding="utf-8") as f:
        for log in data:
            json_line = json.dumps(log, ensure_ascii=False)
            f.write(json_line + "\n")

def sort_logs_by_level(logs):
    """
    Сортирует список логов по заданной градации LEVEL_ORDER.
    """
    return sorted(logs, key=lambda log: LEVEL_ORDER.get(log.get("@level", "off"), 999))

def sort_logs_by_time(logs, start_time: str, end_time: str):
    """
    Фильтрует логи по временному промежутку.
    Формат времени: 'YYYY-MM-DDTHH:MM:SS.ssssss+HH:MM'
    Например: '2025-09-09T11:05:51.089168+03:00'
    """
    # преобразуем строки в datetime
    start_dt = datetime.fromisoformat(start_time)
    end_dt = datetime.fromisoformat(end_time)

    result = []
    for log in logs:
        ts = log.get("@timestamp")
        if not ts:
            continue
        try:
            log_dt = datetime.fromisoformat(ts)
        except ValueError:
            continue

        if start_dt <= log_dt <= end_dt:
            result.append(log)

    return result

def get_errors(logs):
    """
    Пробегает по списку логов и возвращает список всех ошибок.
    """
    errors = []
    for log in logs:
        if log.get("@level") == "error":
            errors.append(log)
    return errors

def get_callers(logs):
    """
    Возвращает список логов, в которых есть ключ "@caller".
    """
    callers = []
    for log in logs:
        if "@caller" in log:
            callers.append(log)
    return callers

def get_no_callers(logs):
    """
    Возвращает список логов, в которых есть ключ "@caller".
    """
    nocallers = []
    for log in logs:
        if "@caller" not in log:
            nocallers.append(log)
    return nocallers

def search(logs):
    """
    Ищет заданное слово в логах и возвращает список словарей с найденными записями.

    logs_list — список словарей (логов)
    keyword — строка для поиска
    """
    result = []
    for log in logs:
        # Проверяем все значения словаря на наличие ключевого слова
        if any(keyword("input/wordsearch.txt") in str(value) for value in log.values()):
            result.append(log)
    return result

def save_list_to_json(logs, filename="logs.json"):
    """
    Принимает список словарей (логи) и сохраняет их:
    1. В обычный JSON-массив (filename)
    2. В формате JSON Lines (каждый лог на отдельной строке) (lines_filename)
    """
    # Сохраняем построчно (каждый лог отдельная строка)
    with open(filename, "w", encoding="utf-8") as f:
        for log in logs:
            f.write(json.dumps(log, ensure_ascii=False) + "\n")

def PARCE(logs: List[Union[Dict[str, Any], str]], output_path: [str]) -> List[Dict[str, Any]]:
    """
    Обрабатывает уже прочитанный список логов (список dict или JSON-строк).
    То же, что раньше, плюс в конце записывает результат в файл (JSON Lines).

    Параметры:
      logs        - список словарей или JSON-строк
      output_path - путь для записи результата (если None — не записывает). По умолчанию
                    "output/parsed_logs_processed.jsonl".

    Возвращает:
      список обработанных логов (List[Dict[str, Any]]) в нужном порядке полей.
    """
    logs_with_sections: List[Dict[str, Any]] = []
    current_section = None

    for idx, item in enumerate(logs, start=1):
        # Подготовим словарь: если на входе JSON-строка — распарсим, если dict — скопируем
        if isinstance(item, str):
            try:
                log_entry = json.loads(item)
                if not isinstance(log_entry, dict):
                    continue
            except json.JSONDecodeError:
                continue
        elif isinstance(item, dict):
            log_entry = dict(item)  # shallow copy
        else:
            continue

        # определяем секцию по сообщению CLI args
        message = log_entry.get("@message", "")
        if "CLI args" in message:
            if "plan" in message:
                current_section = "plan"
            elif "apply" in message:
                current_section = "apply"
            else:
                current_section = None

        # проставляем/обновляем поля
        log_entry["section"] = current_section

        if "@caller" not in log_entry:
            log_entry["@caller"] = None
        if "@module" not in log_entry:
            log_entry["@module"] = None

        # собираем все ключи, не начинающиеся с '@', в about (кроме line_number и section)
        about: Dict[str, Any] = {}
        for key in list(log_entry.keys()):
            if key == "about":
                continue
            if not key.startswith("@") and key not in ["line_number", "section"]:
                about[key] = log_entry.pop(key)

        log_entry["about"] = about

        # Формируем итоговый словарь в нужном порядке
        ordered_log: Dict[str, Any] = {
            "@id": log_entry["@id"],
            "@level": log_entry.get("@level"),
            "@message": log_entry.get("@message"),
            "@caller": log_entry.get("@caller"),
            "@module": log_entry.get("@module"),
            "about": log_entry.get("about"),
            "@timestamp": log_entry.get("@timestamp"),
            "section": log_entry.get("section"),
        }

        logs_with_sections.append(ordered_log)


        with open(output_path, "w", encoding="utf-8") as outf:
            for entry in logs_with_sections:
                outf.write(json.dumps(entry, ensure_ascii=False) + "\n")

    return logs_with_sections

def process_and_save(path: str):
    """
    Читает файл через read_json_logs и перезаписывает его
    обновлёнными логами (в том же файле).
    """
    logs = read_json_logs(path)

    with open(path, "w", encoding="utf-8") as f:
        for log in logs:
            f.write(json.dumps(log, ensure_ascii=False) + "\n")

def main_func(path):
    victim=choose("input/victim.txt")
    logs = process_logs(read_json_logs(path))
    process_and_save(path)
    if victim[0]=="1":
        logs=sort_logs_by_level(logs)
    if victim[1]=="1":
        logs=get_errors(logs)
    if victim[2]=="1":
        logs=get_callers(logs)
    if victim[3]=="2":
        logs=get_no_callers(logs)
    if keyword("input/wordsearch.txt") !="":
        logs=search(logs)
    PARCE(sort_logs_by_time(logs, choose_time("input/timespan.txt")[0], choose_time("input/timespan.txt")[1]), "output/parsed_logs.json")

print(process_logs(read_json_logs("input/logs.json")))
