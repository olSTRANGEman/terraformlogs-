import json
from datetime import datetime
from re import search
from typing import List, Dict
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
    return [item.strip() for item in line.split(" ")]

def read_json_logs(path: str) -> List[Dict]:
    """
    Читает файл с логами Terraform и помечает каждый лог
    """
    logs_with_sections = []
    current_section = None

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                log_entry = json.loads(line)
            except json.JSONDecodeError:
                continue  # пропускаем некорректные строки

            message = log_entry.get("@message", "")

            # Определяем начало новой секции
            if "CLI args" in message:
                if "plan" in message:
                    current_section = "plan"
                elif "apply" in message:
                    current_section = "apply"
                else:
                    current_section = None

            logs_with_sections.append({
                "log": log_entry,
                "section": current_section
            })

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

def print_logs(logs):
    """
    Красиво выводит список словарей.
    """
    for i, log in enumerate(logs, start=1):
        print(f"{i}:")
        for key, value in log.items():
            print(f"  {key}: {value}")
        print("-" * 30)

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
        if any(keyword("wordsearch.txt") in str(value) for value in log.values()):
            result.append(log)
    return result


def main(path):
    victim=choose("victim.txt")
    logs = read_json_logs(path)
    timespan=choose_time("timespan.txt")
    if victim[0]=="1":
        logs=sort_logs_by_level(logs)
    if victim[1]=="1":
        logs=sort_logs_by_time(logs, timespan[0], timespan[1])
    if victim[2]=="1":
        logs=get_errors(logs)
    if victim[3]=="1":
        logs=get_callers(logs)
    if victim[3]=="2":
        logs=get_no_callers(logs)
    if keyword("wordsearch.txt")!="":
        logs=search(logs)
    save_list_to_file(logs, "output.txt")


