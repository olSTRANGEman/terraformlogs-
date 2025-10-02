from http.cookiejar import debug

from flask import Flask, request
import os
from func import *

app = Flask(__name__)
UPLOAD_FOLDER = "input"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def logs_to_html_table(logs):
    """Преобразует список логов в HTML-таблицу"""

    if not logs:
        return "<p>Нет данных для отображения</p>"

    # Собираем все уникальные ключи для заголовков таблицы
    all_keys = set()
    for log in logs:
        all_keys.update(log.keys())

    headers = sorted(list(all_keys))

    # Генерируем HTML
    html = ['<table border="1" style="border-collapse: collapse; width: 100%;">']

    # Заголовки таблицы
    html.append('<tr style="background-color: #f2f2f2;">')
    for header in headers:
        html.append(f'<th style="padding: 8px; text-align: left;">{header}</th>')
    html.append('</tr>')

    # Данные таблицы
    for i, log in enumerate(logs):
        row_color = '#f9f9f9' if i % 2 == 0 else '#ffffff'
        html.append(f'<tr style="background-color: {row_color};">')

        for header in headers:
            value = log.get(header, '')
            # Форматируем специальные значения
            if header == '@timestamp' and value:
                try:
                    # Пытаемся красиво отформатировать timestamp
                    dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    value = dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass

            cell_content = str(value) if value is not None else ''
            html.append(f'<td style="padding: 8px; border: 1px solid #ddd;">{cell_content}</td>')

        html.append('</tr>')

    html.append('</table>')
    return '\n'.join(html)

@app.route("/", methods=["GET"])
def index():
    return """<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Загрузка файла с критериями и заметкой</title>
</head>
<body>
    <h2>Загрузить файл и выбрать критерии</h2>
    <form action="/upload" method="post" enctype="multipart/form-data">
        <label>Выберите файл логов (json-формат):</label><br>
        <input type="file" name="file" required><br><br>

        <label>Критерии парсинга:</label><br>
        <input type="checkbox" name="criteria" value="criterion1"> Сортировка по уровню<br>
        <input type="checkbox" name="criteria" value="criterion2"> Только ошибки<br>
        <input type="checkbox" name="criteria" value="criterion3"> Только вызовы провайдеров<br>
        <input type="checkbox" name="criteria" value="criterion4"> Без вызовов провайдеров<br><br>

        <label>Поиск по полю времени</label><br>
        <div style="display:flex; gap:10px;">
            <input type="text" name="time1" placeholder="Начало времени" style="flex:1;">
            <input type="text" name="time2" placeholder="Конец времени" style="flex:1;">
        </div><br><br>

        <label>Поиск по ключевым словам:</label><br>
        <input type="text" name="comment" placeholder="Поиск..."><br><br>

        <button type="submit">Загрузить</button>
    </form>
</body>
</html>
    """

@app.route('/upload', methods=['POST'])
def main_web():
    if "file" not in request.files:
        return "Файл не прикреплён", 400

    file = request.files["file"]
    file.save(os.path.join(UPLOAD_FOLDER, "logs.json"))

    # чтение галочек
    criteria = request.form.getlist('criteria')
    output = []
    for i in range(1, 5):
        output.append("1" if f"criterion{i}" in criteria else "0")
    result_str = " ".join(output)

    # выписываение галочек
    result_path = os.path.join(UPLOAD_FOLDER, "victim.txt")
    with open(result_path, "w") as f:
        f.write(result_str)

    # чтение поиска кейвордов
    word = request.form.get("comment", "")
    word_path = os.path.join(UPLOAD_FOLDER, "wordsearch.txt")
    with open(word_path, "w", encoding="utf-8") as f:
        f.write(word)

    # поиск по полю времени или как эта херня называется (пустые поля заменяются началом и концом времен (11 сеннтября и 9999 год))
    time1 = request.form.get("time1", "").strip() or "0"
    time2 = request.form.get("time2", "").strip() or "0"
    time_path = os.path.join(UPLOAD_FOLDER, "timespan.txt")
    with open(time_path, "w", encoding="utf-8") as f:
        f.write(f"{time1} {time2}")

    # парсер
    main_func("input/logs.json")

    # выкидывание парсера
    parsed_path = "output/parsed_logs.json"
    if os.path.exists(parsed_path):
        with open(parsed_path, "r", encoding="utf-8") as f:
            parsed_content = f.read()
        return f"""
        <html>
            <head><title>Результаты парсинга</title></head>
            <body>
                <div style="margin-bottom: 15px;">
                    <button onclick="window.location.href='/'">Назад</button>
                    <button onclick="window.location.href='http://127.0.0.1:1000/supertabloid'">Таблица</button>
                </div>
                <h2>Результаты:</h2>
                <pre>{parsed_content}</pre>
            </body>
        </html>
        """
    else:
        return "Файл parsed_logs.json не найден", 500




@app.route('/supertabloid', methods=['GET', 'POST'])
def process_logs():
    try:
        logs = read_json_logs("output/parsed_logs.json")  # читаем JSON
        html_table = logs_to_html_table(logs)  # преобразуем в таблицу

        html_page = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>Логи Terraform</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                table {{ margin-top: 20px; }}
                h1 {{ color: #333; }}
            </style>
        </head>
        <body>
            <h1>Анализ логов Terraform</h1>
            <p>Всего записей: {len(logs)}</p>
            {html_table}
        </body>
        </html>
        """

        return html_page

    except Exception as e:
        return jsonify({'error': f'Ошибка обработки: {str(e)}'}), 500




if __name__ == '__main__':
    app.run(host='127.0.0.1', port=1000, debug=true)


