from flask import Flask, request
import os
from func import *

app = Flask(__name__)
UPLOAD_FOLDER = "input"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


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
                <button onclick="window.location.href='/'">⬅ Назад</button>
                <h2>Результаты:</h2>
                <pre>{parsed_content}</pre>
            </body>
        </html>
        """
    else:
        return "Файл parsed_logs.json не найден", 500

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=1000)


