FROM python:3.9-slim

WORKDIR /app

# Устанавливаем зависимости системы
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Копируем requirements и устанавливаем Python зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем исходный код
COPY parser_gantt.py .
COPY gantt_visualizer.py .

# Создаем директории для входных/выходных данных
RUN mkdir -p input output

# Команда по умолчанию
CMD ["python", "gantt_visualizer.py"]
