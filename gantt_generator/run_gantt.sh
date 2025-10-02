#!/bin/bash

echo "=== Генератор диаграмм Ганта ==="

# Создаем директории если их нет
mkdir -p input output

# Проверяем наличие логов
if [ ! -f "input/logs.json" ]; then
    echo "ВНИМАНИЕ: Файл input/logs.json не найден"
    echo "Пожалуйста, поместите ваши логи в input/logs.json"
    exit 1
fi

echo "Запуск Docker контейнеров..."
docker-compose up --build

echo "=== Готово ==="
echo "Диаграммы сохранены в директории output:"
echo "  - gantt_chart.html: Основная диаграмма Ганта"
echo "  - timeline_analysis.html: Детальный анализ времени"
echo "  - gantt_data.json: Данные для диаграммы в JSON формате"
