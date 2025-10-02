import json
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime

def create_gantt_chart(gantt_data: list):
    """Создает диаграмму Ганта из данных"""
    if not gantt_data:
        print("Нет данных для построения диаграммы")
        return
    
    # Создаем DataFrame для Plotly
    df = pd.DataFrame(gantt_data)
    df['Start'] = pd.to_datetime(df['Start'])
    df['Finish'] = pd.to_datetime(df['Finish'])
    
    # Создаем диаграмму Ганта
    fig = px.timeline(
        df, 
        x_start="Start", 
        x_end="Finish", 
        y="Task",
        color="Resource",
        title="Диаграмма Ганта - Хронология запросов",
        labels={"Task": "Запросы", "Start": "Время начала", "Finish": "Время окончания"},
        hover_data=["tf_req_id", "Duration", "events_count", "has_errors"]
    )
    
    # Настраиваем внешний вид
    fig.update_layout(
        xaxis_title="Время",
        yaxis_title="Запросы (tf_req_id)",
        showlegend=True,
        height=max(600, len(gantt_data) * 30),
        font=dict(size=12)
    )
    
    # Сохраняем как HTML
    fig.write_html("output/gantt_chart.html")
    print("Диаграмма Ганта сохранена в output/gantt_chart.html")
    
    # Дополнительная визуализация - временная шкала
    create_timeline_chart(gantt_data)

def create_timeline_chart(gantt_data: list):
    """Создает дополнительную временную шкалу"""
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Временная шкала запросов', 'Длительность запросов'),
        vertical_spacing=0.1,
        row_heights=[0.7, 0.3]
    )
    
    # Добавляем временные интервалы
    for i, req in enumerate(gantt_data):
        start = datetime.fromisoformat(req['Start'])
        finish = datetime.fromisoformat(req['Finish'])
        
        fig.add_trace(
            go.Scatter(
                x=[start, finish],
                y=[req['Task'], req['Task']],
                mode='lines+markers',
                name=req['tf_req_id'],
                line=dict(width=10),
                marker=dict(size=8),
                hovertemplate=f"<b>{req['Task']}</b><br>"
                            f"Начало: {start}<br>"
                            f"Окончание: {finish}<br>"
                            f"Длительность: {req['Duration']:.2f}с<br>"
                            f"Событий: {req['events_count']}<extra></extra>"
            ),
            row=1, col=1
        )
    
    # Добавляем график длительностей
    durations = [req['Duration'] for req in gantt_data]
    tasks = [req['Task'] for req in gantt_data]
    
    fig.add_trace(
        go.Bar(
            x=tasks,
            y=durations,
            name="Длительность (сек)",
            marker_color='lightblue'
        ),
        row=2, col=1
    )
    
    fig.update_layout(
        height=800,
        title_text="Анализ временных характеристик запросов",
        showlegend=False
    )
    
    fig.update_xaxes(title_text="Время", row=1, col=1)
    fig.update_xaxes(title_text="Запросы", row=2, col=1)
    fig.update_yaxes(title_text="Длительность (сек)", row=2, col=1)
    
    fig.write_html("output/timeline_analysis.html")
    print("Дополнительная временная шкала сохранена в output/timeline_analysis.html")

def main():
    try:
        with open("output/gantt_data.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        
        gantt_data = data['gantt_data']
        summary = data['summary']
        
        print(f"Всего запросов: {summary['total_requests']}")
        print(f"Запросов с ошибками: {summary['requests_with_errors']}")
        print(f"Временной диапазон: {summary['time_range']['start']} - {summary['time_range']['end']}")
        
        # Создаем визуализации
        create_gantt_chart(gantt_data)
        
    except FileNotFoundError:
        print("Файл gantt_data.json не найден. Сначала запустите парсер.")
    except Exception as e:
        print(f"Ошибка при создании визуализации: {e}")

if __name__ == "__main__":
    main()
