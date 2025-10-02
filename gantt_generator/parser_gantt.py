import json
from datetime import datetime
from typing import List, Dict, Any
import re

class GanttParser:
    def __init__(self):
        self.requests = {}
        
    def extract_request_data(self, logs: List[Dict]) -> List[Dict]:
        """
        Извлекает данные о запросах для построения диаграммы Ганта
        """
        request_data = []
        
        for log in logs:
            message = log.get('@message', '')
            timestamp = log.get('@timestamp', '')
            level = log.get('@level', '')
            
            # Ищем tf_req_id в сообщении
            tf_req_id = self.extract_tf_req_id(message)
            if not tf_req_id:
                continue
                
            # Определяем тип события (начало/конец/промежуточный)
            event_type = self.classify_event(message, level)
            
            request_data.append({
                'tf_req_id': tf_req_id,
                'timestamp': timestamp,
                'event_type': event_type,
                'message': message,
                'level': level,
                'full_log': log
            })
            
        return request_data
    
    def extract_tf_req_id(self, message: str) -> str:
        """
        Извлекает tf_req_id из сообщения
        """
        patterns = [
            r'tf_req_id[=:\s]+([\w-]+)',
            r'req_id[=:\s]+([\w-]+)',
            r'request[=:\s]+([\w-]+)',
            r'\[([\w-]+)\]'  # ID в квадратных скобках
        ]
        
        for pattern in patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                return match.group(1)
        return ""
    
    def classify_event(self, message: str, level: str) -> str:
        """
        Классифицирует событие для диаграммы Ганта
        """
        message_lower = message.lower()
        
        if any(word in message_lower for word in ['start', 'begin', 'init', 'processing']):
            return 'start'
        elif any(word in message_lower for word in ['end', 'finish', 'complete', 'done', 'success']):
            return 'end'
        elif any(word in message_lower for word in ['error', 'fail', 'exception']):
            return 'error'
        elif any(word in message_lower for word in ['request', 'call', 'invoke']):
            return 'request'
        elif any(word in message_lower for word in ['response', 'result']):
            return 'response'
        else:
            return 'info'
    
    def build_gantt_data(self, request_data: List[Dict]) -> List[Dict]:
        """
        Строит данные для диаграммы Ганта, группируя по tf_req_id
        """
        requests = {}
        
        # Группируем события по tf_req_id
        for event in request_data:
            req_id = event['tf_req_id']
            if req_id not in requests:
                requests[req_id] = {
                    'tf_req_id': req_id,
                    'events': [],
                    'start_time': None,
                    'end_time': None,
                    'duration': None
                }
            
            requests[req_id]['events'].append(event)
            
            # Обновляем время начала и окончания
            event_time = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
            if not requests[req_id]['start_time'] or event_time < requests[req_id]['start_time']:
                requests[req_id]['start_time'] = event_time
            if not requests[req_id]['end_time'] or event_time > requests[req_id]['end_time']:
                requests[req_id]['end_time'] = event_time
        
        # Рассчитываем длительность и форматируем данные
        gantt_data = []
        for req_id, data in requests.items():
            if data['start_time'] and data['end_time']:
                duration = (data['end_time'] - data['start_time']).total_seconds()
                
                gantt_data.append({
                    'Task': f"Request {req_id}",
                    'Start': data['start_time'].isoformat(),
                    'Finish': data['end_time'].isoformat(),
                    'Duration': duration,
                    'Resource': self.determine_resource(data['events']),
                    'tf_req_id': req_id,
                    'events_count': len(data['events']),
                    'has_errors': any(e['event_type'] == 'error' for e in data['events'])
                })
        
        return sorted(gantt_data, key=lambda x: x['Start'])
    
    def determine_resource(self, events: List[Dict]) -> str:
        """
        Определяет тип ресурса на основе событий
        """
        event_types = [e['event_type'] for e in events]
        
        if 'error' in event_types:
            return 'Error'
        elif 'request' in event_types and 'response' in event_types:
            return 'API Call'
        elif any(e['level'] == 'warn' for e in events):
            return 'Warning'
        else:
            return 'Info'

def read_json_logs(path: str) -> List[Dict]:
    """Читает JSON логи из файла"""
    logs = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                logs.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return logs

def main():
    # Читаем логи
    logs = read_json_logs("input/logs.json")
    
    # Парсим данные для диаграммы Ганта
    parser = GanttParser()
    request_data = parser.extract_request_data(logs)
    gantt_data = parser.build_gantt_data(request_data)
    
    # Сохраняем данные для визуализации
    with open("output/gantt_data.json", "w", encoding="utf-8") as f:
        json.dump({
            'gantt_data': gantt_data,
            'summary': {
                'total_requests': len(gantt_data),
                'requests_with_errors': len([r for r in gantt_data if r['has_errors']]),
                'time_range': {
                    'start': gantt_data[0]['Start'] if gantt_data else None,
                    'end': gantt_data[-1]['Finish'] if gantt_data else None
                }
            }
        }, f, indent=2, ensure_ascii=False)
    
    print(f"Обработано {len(gantt_data)} запросов для диаграммы Ганта")

if __name__ == "__main__":
    main()
