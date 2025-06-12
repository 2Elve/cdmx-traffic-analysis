import json
from shapely.geometry import LineString, Point
import pandas as pd

# Configura tu ruta específica (ejemplo: de Polanco a Santa Fe)
MY_ROUTE = [
    [-99.1947, 19.4336],  # Punto inicial (lat, lon)
    [-99.2034, 19.4285],   # Puntos intermedios...
    [-99.2256, 19.3698]    # Punto final
]

def process_waze_data(waze_json):
    """Procesa los datos de Waze para extraer información relevante"""
    
    # Convertir ruta a LineString
    route_line = LineString(MY_ROUTE)
    
    results = {
        'timestamp': [],
        'incident_type': [],
        'street': [],
        'speed': [],
        'distance_to_route': [],
        'delay_minutes': []
    }
    
    # Procesar alertas (accidentes, policía, etc.)
    for alert in waze_json.get('alerts', []):
        alert_point = Point(alert['location']['x'], alert['location']['y'])
        distance = route_line.distance(alert_point) * 111000  # Convertir a metros
        
        if distance < 500:  # Solo incidentes dentro de 500m de tu ruta
            results['timestamp'].append(alert['pubMillis'])
            results['incident_type'].append(alert.get('type', 'UNKNOWN'))
            results['street'].append(alert.get('street', 'Desconocida'))
            results['speed'].append(None)
            results['distance_to_route'].append(distance)
            results['delay_minutes'].append(estimate_delay(alert['type']))
    
    # Procesar congestiones (jams)
    for jam in waze_json.get('jams', []):
        jam_line = LineString([(p['x'], p['y']) for p in jam['segments']])
        intersection = route_line.intersection(jam_line)
        
        if not intersection.is_empty:
            jam_length = jam_line.length * 111000  # Longitud en metros
            jam_speed = jam.get('speedKMH', 5)
            free_flow_speed = 50  # Velocidad esperada sin tráfico (ajustar)
            
            delay = (jam_length / 1000) * (1/jam_speed - 1/free_flow_speed) * 60
            
            results['timestamp'].append(jam.get('pubMillis', 0))
            results['incident_type'].append('TRAFFIC_JAM')
            results['street'].append(jam.get('street', 'Desconocida'))
            results['speed'].append(jam_speed)
            results['distance_to_route'].append(0)
            results['delay_minutes'].append(delay)
    
    return results

def estimate_delay(incident_type):
    """Estima el retraso basado en tipo de incidente"""
    delay_map = {
        'ACCIDENT': 10,
        'HAZARD': 5,
        'JAM': 15,
        'POLICE': 2,
        'ROAD_CLOSED': 20
    }
    return delay_map.get(incident_type, 5)