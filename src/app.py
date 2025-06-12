import os
from flask import Flask, request, jsonify
from datetime import datetime
import json
import pandas as pd
from dotenv import load_dotenv
from utils.data_processor import process_waze_data

# Cargar variables de entorno
load_dotenv()

app = Flask(__name__)

# Configuración
WEBHOOK_URL = os.getenv('WEBHOOK_URL', '/waze/webhook')
SECRET_TOKEN = os.getenv('SECRET_TOKEN')  # Para validar requests
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

@app.route(WEBHOOK_URL, methods=['POST'])
def waze_webhook():
    """Endpoint para recibir datos de Waze"""
    
    # 1. Validar el request
    if not validate_request(request):
        return jsonify({'error': 'Unauthorized'}), 401
    
    # 2. Obtener datos JSON
    try:
        data = request.json
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 3. Guardar datos crudos
        raw_path = os.path.join(DATA_DIR, f'raw_{timestamp}.json')
        with open(raw_path, 'w') as f:
            json.dump(data, f)
        
        # 4. Procesar datos (ejemplo: extraer info de tu ruta)
        processed_data = process_waze_data(data)
        
        # 5. Guardar datos procesados
        processed_path = os.path.join(DATA_DIR, 'processed', f'processed_{timestamp}.csv')
        pd.DataFrame(processed_data).to_csv(processed_path, index=False)
        
        return jsonify({'status': 'success'}), 200
    
    except Exception as e:
        app.logger.error(f"Error processing Waze data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/traffic', methods=['GET'])
def get_traffic_data():
    """Endpoint para consultar datos procesados"""
    date_from = request.args.get('from')
    date_to = request.args.get('to')
    
    # Cargar todos los datos procesados en el rango de fechas
    data_files = [f for f in os.listdir('data/processed') if f.endswith('.csv')]
    
    if date_from and date_to:
        data_files = [f for f in data_files if date_from <= f.split('_')[1] <= date_to]
    
    dfs = []
    for file in data_files:
        dfs.append(pd.read_csv(os.path.join('data/processed', file)))
    
    if not dfs:
        return jsonify({'error': 'No data found'}), 404
    
    combined_df = pd.concat(dfs)
    return combined_df.to_json(orient='records')

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Estadísticas agregadas"""
    df = pd.read_json(get_traffic_data().get_data(as_text=True))
    
    stats = {
        'worst_time': df.groupby('hour')['delay_minutes'].mean().idxmax(),
        'best_time': df.groupby('hour')['delay_minutes'].mean().idxmin(),
        'common_incidents': df['incident_type'].value_counts().to_dict()
    }
    
    return jsonify(stats)


def validate_request(req):
    """Valida que el request sea auténtico"""
    # Waze puede enviar un token de verificación
    return req.headers.get('X-Secret-Token') == SECRET_TOKEN

if __name__ == '__main__':
    # Crear directorios si no existen
    os.makedirs(os.path.join(DATA_DIR, 'processed'), exist_ok=True)
    app.run(host='0.0.0.0', port=5000, debug=True)