from flask import Flask, request, render_template, jsonify, send_from_directory
import os
import json
import subprocess
from werkzeug.utils import secure_filename
from datetime import datetime
import threading
import time
from redis import Redis
from rq import Queue

app = Flask(__name__)
base_path = '/workspaces/t3c-dev/src/ollama/talk-to-the-city-reports/scatter/pipeline'
app.config['UPLOAD_FOLDER'] = os.path.join(base_path, 'inputs')
app.config['OUTPUT_FOLDER'] = os.path.join(base_path, 'outputs')
app.config['CONFIG_FOLDER'] = os.path.join(base_path, 'configs')
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
app.config['JOBS'] = {}

redis_conn = Redis(host='redis', port=6379)
q = Queue(connection=redis_conn)

# 必要なディレクトリを作成する関数
def ensure_directories():
    for directory in [
        app.config['UPLOAD_FOLDER'],
        app.config['OUTPUT_FOLDER'],
        app.config['CONFIG_FOLDER']
    ]:
        os.makedirs(directory, exist_ok=True)

# アプリケーション起動時にディレクトリを作成
ensure_directories()

def create_config(filename, output_dir):
    return {
        "name": "Recursive Public, Agenda Setting",
        "question": "「第２章 都市づくりのテーマと⽅針」に関してどんな意見がありますか？",
        "input": filename,
        "model": "local:pakachan/elyza-llama3-8b:latest",
        "embedding": {
            "model": "local:pakachan/elyza-llama3-8b:latest",
            "prompt": ""
        },
        "extraction": {
            "workers": 3,
            "limit": 150
        },
        "clustering": {
            "clusters": 5
        },
        "translation": {
            "model": "local:pakachan/elyza-llama3-8b:latest",
            "flags": ["JP"]
        },
        "intro": "This AI-generated report relies on data from a Polis consultation run by the Recursive Public team."
    }

def run_pipeline(config_path, job_id):
    try:
        app.config['JOBS'][job_id] = {
            'status': 'queued',
            'started_at': datetime.now().isoformat()
        }
        
        job = q.enqueue(
            'worker.process_pipeline',
            config_path,
            job_timeout=3600,
            result_ttl=86400     # 24時間保持
        )
        app.config['JOBS'][job_id]['rq_job_id'] = job.id
        
    except Exception as e:
        print(f"Exception in pipeline: {str(e)}")
        app.config['JOBS'][job_id]['status'] = 'failed'
        app.config['JOBS'][job_id]['error'] = str(e)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'ファイルがありません'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'ファイルが選択されていません'}), 400
        
        if not file.filename.endswith('.csv'):
            return jsonify({'error': '拡張子がCSVではありません'}), 400

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = f"project_{timestamp}"
        filename = secure_filename(file.filename)
        base_filename = os.path.splitext(filename)[0]
        
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        
        config = create_config(base_filename, output_dir)
        config_path = os.path.join(app.config['CONFIG_FOLDER'], f"{output_dir}.json")
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        job_id = f"job_{timestamp}"
        thread = threading.Thread(target=run_pipeline, args=(config_path, job_id))
        thread.start()
        
        return jsonify({
            'success': True,
            'message': '処理を開始しました',
            'job_id': job_id
        })
    except Exception as e:
        return jsonify({'error': f'エラーが発生しました: {str(e)}'}), 500

@app.route('/status/<job_id>')
def job_status(job_id):
    if job_id not in app.config['JOBS']:
        return jsonify({'error': 'ジョブが見つかりません'}), 404
    
    job = app.config['JOBS'][job_id]
    if 'rq_job_id' in job:
        rq_job = q.fetch_job(job['rq_job_id'])
        if rq_job is None:
            job['status'] = 'failed'
            job['error'] = 'ジョブが見つかりません'
        elif rq_job.is_finished:
            job['status'] = 'completed'
        elif rq_job.is_failed:
            job['status'] = 'failed'
            job['error'] = str(rq_job.exc_info)
        else:
            job['status'] = 'running'
            # 現在のステップと進捗情報を取得
            try:
                with open(f"{app.config['OUTPUT_FOLDER']}/{job_id.replace('job_', 'project_')}/status.json") as f:
                    pipeline_status = json.load(f)
                    job['current_step'] = pipeline_status.get('current_job', '')
                    job['progress'] = {
                        'current': pipeline_status.get('current_job_progress', 0),
                        'total': pipeline_status.get('current_jop_tasks', 0)
                    }
            except:
                pass
    
    return jsonify(job)

@app.route('/report/<path:path>')
def serve_static_report(path):
    return send_from_directory(app.config['OUTPUT_FOLDER'], path)

@app.route('/pipeline/outputs/<project>/report/', defaults={'path': 'index.html'})
@app.route('/pipeline/outputs/<project>/report/<path:path>')
def serve_project_report(project, path):
    report_dir = os.path.join(app.config['OUTPUT_FOLDER'], project, 'report')
    return send_from_directory(report_dir, path)

@app.route('/sample')
def download_sample():
    return send_from_directory(
        os.path.dirname(os.path.abspath(__file__)),
        'sample_input.csv',
        as_attachment=True,
        mimetype='text/csv'
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)