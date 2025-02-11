from flask import Flask, request, render_template, jsonify, send_from_directory, url_for
import os
import json
import subprocess
from werkzeug.utils import secure_filename
from datetime import datetime
import threading
import time
from redis import Redis
from rq import Queue
import pandas as pd

app = Flask(__name__, static_folder='static')
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

def create_config(filename, output_dir, custom_config=None):
    default_config = {
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
        "labelling": {
            "prompt": "/system\n\nあなたは、より広い協議の中で一連の意見に対してカテゴリーラベルを生成するカテゴリー分類アシスタントです。協議の主要な質問、クラスター内の意見のリスト、このクラスター外の意見のリストが与えられます。クラスターを要約する単一のカテゴリーラベルで回答します。\n\n質問から既に明らかな文脈は含めません（例：協議の質問が「フランスでどのような課題に直面していますか」のような場合、クラスターラベルで「フランスで」を繰り返す必要はありません）。\n\nラベルは非常に簡潔で、クラスターを外部の意見と区別する特徴を捉えるのに十分な程度の正確さである必要があります。\n\n/human\n\n協議の質問：「イギリスのEU離脱決定の影響は何だと思いますか？」\n\n対象クラスター外の意見例：\n\n* Erasmusプログラムからの除外による教育・文化交流機会の制限に直面\n* 国境検査の強化による移動時間の増加（通勤者や休暇旅行者に影響）\n* 環境基準における協力の減少、気候変動対策の妨げ\n* 相互医療協定の混乱による患者ケアの課題\n* Brexit関連の変更による家族の居住権・市民権申請の複雑化\n* 研究協力機会の減少による国際的な研究課題への取り組みの妨げ\n* EU文化基金プログラムからの除外によるクリエイティブプロジェクトの制限\n* EU資金損失による慈善活動やコミュニティ支援の後退\n* 消費者保護の弱体化による越境紛争解決の課題\n* プロの音楽家としてのEU諸国ツアーの制限によるキャリアへの影響\n\n対象クラスター内の意見例：\n\n* Brexitによるサプライチェーンの混乱、企業のコスト増加と配送遅延\n* Brexitによる投資・退職金の市場変動と不確実性\n* 新たな関税と通関手続きによる輸出業者の利益率低下\n* EU市場内に留まるための企業移転による雇用喪失\n* 輸入品価格高騰による生活費の上昇\n* 英国テクノロジー部門への投資減少、イノベーションと雇用機会への影響\n* 新ビザ規制による観光業の低迷、ホスピタリティ産業への影響\n* ポンド価値下落による購買力低下と旅行費用の増加\n\n/ai\n\n経済的悪影響"
        },
        "translation": {
            "model": "local:pakachan/elyza-llama3-8b:latest",
            "flags": ["JP"]
        },
        "takeaways": {
            "prompt": "/system\n\nあなたはシンクタンクで働く専門的なリサーチアシスタントです。市民協議の中である参加者グループから出された意見のリストが与えられます。それらから主な知見を1〜2段落で要約し、非常に簡潔で読みやすい文章で回答してください。\n\n/human\n\n[\n  \"銃規制を強化すべきだと強く信じています。\",\n  \"包括的な銃規制措置を通じて、この問題に緊急に対処する必要があります。\",\n  \"すべての銃購入者に対する包括的な身元調査の実施を支持します。\",\n  \"アサルト武器と大容量弾倉の禁止に賛成です。\",\n  \"不法な銃器取引を防ぐためのより厳格な規制を提唱します。\",\n  \"銃購入プロセスの一部として、メンタルヘルス評価を義務付けるべきです。\"\n]\n\n/ai\n\n参加者は包括的な銃規制を求めており、特に全購入者への身元調査、アサルト武器の禁止、不法取引の防止、メンタルヘルス評価の義務付けを強調しています。"
        },
        "intro": "これはサンプルです。",
        "overview": {
            "prompt": "/system\n\nあなたはシンクタンクで働く専門的なリサーチアシスタントです。あなたのチームは特定のテーマについて市民協議を実施し、様々な意見のクラスター（グループ）分析を始めています。これから、各クラスターのリストと簡単な分析結果が与えられます。あなたの仕事は、その調査結果を短く要約することです。要約は非常に簡潔（最大1段落、4文以内）で、陳腐な表現を避けて書いてください。"
        }
    }

    if custom_config:
        # 必須フィールドは必ず上書き
        custom_config["input"] = filename
        return custom_config
    
    return default_config

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

def get_spreadsheet_data(spreadsheet_url):
    """スプレッドシートURLからデータを取得"""
    if 'edit#gid=' in spreadsheet_url:
        export_url = spreadsheet_url.replace('/edit#gid=', '/export?format=csv&gid=')
    elif 'edit?usp=sharing' in spreadsheet_url:
        base_url = spreadsheet_url.split('edit?')[0]
        export_url = f"{base_url}export?format=csv"
    else:
        raise ValueError("不正なスプレッドシートURLです")

    df = pd.read_csv(export_url)

    # 必要なカラムの追加・変換
    if '意見' in df.columns:
        df = df.rename(columns={'意見': 'comment-body'})

    # comment-idカラムの追加
    df.insert(0, 'comment-id', range(1, len(df) + 1))

    return df

def process_input_data(data_source):
    """CSVデータの処理（共通処理）"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = f"project_{timestamp}"

    # 一時ファイルの保存
    input_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{output_dir}.csv")
    data_source.to_csv(input_path, index=False)

    return input_path, output_dir, timestamp

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = f"project_{timestamp}"
        custom_config = None
        
        # 設定ファイルの処理
        if 'config' in request.files:
            config_file = request.files['config']
            if config_file.filename != '' and config_file.filename.endswith('.json'):
                try:
                    custom_config = json.loads(config_file.read().decode('utf-8'))
                except json.JSONDecodeError:
                    return jsonify({'error': '設定ファイルのJSONが不正です'}), 400

        # スプレッドシートURLからの処理
        if request.form.get('spreadsheet_url'):
            try:
                df = get_spreadsheet_data(request.form['spreadsheet_url'])
                filename = f"spreadsheet_{timestamp}.csv"
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                df.to_csv(filepath, index=False)
                base_filename = f"spreadsheet_{timestamp}"
            except Exception as e:
                return jsonify({'error': f'スプレッドシートの読み込みに失敗: {str(e)}'}), 400

        # 既存のファイルアップロード処理
        else:
            if 'file' not in request.files:
                return jsonify({'error': 'ファイルがありません'}), 400

            file = request.files['file']
            if file.filename == '':
                return jsonify({'error': 'ファイルが選択されていません'}), 400

            if not file.filename.endswith('.csv'):
                return jsonify({'error': '拡張子がCSVではありません'}), 400

            filename = secure_filename(file.filename)
            base_filename = os.path.splitext(filename)[0]
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))

        # 共通の後続処理
        config = create_config(base_filename, output_dir, custom_config)
        config_path = os.path.join(app.config['CONFIG_FOLDER'], f"{output_dir}.json")
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        
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

@app.route('/sample-config')
def download_sample_config():
    """サンプル設定ファイルをダウンロードするエンドポイント"""
    sample_config = create_config("sample_input", "sample_project")
    
    response = jsonify(sample_config)
    response.data = json.dumps(
        sample_config,
        ensure_ascii=False,  # 日本語を \u エスケープしない
        indent=2           # インデントを2スペースに
    ).encode('utf-8')      # UTF-8でエンコード
    
    response.headers['Content-Type'] = 'application/json; charset=utf-8'  # 文字コードを指定
    response.headers['Content-Disposition'] = 'attachment; filename=sample_config.json'
    return response

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)