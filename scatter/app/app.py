from flask import Flask, request, render_template, jsonify, send_from_directory, url_for, current_app
from flask.cli import with_appcontext, AppGroup
import os
import json
import subprocess
from werkzeug.utils import secure_filename
from datetime import datetime, timezone, timedelta
import threading
import time
from redis import Redis
from rq import Queue
import hashlib
import pandas as pd
import traceback  # tracebackモジュールを追加
import sys
from pathlib import Path
import importlib.util

app = Flask(__name__, static_folder='static')

base_path = '/workspaces/t3c-dev/src/ollama/talk-to-the-city-reports/scatter/pipeline'
app.config.update({
    'UPLOAD_FOLDER': os.path.join(base_path, 'inputs'),
    'OUTPUT_FOLDER': os.path.join(base_path, 'outputs'),
    'CONFIG_FOLDER': os.path.join(base_path, 'configs'),
    'MAX_CONTENT_LENGTH': 16 * 1024 * 1024,
    'JOBS': {}
})
UPDATE_INTERVALS = {
    'DEFAULT_INTERVAL_SECONDS': 86400,  # 24時間
    'MAX_CHECK_COUNT': 30,       # 最大チェック回数
    'MAX_ERROR_COUNT': 3,        # 最大連続エラー回数
}

redis_conn = Redis(host='redis', port=6379)
high_priority_queue = Queue('high', connection=redis_conn)    # スケジューラー用の優先キュー
default_queue = Queue('default', connection=redis_conn)       # 通常の処理用キュー
q = high_priority_queue

# 必要なディレクトリを作成する関数
def ensure_directories():
    for directory in [
        app.config['UPLOAD_FOLDER'],
        app.config['OUTPUT_FOLDER'],
        app.config['CONFIG_FOLDER']
    ]:
        os.makedirs(directory, exist_ok=True)

def init_app():
    """アプリケーションの初期化"""
    try:
        # 必要なディレクトリを作成
        ensure_directories()
        print("Application directories initialized")
        return True
        
    except Exception as e:
        print(f"Error during initialization: {str(e)}")
        traceback.print_exc()
        return False

def create_config(filename, output_dir, custom_config=None):
    """設定ファイルを作成する関数"""
    print(f"=== CREATE_CONFIG DEBUG ===")
    print(f"Called with filename: {filename}")
    print(f"output_dir: {output_dir}")
    print(f"custom_config: {json.dumps(custom_config, indent=2) if custom_config else 'None'}")
    
    # 1. パイプライン用の基本設定
    pipeline_config = {
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
            "limit": 150,
            "prompt": "/system\n\n与えられた投稿を要約し、文字列のみを要素として持つJSONリストとして返してください。\n辞書型（キーと値のペア）を含めず、文字列のみのリストを返してください。\n追加の説明は含めず、必ずJSONリストのみを返してください。\n予め提供された例や過去の回答を含めないでください。\n投稿内容を要約することができない旨の出力だった場合は、要約せずに与えられた投稿を文字列としてJSONリストの形で出力してください。\n\n正しい例:\n[\"要約された意見1\", \"要約された意見2\"]\n\n誤った例:\n[{\"内容\": \"要約された意見\"}]\n\n注意:\n- 文字列のみを含むJSONリスト形式で出力\n- 辞書型オブジェクトは使用しない\n- システムメッセージや注釈を含めない\n- 過去の例を含めない"
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
    
    print(f"Initial pipeline_config input: {pipeline_config['input']}")
    
    # 2. アプリケーション管理用の設定（別ファイル）
    app_config = {
        "project_id": output_dir,
        "created_at": datetime.now().isoformat(),
        "status": "pending"
    }

    # 3. 自動更新設定（別ファイル）
    auto_update_config = None

    if custom_config:
        # auto_update設定の抽出と保存
        if 'auto_update' in custom_config:
            auto_update_config = custom_config.pop('auto_update')
            save_auto_update_config(output_dir, auto_update_config)
            print(f"Extracted auto_update config")

        # カスタム設定のマージ前のinput値
        print(f"Before merge, pipeline_config input: {pipeline_config['input']}")
        print(f"custom_config contains input? {'input' in custom_config}")
        if 'input' in custom_config:
            print(f"custom_config input value: {custom_config['input']}")
        
        # カスタム設定のマージ（パイプライン設定のみ）
        pipeline_config.update(custom_config)
        print(f"After merge, pipeline_config input: {pipeline_config['input']}")
    
    # 重要: custom_configの有無にかかわらず、input フィールドを強制的に上書き
    print(f"Forcing input to: {filename}")
    pipeline_config["input"] = filename
    print(f"Final pipeline_config input: {pipeline_config['input']}")

    # 4. 各設定ファイルの保存
    save_pipeline_config(output_dir, pipeline_config)
    save_app_config(output_dir, app_config)
    
    print(f"=== END CREATE_CONFIG DEBUG ===")
    return pipeline_config

def save_pipeline_config(output_dir, config):
    """パイプライン設定を保存"""
    config_path = os.path.join(app.config['CONFIG_FOLDER'], f"{output_dir}.json")
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

def save_app_config(output_dir, config):
    """アプリケーション管理用設定を保存"""
    config_path = os.path.join(app.config['CONFIG_FOLDER'], f"{output_dir}_app.json")
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

def save_auto_update_config(output_dir, config):
    """自動更新設定を保存"""
    config_path = os.path.join(app.config['CONFIG_FOLDER'], f"{output_dir}_auto_update.json")
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

def run_pipeline(config_path, job_id):
    try:
        # ジョブの初期状態を設定
        app.config['JOBS'][job_id] = {
            'status': 'queued',
            'started_at': datetime.now().isoformat(),
            'current_step': 'initialization',
            'project_id': config_path.split('/')[-1].replace('.json', ''),
            'progress': {
                'current': 0,
                'total': 100,
                'step_progress': 0,
                'step_total': 1
            }
        }

        # 既存のステータスファイルを削除（進捗状況をリセット）
        status_file = os.path.join(
            app.config['OUTPUT_FOLDER'],
            app.config['JOBS'][job_id]['project_id'],
            'status.json'
        )
        if os.path.exists(status_file):
            os.remove(status_file)

        # パイプライン処理をキューに投入
        if process_pipeline(config_path, job_id=job_id):
            return True
        return False

    except Exception as e:
        print(f"Exception in pipeline: {str(e)}")
        app.config['JOBS'][job_id]['status'] = 'failed'
        app.config['JOBS'][job_id]['error'] = str(e)
        return False

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

def process_pipeline(config_path, job_id=None):
    """パイプライン処理をワーカーにエンキューする関数"""
    try:
        # RQジョブをデフォルトキューにエンキュー
        job = default_queue.enqueue(
            'worker.process_pipeline',
            config_path,
            job_id=job_id,
            job_timeout=3600,
            result_ttl=86400
        )
        print(f"Queued pipeline job {job.id} for config {config_path}")
        return True
            
    except Exception as e:
        print(f"Error in process_pipeline: {str(e)}")
        traceback.print_exc()
        raise

def handle_error(e: Exception, context: str = "") -> tuple:
    """共通のエラーハンドリング処理
    
    Args:
        e: 発生した例外
        context: エラーが発生した文脈を示す文字列
    
    Returns:
        tuple: (JSONレスポンス, HTTPステータスコード)
    """
    error_message = f"{context}: {str(e)}" if context else str(e)
    print(f"Error: {error_message}")
    traceback.print_exc()
    
    return jsonify({
        'error': error_message,
        'status': 'error',
        'timestamp': datetime.now().isoformat()
    }), 500

@app.route('/')
def index():
    if not init_app():
        return render_template(
            'index.html',
            error=True,
            message="初期化中にエラーが発生しました"
        )
    return render_template('index.html')

def initialize_job_params():
    """ジョブパラメータを初期化する関数

    Returns:
        tuple: (timestamp, output_dir, job_id)
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = f"project_{timestamp}"
    job_id = f"job_{timestamp}"

    return timestamp, output_dir, job_id

def process_custom_config(request):
    """設定ファイルとフォームからのプロンプト設定を処理する

    Args:
        request: Flaskリクエストオブジェクト

    Returns:
        dict: 処理されたカスタム設定

    Raises:
        ValueError: 設定ファイルのJSONフォーマットが不正な場合
    """
    print("=== PROCESSING CUSTOM CONFIG ===")
    print(f"Request files: {request.files}")
    print(f"Request form: {request.form}")
    custom_config = {}

    # 設定ファイルの処理
    if request.files.get('config'):
        config_file = request.files['config']
        config_content = config_file.read().decode('utf-8')
        try:
            custom_config = json.loads(config_content)
        except json.JSONDecodeError as e:
            raise ValueError(f"設定ファイルのJSONフォーマットが不正です: {str(e)}")

    # フォームからのプロンプト設定の追加
    for prompt_type in ['extraction', 'labelling', 'takeaways', 'overview']:
        if request.form.get(f'{prompt_type}Prompt'):
            if prompt_type not in custom_config:
                custom_config[prompt_type] = {}
            custom_config[prompt_type]['prompt'] = request.form[f'{prompt_type}Prompt']

    print(f"Resulting custom_config: {custom_config}")
    return custom_config

def process_spreadsheet_data(spreadsheet_url, base_filename, output_dir, custom_config):
    """スプレッドシートURLからデータを取得して処理する"""
    print(f"Processing spreadsheet: {spreadsheet_url}")

    # スプレッドシートからデータを取得
    df = get_spreadsheet_data(spreadsheet_url)
    print(f"Retrieved data shape: {df.shape}")

    # CSVファイルを保存
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"{base_filename}.csv")
    df.to_csv(filepath, index=False)
    print(f"Saved CSV to: {filepath}")
    
    # CSVファイルの前処理 - 追加
    preprocess_csv_file(filepath)

    # メイン設定を作成
    config = create_config(base_filename, output_dir, custom_config)
    config_path = os.path.join(app.config['CONFIG_FOLDER'], f"{output_dir}.json")
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

    return df, filepath, config_path

def setup_auto_update(spreadsheet_url, df, output_dir):
    """自動更新設定を作成し保存する

    Args:
        spreadsheet_url: スプレッドシートのURL
        df: 取得したデータのDataFrame
        output_dir: 出力ディレクトリ名

    Returns:
        dict: 自動更新設定
    """
    auto_update_config = {
        "enabled": True,
        "spreadsheet_url": spreadsheet_url,
        "content_hash": hashlib.md5(df.to_csv().encode()).hexdigest(),
        "last_update": datetime.now().isoformat(),
        "check_count": 0,
        "max_checks": UPDATE_INTERVALS['MAX_CHECK_COUNT'],
        "error_count": 0,
        "check_interval": UPDATE_INTERVALS['DEFAULT_INTERVAL_SECONDS'],
        "project_id": output_dir
    }

    auto_update_path = os.path.join(
        app.config['CONFIG_FOLDER'],
        f"{output_dir}_auto_update.json"
    )
    with open(auto_update_path, 'w', encoding='utf-8') as f:
        json.dump(auto_update_config, f, indent=2, ensure_ascii=False)
    print(f"Saved auto-update config to: {auto_update_path}")

    return auto_update_config

def process_csv_file(uploaded_file, base_filename, output_dir, custom_config):
    """アップロードされたCSVファイルを処理する

    Args:
        uploaded_file: アップロードされたCSVファイル
        base_filename: 保存するファイルの基本名
        output_dir: 出力ディレクトリ名
        custom_config: カスタム設定

    Returns:
        tuple: (DataFrameまたはNone, ファイルパス, 設定パス)
    """
    print(f"=== PROCESS_CSV_FILE DEBUG ===")
    print(f"Processing uploaded CSV file: {uploaded_file.filename}")
    print(f"base_filename: {base_filename}")
    print(f"output_dir: {output_dir}")
    print(f"custom_config: {json.dumps(custom_config, indent=2) if custom_config else 'None'}")

    # CSVファイルを一時保存
    filename = secure_filename(uploaded_file.filename)
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"{base_filename}.csv")
    uploaded_file.save(filepath)
    print(f"Saved CSV to: {filepath}")
    print(f"File exists: {os.path.exists(filepath)}")
    print(f"File size: {os.path.getsize(filepath) if os.path.exists(filepath) else 'N/A'}")
    
    # CSVファイルの前処理 - 追加
    preprocess_csv_file(filepath)
    
    try:
        df = pd.read_csv(filepath)
        print(f"CSV loaded successfully. Shape: {df.shape}")
        print(f"Columns: {', '.join(df.columns)}")
    except Exception as e:
        print(f"Error loading CSV: {str(e)}")

    # 設定ファイルを作成
    config = create_config(base_filename, output_dir, custom_config)
    print(f"Created config with input: {config['input']}")
    config_path = os.path.join(app.config['CONFIG_FOLDER'], f"{output_dir}.json")
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    print(f"Saved config to: {config_path}")
    print(f"Config file exists: {os.path.exists(config_path)}")
    
    # 保存した設定ファイルを読み込んで確認
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            saved_config = json.load(f)
            print(f"Saved config input value: {saved_config.get('input')}")
    except Exception as e:
        print(f"Error reading saved config: {str(e)}")
        
    print(f"=== END PROCESS_CSV_FILE DEBUG ===")

    return None, filepath, config_path

def initialize_job(job_id, project_id, auto_update=False):
    """ジョブ情報を初期化する

    Args:
        job_id: ジョブID
        project_id: プロジェクトID
        auto_update: 自動更新の有効/無効

    Returns:
        dict: ジョブ情報
    """
    job_info = {
        'status': 'queued',
        'auto_update': auto_update,
        'project_id': project_id,
        'started_at': datetime.now().isoformat(),
        'current_step': 'initialization',
        'progress': {'current': 0, 'total': 100}
    }

    # グローバルジョブ情報に追加
    app.config['JOBS'][job_id] = job_info

    return job_info

def enqueue_pipeline_job(config_path, job_id, job_meta=None):
    try:
        meta = job_meta or {}

        # APIキーが含まれる場合は安全なログ出力
        if meta and 'env' in meta and 'OPENAI_API_KEY' in meta['env']:
            safe_meta = meta.copy()
            safe_meta['env'] = {k: ('***' if k == 'OPENAI_API_KEY' else v) for k, v in safe_meta['env'].items()}
            print(f"Job metadata with sanitized env vars: {safe_meta}")

        # APIキーを含むメタデータを持つジョブはTTLを短く設定
        ttl_seconds = 1800  # 30分
        result_ttl_seconds = 86400  # 1日

        if job_meta and 'env' in job_meta and 'OPENAI_API_KEY' in job_meta['env']:
            ttl_seconds = 300  # キーを含む場合は5分だけ保持

        job = default_queue.enqueue(
            'worker.process_pipeline',
            config_path,
            job_id=job_id,
            job_timeout=3600,
            ttl=ttl_seconds,  # ジョブ自体の保持期間
            result_ttl=result_ttl_seconds,
            meta=meta
        )
        print(f"Queued pipeline job {job.id} for config {config_path}")

        return True
    except Exception as e:
        print(f"Error enqueuing pipeline job: {str(e)}")
        traceback.print_exc()
        raise

def generate_unique_project_id(spreadsheet_url, custom_config, request):
    """自動更新用の一意のプロジェクトIDを生成する

    Args:
        spreadsheet_url: スプレッドシートのURL
        custom_config: カスタム設定
        request: リクエストオブジェクト

    Returns:
        tuple: (project_id, output_dir, job_id)
    """
    # 設定ファイルの内容、プロンプト、URLのみでハッシュを生成
    unique_config = {
        'spreadsheet_url': spreadsheet_url,
        'custom_config': custom_config,
        'prompts': {
            'extraction': request.form.get('extractionPrompt', ''),
            'labelling': request.form.get('labellingPrompt', ''),
            'takeaways': request.form.get('takeawaysPrompt', ''),
            'overview': request.form.get('overviewPrompt', '')
        }
    }

    # アップロードされた設定ファイルの内容を追加
    if request.files.get('config'):
        config_file = request.files['config']
        config_content = config_file.read().decode('utf-8')
        unique_config['uploaded_config'] = config_content

    # ユニークなハッシュを生成（タイムスタンプを除外）
    unique_hash = hashlib.md5(
        json.dumps(unique_config, sort_keys=True).encode()
    ).hexdigest()[:8]

    project_id = f"auto_{unique_hash}"
    output_dir = project_id
    job_id = f"job_{project_id}"

    return project_id, output_dir, job_id

# app.pyの適切な場所（他のユーティリティ関数の近く）に追加してください

def preprocess_csv_file(filepath):
    """
    CSVファイルを前処理して、comment-bodyカラムが常に文字列として扱われ、
    extraction.pyで処理可能な形式になるようにします
    """
    try:
        print(f"=== PREPROCESS_CSV_FILE ===")
        print(f"Reading CSV file: {filepath}")
        
        # CSVファイルをバックアップ
        backup_path = f"{filepath}.bak"
        shutil.copy(filepath, backup_path)
        print(f"Created backup at: {backup_path}")
        
        # CSVを直接読み込み、quotechar設定を適切に行う
        df = pd.read_csv(filepath, quoting=csv.QUOTE_ALL, escapechar='\\')
        print(f"DataFrame shape: {df.shape}")
        print(f"DataFrame columns: {', '.join(df.columns)}")
        
        # comment-bodyカラムを確認
        if 'comment-body' in df.columns:
            # 最初の行の内容を表示（デバッグ用）
            if len(df) > 0:
                sample = df['comment-body'].iloc[0]
                print(f"Sample comment-body type: {type(sample).__name__}")
                print(f"Sample comment-body (first 100 chars): {str(sample)[:100]}...")
                
            # 各行を処理してJSONエンコード/デコード可能な形式に変換
            # これにより、後続のJSON処理でエラーが発生しにくくなる
            def normalize_text(text):
                if pd.isna(text):
                    return ""
                text_str = str(text).strip()
                # JSON内で問題になりそうな特殊文字をエスケープ
                return text_str.replace('"', '\\"')
                
            df['comment-body'] = df['comment-body'].apply(normalize_text)
            print(f"Normalized all comment-body values")
            
            # 変換後の最初の行を表示（デバッグ用）
            if len(df) > 0:
                sample = df['comment-body'].iloc[0]
                print(f"After conversion - type: {type(sample).__name__}")
                print(f"After conversion (first 100 chars): {str(sample)[:100]}...")
        
        # 処理したDataFrameを保存（クォーティングを明示的に指定）
        df.to_csv(filepath, index=False, quoting=csv.QUOTE_ALL)
        print(f"Saved processed CSV to: {filepath}")
        
        # 再度読み込んで検証
        df_verify = pd.read_csv(filepath)
        if 'comment-body' in df_verify.columns and len(df_verify) > 0:
            verify_sample = df_verify['comment-body'].iloc[0]
            print(f"Verification - type: {type(verify_sample).__name__}")
            print(f"Verification passed: {isinstance(verify_sample, str)}")
        
        print(f"=== END PREPROCESS_CSV_FILE ===")
        return True
        
    except Exception as e:
        print(f"Error preprocessing CSV file: {str(e)}")
        traceback.print_exc()
        return False

# upload_file関数を更新
@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        print("\n=== UPLOAD_FILE DEBUG ===")
        print(f"request.form: {request.form}")
        print(f"request.files: {request.files.keys()}")
        
        # ジョブパラメータの初期化
        timestamp, output_dir, job_id = initialize_job_params()
        base_filename = output_dir  # ファイル名とディレクトリ名を統一
        print(f"Generated job_id: {job_id}, output_dir: {output_dir}, base_filename: {base_filename}")
        
        # カスタム設定を処理
        try:
            custom_config = process_custom_config(request)
            print(f"Processed custom_config: {json.dumps(custom_config, indent=2)}")
        except ValueError as e:
            print(f"Error processing custom config: {str(e)}")
            return handle_error(e, "設定ファイルの処理中にエラーが発生しました")
        
        if request.form.get('spreadsheet_url'):
            # スプレッドシートURLからデータを処理
            spreadsheet_url = request.form['spreadsheet_url']
            auto_update = request.form.get('autoUpdate') == 'true'
            print(f"Processing spreadsheet URL: {spreadsheet_url}")
            print(f"Auto update enabled: {auto_update}")

            # 自動更新の場合は特別な処理
            if auto_update:
                project_id, output_dir, job_id = generate_unique_project_id(
                    spreadsheet_url, custom_config, request
                )
                base_filename = output_dir

            # ジョブ情報の初期化
            initialize_job(job_id, project_id, auto_update)

            # スプレッドシートデータの処理
            df, filepath, config_path = process_spreadsheet_data(
                spreadsheet_url, base_filename, output_dir, custom_config
            )

            # 自動更新設定を保存
            if auto_update:
                setup_auto_update(spreadsheet_url, df, output_dir)

        elif 'fileInput' in request.files and request.files['fileInput'].filename:
            # CSVファイルのアップロード処理
            uploaded_file = request.files['fileInput']
            print(f"Processing uploaded file: {uploaded_file.filename}")

            # 基本的なファイルチェック
            if not uploaded_file.filename.endswith('.csv'):
                return handle_error(ValueError("CSVファイルのみをアップロードしてください"), "不正なファイル形式")

            # ジョブ情報の初期化 - project_idを正しく定義
            project_id = output_dir
            initialize_job(job_id, project_id, False)
            print(f"Initialized job: {job_id}, project_id: {project_id}")

            # CSVファイルの処理
            _, filepath, config_path = process_csv_file(uploaded_file, base_filename, output_dir, custom_config)
            print(f"Processed CSV file. Path: {filepath}, Config: {config_path}")

            # APIキーの処理と取得（モデルに応じて）
            job_meta = {}
            api_key = request.form.get('openaiApiKey', '').strip()
            if (api_key and api_key.startswith('sk-')):
                job_meta['env'] = {'OPENAI_API_KEY': api_key}
                print("API key included in job meta")

            # パイプラインジョブをエンキュー
            print(f"Enqueueing pipeline job: {job_id}, config_path: {config_path}")
            enqueue_pipeline_job(config_path, job_id, job_meta)
            print(f"Pipeline job enqueued successfully")
        
        else:
            print("Error: No input file or spreadsheet URL provided")
            return handle_error(ValueError("CSVファイルまたはスプレッドシートURLを指定してください"), "入力データがありません")

        print("=== END UPLOAD_FILE DEBUG ===\n")
        return jsonify({
            'status': 'success',
            'job_id': job_id,
            'message': '処理を開始しました'
        })

    except Exception as e:
        print(f"Unexpected error in upload_file: {str(e)}")
        traceback.print_exc()
        return handle_error(e, "アップロード処理中にエラーが発生しました")

# 全ステップのリストを定義
PIPELINE_STEPS = [
    'extraction', 'embedding', 'clustering', 'labelling',
    'takeaways', 'overview', 'translation', 'aggregation', 'visualization'
]

@app.route('/status/<job_id>')
def job_status(job_id):
    try:
        if (job_id not in app.config['JOBS']) and job_id.startswith('job_auto_'):
            project_id = None
            # 全ての自動更新設定をチェック
            for filename in os.listdir(app.config['CONFIG_FOLDER']):
                if filename.endswith('_auto_update.json'):
                    auto_update_path = os.path.join(app.config['CONFIG_FOLDER'], filename)
                    with open(auto_update_path) as f:
                        auto_update_config = json.load(f)
                        if auto_update_config.get('current_job_id') == job_id:
                            project_id = filename.replace('_auto_update.json', '')
                            break

            if project_id:
                app.config['JOBS'][job_id] = {
                    'status': 'running',
                    'project_id': project_id,
                    'started_at': datetime.now().isoformat()
                }
            else:
                return jsonify({'status': 'not_found'}), 404
        elif job_id not in app.config['JOBS']:
            return jsonify({'status': 'not_found'}), 404
        
        job = app.config['JOBS'][job_id]
        project_id = job.get('project_id')
        
        if project_id:
            status_file = os.path.join(
                app.config['OUTPUT_FOLDER'],
                project_id,
                'status.json'
            )
            
            if os.path.exists(status_file):
                try:
                    with open(status_file) as f:
                        status_data = json.load(f)
                    
                    # エラー状態の確認
                    if 'error' in status_data:
                        job.update({
                            'status': 'failed',
                            'error': status_data['error'],
                            'traceback': status_data.get('error_stack_trace', '')
                        })
                    else:
                        # 進捗情報の計算
                        job['status'] = status_data.get('status', 'running')
                        current_job = status_data.get('current_job')
                        job['current_step'] = current_job
                        
                        # プログレス情報の更新
                        if current_job and current_job in PIPELINE_STEPS:
                            step_index = PIPELINE_STEPS.index(current_job)
                            current_progress = status_data.get('current_job_progress', 0)
                            total_tasks = status_data.get('current_job_tasks', 100)

                            try:
                                if current_progress is not None and total_tasks > 0:
                                    step_progress = (current_progress * 100) // total_tasks
                                else:
                                    step_progress = 0

                                overall_progress = ((step_index * 100) + step_progress) // len(PIPELINE_STEPS)

                                job['progress'] = {
                                    'current': overall_progress,
                                    'total': 100,
                                    'step_progress': step_progress,
                                    'step_total': 100
                                }
                            except (TypeError, ZeroDivisionError) as e:
                                print(f"Progress calculation error: {e}")
                                # エラー時はデフォルトの進捗状態を設定
                                job['progress'] = {
                                    'current': step_index * 100 // len(PIPELINE_STEPS),
                                    'total': 100,
                                    'step_progress': 0,
                                    'step_total': 100
                                }

                except json.JSONDecodeError as e:
                    print(f"Status file read error: {e}")
                    job['status'] = 'error'
                    job['error'] = 'ステータスファイルの読み取りに失敗しました'
        
        return jsonify(job)
        
    except Exception as e:
        return handle_error(e, "ジョブステータスの取得に失敗しました")

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

@app.route('/pipeline/outputs/')
def list_reports():
    """生成されたレポートの一覧を表示"""
    try:
        reports = []
        for dir_name in os.listdir(app.config['OUTPUT_FOLDER']):
            dir_path = os.path.join(app.config['OUTPUT_FOLDER'], dir_name)
            if os.path.isdir(dir_path):
                # レポートの存在確認
                report_path = os.path.join(dir_path, 'report', 'index.html')
                if os.path.exists(report_path):
                    try:
                        # 設定ファイルからレポート情報を取得
                        config_path = os.path.join(app.config['CONFIG_FOLDER'], f"{dir_name}.json")
                        auto_update_path = os.path.join(app.config['CONFIG_FOLDER'], f"{dir_name}_auto_update.json")
                        
                        # レポート名とその他の情報を取得
                        report_info = {
                            'name': dir_name.replace('project_', ''),  # デフォルト名
                            'url': f"/pipeline/outputs/{dir_name}/report/",
                            'created_at': datetime.fromtimestamp(os.path.getctime(dir_path)).strftime('%Y-%m-%d %H:%M'),
                            'auto_update': False
                        }

                        # 設定ファイルが存在する場合は情報を更新
                        if os.path.exists(config_path):
                            with open(config_path, 'r') as f:
                                config = json.load(f)
                                if 'name' in config:
                                    report_info['name'] = config['name']

                        # 自動更新設定が存在する場合はその情報を追加
                        if os.path.exists(auto_update_path):
                            with open(auto_update_path, 'r') as f:
                                auto_update_config = json.load(f)
                                report_info['auto_update'] = auto_update_config.get('enabled', False)

                        reports.append(report_info)

                    except Exception as e:
                        print(f"Error processing report {dir_name}: {str(e)}")
                        continue

        # 生成日時で降順ソート
        reports.sort(key=lambda x: x['created_at'], reverse=True)
        
        return render_template('reports.html', reports=reports)
        
    except Exception as e:
        return handle_error(e, "レポート一覧の取得に失敗しました")

# デバッグ用のエラーハンドラーを追加
@app.errorhandler(400)
def bad_request_error(error):
    print(f"400 Error: {error}")
    return jsonify({
        'error': 'Bad Request',
        'message': str(error),
        'debug_info': {
            'form_data': dict(request.form),
            'files': [f.filename for f in request.files.values()]
        }
    }), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)