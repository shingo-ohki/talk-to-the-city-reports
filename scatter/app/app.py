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
from rq_scheduler import Scheduler
import hashlib
import pandas as pd
import traceback  # tracebackモジュールを追加
import sys
from pathlib import Path
import importlib.util

# グローバル変数として定義
init_complete = False
initialization_error = None

app = Flask(__name__, static_folder='static')

base_path = '/workspaces/t3c-dev/src/ollama/talk-to-the-city-reports/scatter/pipeline'
app.config.update({
    'UPLOAD_FOLDER': os.path.join(base_path, 'inputs'),
    'OUTPUT_FOLDER': os.path.join(base_path, 'outputs'),
    'CONFIG_FOLDER': os.path.join(base_path, 'configs'),
    'MAX_CONTENT_LENGTH': 16 * 1024 * 1024,
    'JOBS': {}
})

redis_conn = Redis(host='redis', port=6379)
high_priority_queue = Queue('high', connection=redis_conn)    # スケジューラー用の優先キュー
default_queue = Queue('default', connection=redis_conn)       # 通常の処理用キュー
scheduler = Scheduler(queue=high_priority_queue, connection=redis_conn)  # スケジューラーは優先キューを使用
q = high_priority_queue

# アプリケーション初期化用のコマンドグループを作成
init_cli = AppGroup('init')

# 必要なディレクトリを作成する関数
def ensure_directories():
    for directory in [
        app.config['UPLOAD_FOLDER'],
        app.config['OUTPUT_FOLDER'],
        app.config['CONFIG_FOLDER']
    ]:
        os.makedirs(directory, exist_ok=True)

def init_app():
    """アプリケーションの初期化を非同期で実行"""
    global init_complete, initialization_error
    try:
        if init_complete:
            return

        ensure_directories()

        # スケジューラー関連の初期化を非同期キューに投入
        q.enqueue(
            'app.init_scheduler',
            job_timeout=60,
            description='Application initialization'
        )

        init_complete = True
        print("Application initialization completed")
    except Exception as e:
        initialization_error = str(e)
        print(f"Error during initialization: {str(e)}")
        traceback.print_exc()


@init_cli.command('app')
@with_appcontext
def init_command():
    """アプリケーションの初期化を実行"""
    init_app()

UPDATE_INTERVALS = {
    'DEFAULT_INTERVAL_SECONDS': 300, # 86400,  # 24時間
    'MAX_CHECK_COUNT': 30,       # 最大チェック回数
    'MAX_ERROR_COUNT': 3,        # 最大連続エラー回数
}

def schedule_spreadsheet_check(spreadsheet_url, config_path, project_id):
    """定期実行ジョブのスケジューリング"""
    try:
        # 既存のジョブをキャンセル
        for job in scheduler.get_jobs():
            if job.meta.get('project_id') == project_id:
                scheduler.cancel(job)

        # 5分後の時刻を設定
        now = datetime.now(timezone.utc)
        next_run = now + timedelta(seconds=UPDATE_INTERVALS['DEFAULT_INTERVAL_SECONDS'])

        # ジョブをスケジュール（軽量なキューイング関数を使用）
        job = scheduler.schedule(
            scheduled_time=next_run,
            func='worker.queue_spreadsheet_check',  # キューイング用の軽量な関数
            args=[spreadsheet_url, config_path, project_id],
            interval=UPDATE_INTERVALS['DEFAULT_INTERVAL_SECONDS'],
            repeat=UPDATE_INTERVALS['MAX_CHECK_COUNT'],
            queue_name='high',  # 優先キューを使用
            meta={
                'project_id': project_id,
                'interval': UPDATE_INTERVALS['DEFAULT_INTERVAL_SECONDS'],
                'next_run': next_run.isoformat(),
                'repeat': UPDATE_INTERVALS['MAX_CHECK_COUNT']
            }
        )

        print(f"Scheduled job {job.id} for project {project_id}")
        print(f"First run scheduled at: {next_run.isoformat()}")
        return job.id

    except Exception as e:
        print(f"Error scheduling job: {str(e)}")
        traceback.print_exc()
        raise

def queue_spreadsheet_check(spreadsheet_url, config_path, project_id):
    """実際の更新チェック処理をデフォルトキューに投入"""
    try:
        # 更新チェック処理を通常キューに投入
        job = default_queue.enqueue(
            'worker.check_spreadsheet_updates',
            spreadsheet_url,
            config_path,
            project_id,
            job_timeout=300
        )
        print(f"Queued spreadsheet check job {job.id} for project {project_id}")
        return job.id
    except Exception as e:
        print(f"Error queueing spreadsheet check: {str(e)}")
        traceback.print_exc()
        raise

# 2. リストア関数を定義
def restore_scheduled_jobs():
    """アプリケーション起動時に自動更新ジョブを復元する"""
    try:
        config_dir = app.config['CONFIG_FOLDER']
        for filename in os.listdir(config_dir):
            if filename.endswith('_auto_update.json'):
                project_id = filename.replace('_auto_update.json', '')
                auto_update_path = os.path.join(config_dir, filename)
                config_path = os.path.join(config_dir, f"{project_id}.json")

                with open(auto_update_path, 'r') as f:
                    auto_update_config = json.load(f)

                if auto_update_config.get('enabled', False):
                    schedule_spreadsheet_check(
                        auto_update_config['spreadsheet_url'],
                        config_path,
                        project_id
                    )
                    print(f"Restored scheduled job for project: {project_id}")

    except Exception as e:
        print(f"Error restoring scheduled jobs: {str(e)}")
        traceback.print_exc()

def init_scheduler():
    """スケジューラーの初期化を行う"""
    try:
        # 既存のジョブをすべてクリア
        for job in scheduler.get_jobs():
            scheduler.cancel(job)

        config_dir = app.config['CONFIG_FOLDER']
        processed_projects = set()

        for filename in os.listdir(config_dir):
            if filename.endswith('_auto_update.json'):
                project_id = filename.replace('_auto_update.json', '')

                # 重複チェック
                if project_id in processed_projects:
                    print(f"Skipping duplicate project: {project_id}")
                    continue

                auto_update_path = os.path.join(config_dir, filename)
                config_path = os.path.join(config_dir, f"{project_id}.json")

                with open(auto_update_path, 'r') as f:
                    auto_update_config = json.load(f)

                if auto_update_config.get('enabled', False):
                    # 直接スケジュール（非同期キューを使用しない）
                    schedule_spreadsheet_check(
                        auto_update_config['spreadsheet_url'],
                        config_path,
                        project_id
                    )
                    print(f"Restored scheduled job for project: {project_id}")
    except Exception as e:
        print(f"Error in scheduler initialization: {str(e)}")
        traceback.print_exc()

def create_config(filename, output_dir, custom_config=None):
    """設定ファイルを作成する関数"""
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
            "prompt": "/system\n\n与えられた投稿を要約し、JSONリストとして返してください。\n追加の説明は含めず、必ずJSONリストのみを返してください。\n予め提供された例や過去の回答を含めないでください。\n投稿内容を要約することができない旨の出力だった場合は、要約せずに与えられた投稿JSONリストとして出力してください\n\n例:\n[\"要約された意見\"]\n\n注意:\n- JSONリストのみを出力\n- システムメッセージや注釈を含めない\n- 過去の例を含めない"
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

        # カスタム設定のマージ（パイプライン設定のみ）
        pipeline_config.update(custom_config)

    # 4. 各設定ファイルの保存
    save_pipeline_config(output_dir, pipeline_config)
    save_app_config(output_dir, app_config)

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
        if process_pipeline(config_path):
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

def process_pipeline(config_path):
    """パイプライン処理をワーカーにエンキューする関数"""
    try:
        # RQジョブをデフォルトキューにエンキュー
        job = default_queue.enqueue(
            'worker.process_pipeline',
            config_path,
            job_timeout=3600,
            result_ttl=86400
        )
        print(f"Queued pipeline job {job.id} for config {config_path}")
        return True
            
    except Exception as e:
        print(f"Error in process_pipeline: {str(e)}")
        traceback.print_exc()
        raise

@app.route('/')
def index():
    if not init_complete:
        # 初回アクセス時に初期化
        init_app()

    if initialization_error:
        return render_template(
            'index.html',
            error=True,
            message=f"初期化中にエラーが発生しました: {initialization_error}"
        )
    elif not init_complete:
        return render_template(
            'index.html',
            initializing=True,
            message="アプリケーションを初期化中です..."
        )

    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        # リクエストの内容を詳細にログ出出力
        print("=== Request Debug ===")
        print("Form Data:")
        for key, value in request.form.items():
            print(f"{key}: {value}")
        print("\nFiles:")
        for key, file in request.files.items():
            print(f"{key}: {file.filename if file else None}")
        print("===================")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = f"project_{timestamp}"
        job_id = f"job_{timestamp}"  # job_idをここで定義
        project_id = output_dir      # project_idをここで定義
        custom_config = {}
        base_filename = output_dir
        config_path = None

        # カスタム設定の処理を追加
        custom_config = {}
        
        # 設定ファイルの処理
        if request.files.get('config'):
            config_file = request.files['config']
            config_content = config_file.read().decode('utf-8')
            try:
                custom_config = json.loads(config_content)
                print(f"Loaded custom config: {custom_config}")
            except json.JSONDecodeError as e:
                raise ValueError(f"設定ファイルのJSONフォーマットが不正です: {str(e)}")

        # フォームからのプロンプト設定の追加
        for prompt_type in ['extraction', 'labelling', 'takeaways', 'overview']:
            if request.form.get(f'{prompt_type}Prompt'):
                if prompt_type not in custom_config:
                    custom_config[prompt_type] = {}
                custom_config[prompt_type]['prompt'] = request.form[f'{prompt_type}Prompt']

        # スプレッドシートURLと自動更新の処理
        if request.form.get('spreadsheet_url'):
            try:
                spreadsheet_url = request.form['spreadsheet_url']
                auto_update = request.form.get('autoUpdate') == 'true'
                print(f"Processing spreadsheet: {spreadsheet_url}")
                print(f"Auto update enabled: {auto_update}")

                if auto_update:
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
                    base_filename = output_dir
                    job_id = f"job_{project_id}"

                # RQジョブ情報を設定
                app.config['JOBS'][job_id] = {
                    'status': 'queued',
                    'auto_update': auto_update if request.form.get('spreadsheet_url') else False,
                    'project_id': project_id,
                    'started_at': datetime.now().isoformat(),
                    'current_step': 'initialization',
                    'progress': {'current': 0, 'total': 100}
                }

                # スプレッドシートからデータを取得
                df = get_spreadsheet_data(spreadsheet_url)
                print(f"Retrieved data shape: {df.shape}")
                
                # CSVファイルを保存
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"{base_filename}.csv")
                df.to_csv(filepath, index=False)
                print(f"Saved CSV to: {filepath}")

                # メイン設定を作成
                config = create_config(base_filename, output_dir, custom_config)
                config_path = os.path.join(app.config['CONFIG_FOLDER'], f"{output_dir}.json")
                with open(config_path, 'w', encoding='utf-8') as f:
                    json.dump(config, f, indent=2, ensure_ascii=False)

                if auto_update:
                    auto_update_config = {
                        "enabled": True,
                        "spreadsheet_url": spreadsheet_url,
                        "content_hash": hashlib.md5(df.to_csv().encode()).hexdigest(),
                        "last_update": datetime.now().isoformat(),
                        "check_count": 0,
                        "max_checks": UPDATE_INTERVALS['MAX_CHECK_COUNT'],
                        "error_count": 0,
                        "check_interval": UPDATE_INTERVALS['DEFAULT_INTERVAL_SECONDS']
                    }
                    
                    auto_update_path = os.path.join(
                        app.config['CONFIG_FOLDER'],
                        f"{output_dir}_auto_update.json"
                    )
                    with open(auto_update_path, 'w', encoding='utf-8') as f:
                        json.dump(auto_update_config, f, indent=2, ensure_ascii=False)
                    print(f"Saved auto-update config to: {auto_update_path}")

                    # 自動更新のスケジュール
                    schedule_spreadsheet_check(spreadsheet_url, config_path, project_id)
                    print("Scheduled auto-update check")

            except Exception as e:
                print(f"Error processing spreadsheet: {str(e)}")
                traceback.print_exc()  # スタックトレースを出力
                raise

        default_queue.enqueue(
            'worker.process_pipeline',
            config_path,
            job_id=job_id,
            job_timeout=3600
        )
        
        return jsonify({
            'success': True,
            'message': '処理を開始しました',
            'job_id': job_id,
            'project_id': project_id
        })

    except Exception as e:
        print(f"Upload error: {str(e)}")
        traceback.print_exc()  # スタックトレースを出力
        return jsonify({'error': f'エラーが発生しました: {str(e)}'}), 500

# 全ステップのリストを定義
PIPELINE_STEPS = [
    'extraction', 'embedding', 'clustering', 'labelling',
    'takeaways', 'overview', 'translation', 'aggregation', 'visualization'
]

@app.route('/status/<job_id>')
def job_status(job_id):
    try:
        if job_id not in app.config['JOBS']:
            # 自動更新ジョブの場合は、JOBSに追加
            if job_id.startswith('job_auto_'):
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
            else:
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
        print(f"Job status error: {str(e)}")
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

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
        return jsonify({'error': f'レポート一覧の取得に失敗しました: {str(e)}'}), 500

@app.route('/debug/scheduler')
def debug_scheduler():
    try:
        scheduled_jobs = []
        for job in scheduler.get_jobs():
            job_info = {
                'id': job.id,
                'func': job.func_name,
                'args': job.args,
                'kwargs': job.kwargs,
                'scheduled_time': job.scheduled_time.isoformat() if hasattr(job, 'scheduled_time') else None,
                'interval': job.meta.get('interval'),
                'project_id': job.meta.get('project_id')
            }
            scheduled_jobs.append(job_info)

        # 実行済みジョブの履歴も取得
        completed_jobs = []
        for key in redis_conn.keys('rq:job:*'):
            job_data = redis_conn.hgetall(key)
            if job_data:
                completed_jobs.append({
                    'id': key.decode('utf-8'),
                    'status': job_data.get(b'status', b'unknown').decode('utf-8'),
                    'enqueued_at': job_data.get(b'enqueued_at', b'unknown').decode('utf-8'),
                    'started_at': job_data.get(b'started_at', b'unknown').decode('utf-8'),
                    'ended_at': job_data.get(b'ended_at', b'unknown').decode('utf-8')
                })

        return jsonify({
            'scheduled_jobs': scheduled_jobs,
            'completed_jobs': completed_jobs,
            'current_time': datetime.now(timezone.utc).isoformat()
        })

    except Exception as e:
        return jsonify({
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500

@app.route('/debug/scheduler/details')
def debug_scheduler_details():
    try:
        jobs_info = []
        for job in scheduler.get_jobs():
            # jobのscheduled_timeはRQがjsonシリアライズ可能な形式で保存している
            scheduled_time = job.meta.get('next_run')  # metaから次回実行時刻を取得

            jobs_info.append({
                'id': job.id,
                'func': job.func_name,
                'args': job.args,
                'scheduled_time': scheduled_time,  # metaから取得した時刻を使用
                'interval': job.meta.get('interval'),
                'meta': job.meta
            })
        
        return jsonify({
            'jobs': jobs_info,
            'current_time': datetime.now(timezone.utc).isoformat(),
            'scheduler_info': {
                'connection_info': str(scheduler.connection)
            }
        })
    except Exception as e:
        print(f"Scheduler details error: {str(e)}")
        traceback.print_exc()
        return jsonify({
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500

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