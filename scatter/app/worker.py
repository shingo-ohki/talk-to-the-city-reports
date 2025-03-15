import os
import sys
from pathlib import Path
import json
import traceback
from datetime import datetime, timedelta
import pandas as pd
import hashlib
from app import get_spreadsheet_data

# デバッグログ用の関数を追加
def debug_log(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{level}][{timestamp}] {message}", flush=True)

# mainのインポートを前に移動
PIPELINE_DIR = Path('/workspaces/t3c-dev/src/ollama/talk-to-the-city-reports/scatter/pipeline')
sys.path.append(str(PIPELINE_DIR))
import main

from redis import Redis
from rq import Queue
from rq_scheduler import Scheduler

# 定数定義
REDIS_HOST = 'redis'
REDIS_PORT = 6379

def convert_config_path(config_path: str) -> Path:
    """設定ファイルのパスを変換"""
    converted_path = PIPELINE_DIR / config_path.replace('/app', '')
    return converted_path

def process_pipeline(config_path: str, job_id: str = None, timeout: int = None) -> None:
    """パイプライン処理を実行"""
    original_dir = os.getcwd()
    original_argv = sys.argv
    env_backup = {}
    
    try:
        try:
            with open(config_path, 'r') as f:
                config_content = json.load(f)

        except Exception as e:
            debug_log(f"設定ファイル読み込みエラー: {str(e)}", "ERROR")

        # RQジョブのメタデータから環境変数を取得
        from rq import get_current_job
        job = get_current_job()
        
        # 環境変数として一時的にAPIキーを設定
        if job and job.meta.get('env'):
            for key, value in job.meta['env'].items():
                env_backup[key] = os.environ.get(key)
                os.environ[key] = value
            
        pipeline_config = convert_config_path(config_path)
        os.chdir(PIPELINE_DIR)
        
        # パイプライン実行
        sys.argv = [
            'main.py',
            os.path.relpath(pipeline_config, PIPELINE_DIR),
            '-skip-interaction',
            '-f'
        ]
        
        main.main()

        # パイプライン成功後、自動更新設定をチェック
        auto_update_path = config_path.replace('.json', '_auto_update.json')
        if os.path.exists(auto_update_path):
            with open(auto_update_path, 'r') as f:
                auto_update_config = json.load(f)
                
            if auto_update_config.get('enabled', False):
                schedule_next_check(
                    auto_update_config['spreadsheet_url'],
                    config_path,
                    auto_update_config['project_id']
                )

    except Exception as e:
        traceback.print_exc()
        raise

    finally:
        # 環境変数を元に戻す
        if job and job.meta.get('env'):
            for key in job.meta['env'].keys():
                if key == 'OPENAI_API_KEY':
                    if key in os.environ:
                        del os.environ[key]
                    continue

                if key in env_backup:
                    if env_backup[key] is not None:
                        os.environ[key] = env_backup[key]
                    else:
                        os.environ.pop(key, None)
                else:
                    os.environ.pop(key, None)
        
        # APIキーを消去
        if 'env_backup' in locals() and 'OPENAI_API_KEY' in env_backup:
            env_backup['OPENAI_API_KEY'] = None

        sys.argv = original_argv
        os.chdir(original_dir)

def check_spreadsheet_updates(spreadsheet_url, config_path, project_id):
    """スプレッドシートの更新をチェックし、変更があれば処理を実行"""
    try:
        # 自動更新設定を読み込む
        auto_update_path = config_path.replace('.json', '_auto_update.json')
        auto_update_config = {}

        if os.path.exists(auto_update_path):
            with open(auto_update_path, 'r') as f:
                auto_update_config = json.load(f)

        if not auto_update_config.get('enabled', False):
            return

        # スプレッドシートの内容を取得
        df = get_spreadsheet_data(spreadsheet_url)

        # データフレームから直接ハッシュを計算
        # CSVに変換して文字列としてハッシュ化
        csv_content = df.to_csv(index=False).encode('utf-8')
        current_hash = hashlib.md5(csv_content).hexdigest()

        last_hash = auto_update_config.get('content_hash')

        if current_hash != last_hash:
            # パイプライン処理をキューに投入
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            job_id = f"job_auto_{timestamp}"
            
            redis_conn = Redis(host=REDIS_HOST, port=REDIS_PORT)
            default_queue = Queue('default', connection=redis_conn)
            
            # 処理用に実際のファイルを保存
            input_path = os.path.join(PIPELINE_DIR, 'inputs', f"{project_id}.csv")
            df.to_csv(input_path, index=False)

            job = default_queue.enqueue(
                'worker.process_pipeline',
                config_path,
                job_id=job_id,
                job_timeout=3600,
                result_ttl=86400,
                meta={'project_id': project_id}
            )

            # ハッシュを更新
            auto_update_config.update({
                'content_hash': current_hash,
                'last_update': datetime.now().isoformat(),
                'error_count': 0,
                'current_job_id': job_id
            })

            with open(auto_update_path, 'w') as f:
                json.dump(auto_update_config, f, indent=2)

        else:
            debug_log(f"プロジェクト {project_id} に変更はありません")

        # 変更の有無に関わらず、次回のチェックをスケジュール
        schedule_next_check(spreadsheet_url, config_path, project_id)

    except Exception as e:
        debug_log(f"スプレッドシート更新チェック中にエラー: {str(e)}", "ERROR")
        traceback.print_exc()
        raise

def schedule_next_check(spreadsheet_url, config_path, project_id):
    """次回のスプレッドシートチェックをスケジュールする共通処理"""
    auto_update_path = config_path.replace('.json', '_auto_update.json')
    try:
        with open(auto_update_path, 'r') as f:
            auto_update_config = json.load(f)
            check_interval = auto_update_config.get('check_interval', 86400)
    except Exception as e:
        debug_log(f"自動更新設定の読み込みエラー: {str(e)}", "ERROR")
        check_interval = 300

    redis_conn = Redis(host=REDIS_HOST, port=REDIS_PORT)
    high_queue = Queue('high', connection=redis_conn)
    scheduler = Scheduler(queue=high_queue, connection=redis_conn)

    next_job = scheduler.enqueue_in(
        timedelta(seconds=check_interval),
        'worker.check_spreadsheet_updates',
        spreadsheet_url,
        config_path,
        project_id,
        timeout=300  # RQのタイムアウトオプション
    )
    
    return next_job

if __name__ == '__main__':
    debug_log("ワーカーの直接起動は未対応")