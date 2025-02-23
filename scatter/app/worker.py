import os
import sys
from pathlib import Path
import json
import traceback
from datetime import datetime, timedelta
import pandas as pd
import hashlib
from app import get_spreadsheet_data

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
    return PIPELINE_DIR / config_path.replace('/app', '')

def process_pipeline(config_path: str, job_id: str = None, timeout: int = None) -> None:
    """パイプライン処理を実行"""
    original_dir = os.getcwd()
    original_argv = sys.argv
    
    try:
        if job_id:
            print(f"[DEBUG] Processing pipeline for job: {job_id}")
            
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
                # 次回のチェックをスケジュール
                schedule_next_check(
                    auto_update_config['spreadsheet_url'],
                    config_path,
                    auto_update_config['project_id']
                )

    except Exception as e:
        print(f"Error in process_pipeline: {str(e)}")
        traceback.print_exc()
        raise

    finally:
        sys.argv = original_argv
        os.chdir(original_dir)

def check_spreadsheet_updates(spreadsheet_url, config_path, project_id):
    """スプレッドシートの更新をチェックし、変更があれば処理を実行"""
    try:
        print(f"Checking for updates in project {project_id}")

        # 自動更新設定を読み込む
        auto_update_path = config_path.replace('.json', '_auto_update.json')
        auto_update_config = {}

        if os.path.exists(auto_update_path):
            with open(auto_update_path, 'r') as f:
                auto_update_config = json.load(f)

        if not auto_update_config.get('enabled', False):
            print(f"Auto update disabled for project {project_id}")
            return

        # スプレッドシートの内容を取得してハッシュ化
        df = get_spreadsheet_data(spreadsheet_url)
        current_hash = hashlib.md5(df.to_csv().encode()).hexdigest()
        last_hash = auto_update_config.get('content_hash')

        if current_hash != last_hash:
            print(f"Content changed for project {project_id}, triggering update")
            
            # パイプライン処理をキューに投入
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            job_id = f"job_auto_{timestamp}"
            
            redis_conn = Redis(host=REDIS_HOST, port=REDIS_PORT)
            default_queue = Queue('default', connection=redis_conn)
            
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

            print(f"Queued update job {job_id} for project {project_id}")
        else:
            print(f"No changes detected for project {project_id}")

        # 変更の有無に関わらず、次回のチェックをスケジュール
        schedule_next_check(spreadsheet_url, config_path, project_id)

    except Exception as e:
        print(f"Error in check_spreadsheet_updates: {str(e)}")
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
        print(f"Error reading auto_update_config: {str(e)}")
        check_interval = 300

    redis_conn = Redis(host=REDIS_HOST, port=REDIS_PORT)
    high_queue = Queue('high', connection=redis_conn)
    scheduler = Scheduler(queue=high_queue, connection=redis_conn)

    # 実行時刻の設定
    execute_at = datetime.now() + timedelta(seconds=check_interval)
    execute_timestamp = int(execute_at.timestamp())

    next_job = scheduler.enqueue_in(
        timedelta(seconds=check_interval),
        'worker.check_spreadsheet_updates',
        spreadsheet_url,
        config_path,
        project_id,
        timeout=300  # RQのタイムアウトオプション
    )
    
    # スケジューリング結果の確認（必要な場合のみ保持）
    scheduled_jobs = redis_conn.zrange('rq:scheduled:high', 0, -1, withscores=True)
    
    return next_job

if __name__ == '__main__':
    start_worker()