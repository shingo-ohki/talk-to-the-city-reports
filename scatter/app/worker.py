import os
import sys
from pathlib import Path
import json
import traceback
from datetime import datetime, timedelta

PIPELINE_DIR = Path('/workspaces/t3c-dev/src/ollama/talk-to-the-city-reports/scatter/pipeline')
sys.path.append(str(PIPELINE_DIR))

import main
from redis import Redis
from rq import Worker, Queue
from rq.worker import Worker
from rq_scheduler import Scheduler

# 定数定義
REDIS_HOST = 'redis'
REDIS_PORT = 6379

def convert_config_path(config_path: str) -> Path:
    """設定ファイルのパスを変換"""
    return PIPELINE_DIR / config_path.replace('/app', '')

def process_pipeline(config_path: str, timeout: int = None) -> None:
    """パイプライン処理を実行"""
    original_dir = os.getcwd()
    original_argv = sys.argv
    
    try:
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

    except Exception as e:
        print(f"Error in process_pipeline: {str(e)}")
        traceback.print_exc()
        raise

    finally:
        sys.argv = original_argv
        os.chdir(original_dir)

def start_worker():
    """ワーカーを起動"""
    redis_conn = Redis(host=REDIS_HOST, port=REDIS_PORT)
    queue = Queue(connection=redis_conn)
    
    worker = Worker([queue], connection=redis_conn)
    worker.work()

def queue_spreadsheet_check(spreadsheet_url, config_path, project_id):
    """スプレッドシート更新チェック処理をキューに投入"""
    try:
        # Redisへの接続
        redis_conn = Redis(host='redis', port=6379)
        default_queue = Queue('default', connection=redis_conn)

        # 実際の更新チェック処理を通常キューに投入
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
            return

        # スプレッドシートの内容を取得してハッシュ化
        df = pd.read_csv(spreadsheet_url)
        current_hash = hashlib.md5(df.to_csv().encode()).hexdigest()
        last_hash = auto_update_config.get('content_hash')

        if current_hash != last_hash:
            print(f"Content changed for project {project_id}, triggering update")

            # 新しいjob_idを生成
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            job_id = f"job_auto_{timestamp}"

            # Redis接続を取得
            redis_conn = Redis(host='redis', port=6379)
            default_queue = Queue('default', connection=redis_conn)

            # パイプライン処理をキューに投入（job_idを指定）
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
                'current_job_id': job_id  # 現在のjob_idを保存
            })

            with open(auto_update_path, 'w') as f:
                json.dump(auto_update_config, f, indent=2)

            print(f"Queued update job {job_id} for project {project_id}")

    except Exception as e:
        print(f"Error in check_spreadsheet_updates: {str(e)}")
        traceback.print_exc()
        raise

if __name__ == '__main__':
    start_worker()