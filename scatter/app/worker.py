import os
import sys
from pathlib import Path
import json

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
        
        # -f と -skip-interaction オプションを追加
        sys.argv = [
            'main.py',
            os.path.relpath(pipeline_config, PIPELINE_DIR),
            '-skip-interaction',
            '-f'  # 強制実行オプションを追加
        ]
        main.main()
        
    finally:
        sys.argv = original_argv
        os.chdir(original_dir)

def start_worker():
    """ワーカーを起動"""
    redis_conn = Redis(host=REDIS_HOST, port=REDIS_PORT)
    queue = Queue(connection=redis_conn)
    
    # ワーカーはキューのみを監視
    worker = Worker([queue], connection=redis_conn)
    worker.work()

if __name__ == '__main__':
    start_worker()