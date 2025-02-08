import os
import sys
from pathlib import Path

PIPELINE_DIR = Path('/workspaces/t3c-dev/src/ollama/talk-to-the-city-reports/scatter/pipeline')
sys.path.append(str(PIPELINE_DIR))

import main
from redis import Redis
from rq import Worker, Queue
from rq.worker import Worker

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
        
        sys.argv = [
            'main.py',
            os.path.relpath(pipeline_config, PIPELINE_DIR),
            '-skip-interaction'
        ]
        main.main()
        
    finally:
        sys.argv = original_argv
        os.chdir(original_dir)

def start_worker():
    """ワーカーを起動"""
    redis_conn = Redis(host=REDIS_HOST, port=REDIS_PORT)
    queue = Queue(connection=redis_conn)
    worker = Worker([queue], connection=redis_conn)
    worker.work()

if __name__ == '__main__':
    start_worker()