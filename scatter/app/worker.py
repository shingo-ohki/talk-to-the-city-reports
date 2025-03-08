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
debug_log(f"パイプラインディレクトリをPATHに追加: {PIPELINE_DIR}")
import main

from redis import Redis
from rq import Queue
from rq_scheduler import Scheduler

# 定数定義
REDIS_HOST = 'redis'
REDIS_PORT = 6379

def convert_config_path(config_path: str) -> Path:
    """設定ファイルのパスを変換"""
    debug_log(f"設定ファイルのパスを変換: {config_path}")
    converted_path = PIPELINE_DIR / config_path.replace('/app', '')
    debug_log(f"変換後のパス: {converted_path}")
    return converted_path

def process_pipeline(config_path: str, job_id: str = None, timeout: int = None) -> None:
    """パイプライン処理を実行"""
    debug_log(f"パイプライン処理開始: job_id={job_id}, config_path={config_path}")
    original_dir = os.getcwd()
    original_argv = sys.argv
    
    # 環境変数のバックアップ
    env_backup = {}
    
    try:
        # RQジョブのメタデータから環境変数を取得
        from rq import get_current_job
        job = get_current_job()
        debug_log(f"現在のジョブ: {job and job.id}")
        
        # 環境変数として一時的にAPIキーを設定
        if job and job.meta.get('env'):
            for key, value in job.meta['env'].items():
                env_backup[key] = os.environ.get(key)
                os.environ[key] = value
                debug_log(f"環境変数を設定: {key}={'***' if key == 'OPENAI_API_KEY' else value}")
        
        debug_log(f"OPENAI_API_KEY 環境変数の設定状態: {'OPENAI_API_KEY' in os.environ}")

        if job_id:
            debug_log(f"ジョブID: {job_id} の処理を開始")
            
        pipeline_config = convert_config_path(config_path)
        debug_log(f"パイプライン設定ファイル: {pipeline_config}")
        debug_log(f"現在のディレクトリを変更: {PIPELINE_DIR}")
        os.chdir(PIPELINE_DIR)
        
        # 設定ファイルの内容を出力
        try:
            with open(pipeline_config, 'r') as f:
                config_content = json.load(f)
                debug_log(f"設定ファイルの内容:")
                debug_log(f"- model: {config_content.get('model')}")
                debug_log(f"- question: {config_content.get('question')}")
                debug_log(f"- input: {config_content.get('input')}")
                
                # デバッグ: 設定ファイルの内容をより詳細に表示
                debug_log(f"設定ファイル全体: {json.dumps(config_content, indent=2)[:500]}...")
                
                # 入力ファイルの存在確認
                input_file = os.path.join(PIPELINE_DIR, 'inputs', f"{config_content.get('input', '')}")
                input_file_with_ext = os.path.join(PIPELINE_DIR, 'inputs', f"{config_content.get('input', '')}.csv")
                debug_log(f"- 入力ファイルパス (拡張子なし): {input_file}")
                debug_log(f"- 入力ファイルの存在 (拡張子なし): {os.path.exists(input_file)}")
                debug_log(f"- 入力ファイルパス (拡張子あり): {input_file_with_ext}")
                debug_log(f"- 入力ファイルの存在 (拡張子あり): {os.path.exists(input_file_with_ext)}")
                
                # inputsディレクトリの内容を表示
                debug_log(f"inputsディレクトリの内容: {os.listdir(os.path.join(PIPELINE_DIR, 'inputs'))}")
                
                # 入力ファイルの中身を確認
                if os.path.exists(input_file_with_ext):
                    try:
                        df = pd.read_csv(input_file_with_ext)
                        debug_log(f"- 入力ファイルの行数: {len(df)}")
                        debug_log(f"- 入力ファイルのカラム: {', '.join(df.columns)}")
                        debug_log(f"- 入力ファイルの先頭5行: {df.head(5)}")
                    except Exception as e:
                        debug_log(f"入力ファイル読み込みエラー: {str(e)}", "ERROR")
        except Exception as e:
            debug_log(f"設定ファイル読み込みエラー: {str(e)}", "ERROR")
        
        # パイプライン実行
        debug_log(f"パイプライン実行コマンド: main.py {os.path.relpath(pipeline_config, PIPELINE_DIR)} -skip-interaction -f")
        sys.argv = [
            'main.py',
            os.path.relpath(pipeline_config, PIPELINE_DIR),
            '-skip-interaction',
            '-f'
        ]
        
        debug_log("メインパイプライン処理の開始")
        main.main()
        debug_log("メインパイプライン処理の完了")

        # パイプライン成功後、自動更新設定をチェック
        auto_update_path = config_path.replace('.json', '_auto_update.json')
        debug_log(f"自動更新設定ファイルのチェック: {auto_update_path}")
        if os.path.exists(auto_update_path):
            with open(auto_update_path, 'r') as f:
                auto_update_config = json.load(f)
                
            if auto_update_config.get('enabled', False):
                # 次回のチェックをスケジュール
                debug_log(f"自動更新が有効、次回のチェックをスケジュール")
                schedule_next_check(
                    auto_update_config['spreadsheet_url'],
                    config_path,
                    auto_update_config['project_id']
                )

    except Exception as e:
        debug_log(f"パイプライン処理中にエラーが発生: {str(e)}", "ERROR")
        traceback.print_exc()
        raise

    finally:
        debug_log("パイプライン処理の後処理を実行")
        # 環境変数を元に戻す
        if job and job.meta.get('env'):
            for key in job.meta['env'].keys():
                # まずAPIキーを環境変数から確実に削除
                if key == 'OPENAI_API_KEY':
                    if key in os.environ:
                        del os.environ[key]
                        debug_log(f"環境変数を削除: {key}")
                    continue

                # 他の環境変数は元の値に復元
                if key in env_backup:
                    if env_backup[key] is not None:
                        os.environ[key] = env_backup[key]
                    else:
                        os.environ.pop(key, None)
                else:
                    os.environ.pop(key, None)
        
        # 処理完了時にメモリから確実にAPIキーを消去
        if 'env_backup' in locals() and 'OPENAI_API_KEY' in env_backup:
            env_backup['OPENAI_API_KEY'] = None

        debug_log(f"元のディレクトリに戻る: {original_dir}")
        sys.argv = original_argv
        os.chdir(original_dir)
        debug_log("パイプライン処理が完了")

def check_spreadsheet_updates(spreadsheet_url, config_path, project_id):
    """スプレッドシートの更新をチェックし、変更があれば処理を実行"""
    try:
        debug_log(f"プロジェクト {project_id} の更新をチェック")

        # 自動更新設定を読み込む
        auto_update_path = config_path.replace('.json', '_auto_update.json')
        auto_update_config = {}

        if os.path.exists(auto_update_path):
            with open(auto_update_path, 'r') as f:
                auto_update_config = json.load(f)

        if not auto_update_config.get('enabled', False):
            debug_log(f"プロジェクト {project_id} の自動更新は無効")
            return

        # スプレッドシートの内容を取得してハッシュ化
        debug_log(f"スプレッドシート {spreadsheet_url} からデータを取得")
        df = get_spreadsheet_data(spreadsheet_url)
        current_hash = hashlib.md5(df.to_csv().encode()).hexdigest()
        last_hash = auto_update_config.get('content_hash')

        if current_hash != last_hash:
            debug_log(f"プロジェクト {project_id} のコンテンツが変更されました。更新を実行します")
            
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

            debug_log(f"更新ジョブ {job_id} をキューに登録しました")
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

    # 実行時刻の設定
    execute_at = datetime.now() + timedelta(seconds=check_interval)
    execute_timestamp = int(execute_at.timestamp())

    debug_log(f"プロジェクト {project_id} の次回チェックをスケジュール: {execute_at}")
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
    debug_log(f"スケジュール済みのチェック数: {len(scheduled_jobs)}")
    
    return next_job

if __name__ == '__main__':
    debug_log("ワーカーの直接起動は未対応")