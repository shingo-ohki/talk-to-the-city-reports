async function checkStatus(jobId) {
    const response = await fetch(`/status/${jobId}`);
    const data = await response.json();
    const status = document.getElementById('status');
    const statusMessage = document.getElementById('status-message');
    const progressBar = document.getElementById('progress-bar');
    const progressStep = document.getElementById('progress-step');
    const progressPercentage = document.getElementById('progress-percentage');
    const submitButton = document.querySelector('button[type="submit"]');

    status.style.display = 'block';

    if (data.status === 'completed') {
        const projectId = data.project_id || jobId.replace('job_', 'project_');
        status.className = 'status success';
        statusMessage.style.display = 'block';
        statusMessage.innerHTML = `処理が完了しました - <a href="/pipeline/outputs/${projectId}/report/">レポートを表示</a>`;
        progressBar.style.width = '100%';
        progressStep.textContent = '完了';
        progressPercentage.textContent = '100%';
        submitButton.disabled = false;
        submitButton.textContent = 'レポートを生成する';
    } else if (data.status === 'failed') {
        status.className = 'status error';
        statusMessage.style.display = 'block';
        statusMessage.textContent = `エラー: ${data.error}`;
        progressBar.style.width = '0%';
        progressStep.textContent = 'エラー';
        progressPercentage.textContent = '';
        submitButton.disabled = false;
        submitButton.textContent = 'レポートを生成する';
    } else if (data.status === 'running' || data.status === 'queued') {
        status.className = 'status';
        statusMessage.style.display = 'none';
        progressStep.textContent = data.current_step || '他のジョブを実行中です。処理を待っています...';
        
        if (data.progress) {
            progressBar.style.width = `${data.progress.current}%`;
            progressPercentage.textContent = `${data.progress.current}%`;
        }
        
        submitButton.disabled = true;
        submitButton.textContent = '処理中...';
        
        setTimeout(() => checkStatus(jobId), 2000);
    } else {
        // 処理中の表示
        status.className = 'status';
        statusMessage.style.display = 'none';   // 処理中は非表示
        submitButton.disabled = true;
        submitButton.textContent = '処理中...';
        
        if (data.current_step) {
            progressStep.textContent = `処理中: ${data.current_step}`;
            if (data.progress) {
                const percent = data.progress.current;
                progressBar.style.width = `${percent}%`;
                progressPercentage.textContent = `${percent}%`;
            }
        }
        
        setTimeout(() => checkStatus(jobId), 2000);
    }
}

function toggleSection(card) {
    const cardBody = card.querySelector('.card-body');
    const icon = card.querySelector('.toggle-icon');
    
    // Bootstrapのcollapseクラスを使用
    const bsCollapse = new bootstrap.Collapse(cardBody, {
        toggle: true
    });

    // アイコンの回転
    cardBody.addEventListener('shown.bs.collapse', function () {
        icon.style.transform = 'rotate(180deg)';
    });

    cardBody.addEventListener('hidden.bs.collapse', function () {
        icon.style.transform = 'rotate(0deg)';
    });
}

// コピー機能の修正
async function copyPrompt(targetId) {
    const textarea = document.getElementById(targetId);
    const textToCopy = textarea.placeholder;

    try {
        await navigator.clipboard.writeText(textToCopy);

        // コピー成功時のフィードバック
        const button = document.querySelector(`button[onclick="copyPrompt('${targetId}')"]`);
        const icon = button.querySelector('.copy-icon');
        const originalText = icon.textContent;

        // 視覚的フィードバック
        icon.textContent = '✓';
        button.classList.add('btn-success');
        button.classList.remove('btn-outline-secondary');

        setTimeout(() => {
            icon.textContent = originalText;
            button.classList.remove('btn-success');
            button.classList.add('btn-outline-secondary');
        }, 2000);
    } catch (err) {
        console.error('クリップボードへのコピーに失敗しました:', err);
    }
}

document.addEventListener('DOMContentLoaded', () => {
    // デフォルトのプロンプトを取得
    fetch('/sample-config')
        .then(r => r.json())
        .then(config => {
            // 各プロンプトのデフォルト値を placeholder に設定
            if (config.extraction && config.extraction.prompt) {
                document.getElementById('extractionPrompt').placeholder = config.extraction.prompt;
            }
            if (config.labelling && config.labelling.prompt) {
                document.getElementById('labellingPrompt').placeholder = config.labelling.prompt;
            }
            if (config.takeaways && config.takeaways.prompt) {
                document.getElementById('takeawaysPrompt').placeholder = config.takeaways.prompt;
            }
            if (config.overview && config.overview.prompt) {
                document.getElementById('overviewPrompt').placeholder = config.overview.prompt;
            }
        });

    // オプションセクションを初期状態で折りたたむ
    document.querySelectorAll('.collapsible .section-content').forEach(content => {
        content.classList.add('collapsed');
    });

    document.getElementById('uploadForm').onsubmit = async (e) => {
        e.preventDefault();

        // スクロールを画面上部に移動
        window.scrollTo({
            top: 0,
            behavior: 'smooth'  // スムーズスクロール
        });

        // ステータス表示要素の取得と初期化
        const status = document.getElementById('status');
        const statusMessage = document.getElementById('status-message');
        const submitButton = e.target.querySelector('button[type="submit"]');

        try {
            // 入力チェック
            const file = document.getElementById('fileInput').files[0];
            const spreadsheetUrl = document.getElementById('spreadsheetUrl').value.trim();
            const autoUpdate = document.getElementById('autoUpdate').checked;
            const configFile = document.getElementById('configFile').files[0];

            // デバッグ用のログ出力を追加
            console.log('File:', file);
            console.log('Spreadsheet URL:', spreadsheetUrl);

            // 入力チェック
            if (!file && !spreadsheetUrl) {
                status.className = 'alert alert-danger';
                status.classList.remove('d-none');
                statusMessage.textContent = 'ファイルまたはスプレッドシートURLを入力してください';
                return;
            }

            // FormDataの作成
            const formData = new FormData();

            // 必須データの追加
            if (file) {
                formData.append('fileInput', file);
            }
            if (spreadsheetUrl) {
                formData.append('spreadsheet_url', spreadsheetUrl);
                if (autoUpdate) {
                    formData.append('autoUpdate', 'true');
                }
            }

            // FormDataの内容確認を改善
            for (let pair of formData.entries()) {
                console.log(`FormData entry - ${pair[0]}: `, pair[1]);
            }

            // 設定ファイルの追加
            if (configFile) {
                formData.append('config', configFile);
            }

            // OpenAI APIキーの追加
            const apiKey = document.getElementById('openaiApiKey')?.value?.trim();
            if (apiKey) {
                formData.append('openaiApiKey', apiKey);
                console.log('API Key provided: Yes (masked)'); // キーの内容は絶対にログ出力しない

                // 処理完了後にフォームからAPIキーをクリア（セキュリティ対策）
                setTimeout(() => {
                    document.getElementById('openaiApiKey').value = '';
                }, 5000); // 5秒後にクリア（送信完了を待つ）
            }

            // カスタム設定の作成
            const customConfig = {};

            // プロンプトの追加
            ['extraction', 'labelling', 'takeaways', 'overview'].forEach(type => {
                const promptValue = document.getElementById(`${type}Prompt`).value.trim();
                if (promptValue) {
                    customConfig[type] = { prompt: promptValue };
                }
            });

            // カスタム設定がある場合、JSON文字列として追加
            if (Object.keys(customConfig).length > 0) {
                formData.append('custom_config', JSON.stringify(customConfig));
            }

            // 送信ボタンを無効化
            submitButton.disabled = true;
            submitButton.textContent = '処理中...';

            // ステータス表示を初期化
            status.className = 'alert alert-info';
            status.classList.remove('d-none');
            statusMessage.textContent = '処理を開始しています...';

            // APIリクエストのデバッグを追加
            const response = await fetch('/upload', {
                method: 'POST',
                body: formData
            });

            console.log('Response status:', response.status);
            const data = await response.json();
            console.log('Response data:', data);

            if (response.ok) {
                // ステータスチェックの開始
                checkStatus(data.job_id);
            } else {
                throw new Error(data.error || 'アップロードに失敗しました');
            }

        } catch (error) {
            // エラー表示
            status.className = 'alert alert-danger';
            status.classList.remove('d-none');
            statusMessage.textContent = `エラーが発生しました: ${error.message}`;
            
            // 送信ボタンを再度有効化
            submitButton.disabled = false;
            submitButton.textContent = 'レポートを生成する';
        }
    };

    // フォーム送信前にOpenAIモデル使用時にAPIキーが設定されているか確認する
    document.getElementById('uploadForm').addEventListener('submit', function(e) {
        // 設定ファイルからモデル情報を取得
        const configFile = document.getElementById('configFile').files[0];
        
        if (configFile) {
          const reader = new FileReader();
          reader.onload = function(event) {
            try {
              const config = JSON.parse(event.target.result);
              const model = config.model || '';
              
              // OpenAIモデルの判定
              const useOpenAI = model.startsWith('gpt-') || model.startsWith('text-');
              
              if (useOpenAI) {
                const apiKey = document.getElementById('openaiApiKey').value;
                if (!apiKey || !apiKey.startsWith('sk-')) {
                  e.preventDefault();
                  alert('OpenAIモデルを使用するにはAPIキーが必要です');
                  document.getElementById('apiKeyCollapse').classList.add('show');
                  document.getElementById('openaiApiKey').focus();
                }
              }
            } catch(e) {
              console.error('設定ファイルの解析に失敗:', e);
            }
          };
          reader.readAsText(configFile);
        }
    });

    // 古いコピーボタンのイベントリスナーを削除
    document.querySelectorAll('.copy-button').forEach(button => {
        button.replaceWith(button.cloneNode(true));
    });

    // コピーボタンの機能を追加
    document.querySelectorAll('.copy-button').forEach(button => {
        button.addEventListener('click', async () => {
            const targetId = button.getAttribute('data-target');
            const textarea = document.getElementById(targetId);
            const placeholder = textarea.placeholder;
            
            try {
                await navigator.clipboard.writeText(placeholder);
                
                // コピー成功時のフィードバック
                const tooltip = button.querySelector('.copy-tooltip');
                const originalText = tooltip.textContent;
                tooltip.textContent = 'コピーしました！';
                
                setTimeout(() => {
                    tooltip.textContent = originalText;
                }, 2000);
            } catch (err) {
                console.error('クリップボードへのコピーに失敗しました:', err);
            }
        });
    });
});