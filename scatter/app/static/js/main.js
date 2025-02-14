async function checkStatus(jobId) {
    const response = await fetch(`/status/${jobId}`);
    const data = await response.json();
    const status = document.getElementById('status');
    const statusMessage = document.getElementById('status-message');
    const progressBar = document.getElementById('progress-bar');
    const progressStep = document.getElementById('progress-step');
    const progressPercentage = document.getElementById('progress-percentage');
    const progressInfo = document.getElementById('progress-info');
    const submitButton = document.querySelector('button[type="submit"]');

    status.style.display = 'block';

    if (data.status === 'completed') {
        const projectId = jobId.replace('job_', 'project_');
        status.className = 'status success';
        statusMessage.innerHTML = `処理が完了しました<br>
            <a href="/pipeline/outputs/${projectId}/report/">レポートを表示</a>`;
        progressBar.style.width = '100%';
        progressStep.textContent = '完了';
        progressPercentage.textContent = '100%';
        progressInfo.textContent = '';
        submitButton.disabled = false;
        submitButton.textContent = 'レポートを生成する';
    } else if (data.status === 'failed') {
        status.className = 'status error';
        statusMessage.textContent = `エラー: ${data.error}`;
        progressBar.style.width = '0%';
        progressStep.textContent = 'エラー';
        progressPercentage.textContent = '';
        progressInfo.textContent = '';
        submitButton.disabled = false;
        submitButton.textContent = 'レポートを生成する';
    } else {
        status.className = 'status';
        statusMessage.textContent = '処理中...';
        submitButton.disabled = true;
        submitButton.textContent = '処理中...';
        
        if (data.current_step) {
            progressStep.textContent = `処理中: ${data.current_step}`;
            if (data.progress) {
                const percent = data.progress.current;
                progressBar.style.width = `${percent}%`;
                progressPercentage.textContent = `${percent}%`;
                
                // 進捗の詳細表示を更新
                const noProgressSteps = ['embedding', 'clustering', 'translation', 'aggregation', 'visualization'];
                
                if (!noProgressSteps.includes(data.current_step) && 
                    data.progress.step_progress !== undefined && 
                    data.progress.step_total !== undefined) {
                    // 詳細な進捗を表示するステップの場合
                    progressInfo.textContent = `${data.current_step}: ${data.progress.step_progress}/${data.progress.step_total}`;
                } else {
                    // 進捗を表示しないステップの場合は現在のステップ名のみ表示
                    progressInfo.textContent = `${data.current_step}`;
                }
            } else {
                // progressが未定義の場合はステップ名のみ表示
                progressInfo.textContent = data.current_step;
            }
        } else {
            // current_stepが未定義の場合は進捗情報をクリア
            progressInfo.textContent = '';
        }
        
        setTimeout(() => checkStatus(jobId), 2000);
    }
}

function toggleSection(header) {
    const section = header.parentElement;
    const content = section.querySelector('.section-content');
    const icon = header.querySelector('.toggle-icon');
    
    content.classList.toggle('collapsed');
    header.classList.toggle('active');
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

        const submitButton = e.target.querySelector('button[type="submit"]');
        submitButton.disabled = true;
        submitButton.textContent = '処理中...';

        window.scrollTo({
            top: 0,
            behavior: 'smooth'
        });

        const status = document.getElementById('status');
        const statusMessage = document.getElementById('status-message');
        const progressInfo = document.getElementById('progress-info');
        const formData = new FormData();
        const file = document.getElementById('fileInput').files[0];
        const spreadsheetUrl = document.getElementById('spreadsheetUrl').value;
        const configFile = document.getElementById('configFile').files[0];
        const labellingPrompt = document.getElementById('labellingPrompt').value;

        if (!file && !spreadsheetUrl) {
            showError('ファイルまたはスプレッドシートURLを入力してください');
            return;
        }

        // カスタム設定の構築
        let config = {};

        // 既存の設定ファイルの読み込み
        if (configFile) {
            const configText = await configFile.text();
            config = JSON.parse(configText);
        }

        // 各プロンプトの追加（値が存在する場合のみ）
        const takeawaysPrompt = document.getElementById('takeawaysPrompt').value;
        const overviewPrompt = document.getElementById('overviewPrompt').value;

        if (labellingPrompt && labellingPrompt.trim()) {
            config = {
                ...config,
                labelling: {
                    ...config.labelling,
                    prompt: labellingPrompt.trim()
                }
            };
        }

        if (takeawaysPrompt && takeawaysPrompt.trim()) {
            config = {
                ...config,
                takeaways: {
                    ...config.takeaways,
                    prompt: takeawaysPrompt.trim()
                }
            };
        }

        if (overviewPrompt && overviewPrompt.trim()) {
            config = {
                ...config,
                overview: {
                    ...config.overview,
                    prompt: overviewPrompt.trim()
                }
            };
        }

        // ファイルとURLの追加
        if (file) formData.append('file', file);
        if (spreadsheetUrl) formData.append('spreadsheet_url', spreadsheetUrl);

        // 設定をJSONとして追加（必ず実行）
        formData.append('config', new Blob([JSON.stringify(config)], {
            type: 'application/json'
        }));

        try {
            const response = await fetch('/upload', {
                method: 'POST',
                body: formData
            });
            const data = await response.json();

            if (response.ok) {
                checkStatus(data.job_id);
            } else {
                status.className = 'status error';
                statusMessage.textContent = data.error;
                progressInfo.textContent = '';
            }
        } catch (error) {
            showError('エラーが発生しました: ' + error);
        }
    };

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