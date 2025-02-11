async function checkStatus(jobId) {
    const response = await fetch(`/status/${jobId}`);
    const data = await response.json();
    const status = document.getElementById('status');
    const statusMessage = document.getElementById('status-message');
    const progressInfo = document.getElementById('progress-info');
    const submitButton = document.querySelector('button[type="submit"]');

    status.style.display = 'block';

    if (data.status === 'completed') {
        const projectId = jobId.replace('job_', 'project_');
        status.className = 'status success';
        statusMessage.innerHTML = `処理が完了しました<br>
            <a href="/pipeline/outputs/${projectId}/report/">レポートを表示</a>`;
        progressInfo.textContent = '';
        submitButton.disabled = false;
        submitButton.textContent = 'アップロード';
    } else if (data.status === 'failed') {
        status.className = 'status error';
        statusMessage.textContent = `エラー: ${data.error}`;
        progressInfo.textContent = '';
        submitButton.disabled = false;
        submitButton.textContent = 'アップロード';
    } else {
        status.className = 'status';
        statusMessage.textContent = '処理中...';
        submitButton.disabled = true;
        submitButton.textContent = '処理中...';
        
        if (data.current_step) {
            let progressText = `現在の処理: ${data.current_step}`;
            if (data.progress && data.progress.total > 0) {
                const percent = Math.round((data.progress.current / data.progress.total) * 100);
                progressText += ` (${percent}% 完了)`;
            }
            progressInfo.textContent = progressText;
        }
        
        setTimeout(() => checkStatus(jobId), 2000);
    }
}

document.addEventListener('DOMContentLoaded', () => {
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

        if (!file && !spreadsheetUrl) {
            status.className = 'status error';
            status.style.display = 'block';
            statusMessage.textContent = 'ファイルまたはスプレッドシートURLを入力してください';
            return;
        }

        if (file) formData.append('file', file);
        if (spreadsheetUrl) formData.append('spreadsheet_url', spreadsheetUrl);
        if (configFile) formData.append('config', configFile);

        try {
            status.className = 'status';
            statusMessage.textContent = '処理中...';
            progressInfo.textContent = '';
            status.style.display = 'block';

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
            status.className = 'status error';
            statusMessage.textContent = 'エラーが発生しました';
            progressInfo.textContent = '';
            submitButton.disabled = false;
            submitButton.textContent = 'アップロード';
        }
    };
});