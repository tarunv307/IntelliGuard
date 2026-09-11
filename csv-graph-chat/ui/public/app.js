/* app.js — CSV Graph Chat frontend
   Vanilla JS, no frameworks.
   API is proxied through nginx at /api/ → http://api:8000/
*/

const API = '/api';   // proxied by nginx

// ── State ─────────────────────────────────────────────────────────────────────
let currentFile      = null;
let currentJobId     = null;
let pollInterval     = null;

// ── DOM refs ──────────────────────────────────────────────────────────────────
const dropZone       = document.getElementById('drop-zone');
const fileInput      = document.getElementById('file-input');
const uploadControls = document.getElementById('upload-controls');
const selectedName   = document.getElementById('selected-filename');
const selectedSize   = document.getElementById('selected-size');
const uploadBtn      = document.getElementById('upload-btn');
const clearBtn       = document.getElementById('clear-btn');
const jobProgress    = document.getElementById('job-progress');
const progressBar    = document.getElementById('progress-bar');
const progressPct    = document.getElementById('progress-pct');
const progressTrack  = document.getElementById('progress-bar-track');
const jobIdDisplay   = document.getElementById('job-id-display');
const rowsStat       = document.getElementById('rows-stat');
const jobStatusChip  = document.getElementById('job-status-chip');
const uploadBanner   = document.getElementById('upload-banner');
const previewCont    = document.getElementById('preview-container');
const previewBadge   = document.getElementById('preview-badge');
const chatInput      = document.getElementById('chat-input');
const chatSendBtn    = document.getElementById('chat-send-btn');
const chatMessages   = document.getElementById('chat-messages');
const statusDot      = document.getElementById('status-dot');
const statusText     = document.getElementById('status-text');

// ── Health polling ─────────────────────────────────────────────────────────────
async function checkHealth() {
  try {
    const res  = await fetch(`${API}/health`, { cache: 'no-store' });
    const data = await res.json();
    const ok   = data.status === 'ok';
    statusDot.className  = 'status-dot ' + (ok ? 'ok' : 'warn');
    statusText.textContent = ok
      ? `Online — Kafka ✓  Neo4j ✓`
      : `Degraded — Kafka:${data.kafka_connected ? '✓' : '✗'}  Neo4j:${data.neo4j_connected ? '✓' : '✗'}`;
  } catch {
    statusDot.className  = 'status-dot err';
    statusText.textContent = 'API unreachable';
  }
}
checkHealth();
setInterval(checkHealth, 15_000);

// ── Drag & drop ───────────────────────────────────────────────────────────────
dropZone.addEventListener('dragover', (e) => {
  e.preventDefault();
  dropZone.classList.add('drag-over');
});
dropZone.addEventListener('dragleave', () => dropZone.classList.remove('drag-over'));
dropZone.addEventListener('drop', (e) => {
  e.preventDefault();
  dropZone.classList.remove('drag-over');
  const f = e.dataTransfer.files[0];
  if (f) selectFile(f);
});
dropZone.addEventListener('click', () => fileInput.click());
dropZone.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') fileInput.click(); });
fileInput.addEventListener('change', (e) => {
  if (e.target.files[0]) selectFile(e.target.files[0]);
});

function selectFile(f) {
  currentFile = f;
  selectedName.textContent  = f.name;
  selectedSize.textContent  = formatBytes(f.size);
  uploadControls.style.display = 'flex';
  hideBanner();
  // Parse preview immediately
  parseAndPreview(f);
}

function formatBytes(b) {
  if (b < 1024)       return b + ' B';
  if (b < 1048576)    return (b / 1024).toFixed(1) + ' KB';
  return (b / 1048576).toFixed(1) + ' MB';
}

clearBtn.addEventListener('click', () => {
  currentFile = null;
  fileInput.value = '';
  uploadControls.style.display = 'none';
  jobProgress.style.display    = 'none';
  hideBanner();
  resetPreview();
  stopPoll();
});

// ── CSV preview (client-side) ─────────────────────────────────────────────────
function parseAndPreview(file) {
  const reader = new FileReader();
  reader.onload = (e) => {
    const rows = parseCSV(e.target.result, 20);
    renderPreview(rows, file.name);
  };
  reader.readAsText(file);
}

function parseCSV(text, maxRows = 20) {
  const lines = text.split(/\r?\n/).filter(l => l.trim());
  if (!lines.length) return { headers: [], rows: [] };
  const headers = splitCSVLine(lines[0]);
  const rows = [];
  for (let i = 1; i < Math.min(lines.length, maxRows + 1); i++) {
    rows.push(splitCSVLine(lines[i]));
  }
  return { headers, rows };
}

function splitCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') { inQuotes = !inQuotes; continue; }
    if (ch === ',' && !inQuotes) { result.push(current); current = ''; continue; }
    current += ch;
  }
  result.push(current);
  return result;
}

function renderPreview({ headers, rows }, filename) {
  if (!headers.length) {
    previewCont.innerHTML = '<div class="preview-empty"><div class="empty-icon">⚠️</div><p>Could not parse CSV headers</p></div>';
    return;
  }
  let html = '<table class="preview-table"><thead><tr>';
  headers.forEach(h => { html += `<th title="${esc(h)}">${esc(h)}</th>`; });
  html += '</tr></thead><tbody>';
  rows.forEach(row => {
    html += '<tr>';
    headers.forEach((_, i) => {
      const val = row[i] !== undefined ? row[i] : '';
      html += `<td title="${esc(val)}">${esc(val)}</td>`;
    });
    html += '</tr>';
  });
  html += '</tbody></table>';
  previewCont.innerHTML = html;
  previewBadge.textContent   = `${rows.length} of preview rows`;
  previewBadge.style.display = 'inline-block';
}

function resetPreview() {
  previewCont.innerHTML = '<div class="preview-empty"><div class="empty-icon">📋</div><p>Upload a CSV to preview the first 20 rows</p></div>';
  previewBadge.style.display = 'none';
}

// ── Upload ────────────────────────────────────────────────────────────────────
uploadBtn.addEventListener('click', doUpload);

async function doUpload() {
  if (!currentFile) return;
  uploadBtn.disabled = true;
  uploadBtn.innerHTML = '<span class="btn-icon">⏳</span> Uploading…';
  hideBanner();
  stopPoll();

  const form = new FormData();
  form.append('file', currentFile);

  try {
    const res  = await fetch(`${API}/ingest`, { method: 'POST', body: form });
    const data = await res.json();

    if (!res.ok) {
      showBanner('error', `❌ ${data.detail || 'Upload failed'}`);
      return;
    }

    currentJobId = data.job_id;
    showBanner('success', `✅ Uploaded "${data.filename}" — ${data.rows_received} rows queued (job: ${data.job_id})`);

    // Show progress UI
    jobProgress.style.display = 'block';
    jobIdDisplay.textContent  = data.job_id;
    setProgress(0, data.rows_received, 0, 'queued');

    if (data.rows_received === 0) {
      setProgress(100, 0, 0, 'complete');
    } else {
      startPoll(data.job_id, data.rows_received);
    }

  } catch (err) {
    showBanner('error', `❌ Network error: ${err.message}`);
  } finally {
    uploadBtn.disabled = false;
    uploadBtn.innerHTML = '<span class="btn-icon">🚀</span> Upload &amp; Ingest';
  }
}

// ── Status polling ────────────────────────────────────────────────────────────
function startPoll(jobId, total) {
  stopPoll();
  pollInterval = setInterval(async () => {
    try {
      const res  = await fetch(`${API}/status?job_id=${jobId}`, { cache: 'no-store' });
      if (!res.ok) return;
      const data = await res.json();
      const pct  = total > 0 ? Math.round((data.rows_loaded + data.rows_failed) / total * 100) : 100;
      setProgress(pct, total, data.rows_loaded, data.status);
      if (data.status === 'complete' || data.status === 'failed') {
        stopPoll();
      }
    } catch { /* ignore transient errors */ }
  }, 1000);
}

function stopPoll() {
  if (pollInterval) { clearInterval(pollInterval); pollInterval = null; }
}

function setProgress(pct, total, loaded, status) {
  progressBar.style.width = `${pct}%`;
  progressTrack.setAttribute('aria-valuenow', pct);
  progressPct.textContent = `${pct}%`;
  rowsStat.textContent    = `${loaded} / ${total} rows`;
  jobStatusChip.textContent = status;
  jobStatusChip.className   = `status-chip ${status}`;
}

// ── Chat ──────────────────────────────────────────────────────────────────────
chatSendBtn.addEventListener('click', sendChat);
chatInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendChat(); }
});

function fillQuestion(q) {
  chatInput.value = q;
  chatInput.focus();
}

async function sendChat() {
  const q = chatInput.value.trim();
  if (!q) return;

  appendUserMsg(q);
  chatInput.value = '';
  chatSendBtn.disabled = true;

  const typingId = appendTyping();

  try {
    const res  = await fetch(`${API}/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question: q }),
    });
    const data = await res.json();
    removeTyping(typingId);
    appendBotMsg(data);
  } catch (err) {
    removeTyping(typingId);
    appendBotMsg({
      answer: `Network error: ${err.message}`,
      cypher: null,
      result: [],
      grounded: false,
    });
  } finally {
    chatSendBtn.disabled = false;
    chatInput.focus();
  }
}

function appendUserMsg(text) {
  removeWelcome();
  const el = document.createElement('div');
  el.className = 'msg msg-user';
  el.innerHTML = `<div class="msg-bubble">${esc(text)}</div>`;
  chatMessages.appendChild(el);
  scrollChat();
}

function appendBotMsg({ answer, cypher, result, grounded }) {
  removeWelcome();
  const el = document.createElement('div');
  el.className = 'msg msg-bot';

  const gClass = grounded ? 'true' : 'false';
  const gLabel = grounded ? '✓ Grounded' : '✗ Not grounded';

  let inner = `
    <div class="grounded-badge ${gClass}">${gLabel}</div>
    <div class="msg-answer">${esc(answer)}</div>
  `;

  if (cypher || (result && result.length)) {
    const resultStr = result && result.length
      ? JSON.stringify(result, null, 2)
      : '(no results)';
    inner += `
      <details class="cypher-details">
        <summary>🔍 View Cypher query &amp; raw result</summary>
        <div class="cypher-block">
          <div class="section-label" style="padding:10px 16px 0">Cypher</div>
          <pre>${esc(cypher || '—')}</pre>
        </div>
        <div class="result-block">
          <div class="section-label">Raw Result</div>
          <pre>${esc(resultStr)}</pre>
        </div>
      </details>
    `;
  }

  el.innerHTML = `<div class="msg-bubble">${inner}</div>`;
  chatMessages.appendChild(el);
  scrollChat();
}

function appendTyping() {
  removeWelcome();
  const id = 'typing-' + Date.now();
  const el = document.createElement('div');
  el.className = 'msg msg-bot';
  el.id = id;
  el.innerHTML = `
    <div class="typing-indicator">
      <div class="typing-dot"></div>
      <div class="typing-dot"></div>
      <div class="typing-dot"></div>
    </div>`;
  chatMessages.appendChild(el);
  scrollChat();
  return id;
}

function removeTyping(id) {
  const el = document.getElementById(id);
  if (el) el.remove();
}

function removeWelcome() {
  const welcome = chatMessages.querySelector('.chat-welcome');
  if (welcome) welcome.remove();
}

function scrollChat() {
  chatMessages.scrollTop = chatMessages.scrollHeight;
}

// ── Banners ───────────────────────────────────────────────────────────────────
function showBanner(type, msg) {
  uploadBanner.className   = `upload-banner ${type}`;
  uploadBanner.textContent = msg;
  uploadBanner.style.display = 'block';
}
function hideBanner() { uploadBanner.style.display = 'none'; }

// ── HTML escape ───────────────────────────────────────────────────────────────
function esc(str) {
  if (str === null || str === undefined) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}
