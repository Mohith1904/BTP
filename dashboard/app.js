// ── State ──────────────────────────────────────────
let currentInterface = "lifi";
let lastBytes = 0;
let lastTime = Date.now();

// ── DOM refs ──────────────────────────────────────
const dotIface    = document.getElementById("dot-interface");
const lblIface    = document.getElementById("label-interface");
const lblConn     = document.getElementById("label-connection");
const lblUptime   = document.getElementById("label-uptime");
const fileListEl  = document.getElementById("file-list");
const transferEl  = document.getElementById("transfer-list");
const eventLogEl  = document.getElementById("event-log");
const statBytes   = document.getElementById("stat-bytes");
const statChunks  = document.getElementById("stat-chunks");
const statFails   = document.getElementById("stat-failovers");
const statBw      = document.getElementById("stat-bandwidth");

// ── Helpers ───────────────────────────────────────
function formatBytes(b) {
    if (b < 1024) return b + " B";
    if (b < 1048576) return (b / 1024).toFixed(1) + " KB";
    if (b < 1073741824) return (b / 1048576).toFixed(1) + " MB";
    return (b / 1073741824).toFixed(2) + " GB";
}

function formatTime(secs) {
    secs = Math.floor(secs);
    if (secs < 60) return secs + "s";
    const m = Math.floor(secs / 60);
    const s = secs % 60;
    if (m < 60) return m + "m " + s + "s";
    const h = Math.floor(m / 60);
    return h + "h " + (m % 60) + "m";
}

function fileIcon(ext) {
    const map = {
        ".mp4":"🎬", ".mkv":"🎬", ".avi":"🎬", ".mov":"🎬", ".webm":"🎬",
        ".mp3":"🎵", ".wav":"🎵", ".flac":"🎵", ".aac":"🎵",
        ".jpg":"🖼️", ".jpeg":"🖼️", ".png":"🖼️", ".gif":"🖼️", ".webp":"🖼️", ".bmp":"🖼️",
        ".pdf":"📄", ".doc":"📄", ".docx":"📄", ".txt":"📃", ".csv":"📊",
        ".zip":"📦", ".rar":"📦", ".7z":"📦", ".tar":"📦",
        ".py":"🐍", ".js":"💛", ".html":"🌐", ".css":"🎨",
        ".exe":"⚙️", ".msi":"⚙️",
    };
    return map[ext] || "📁";
}

const VIDEO_EXTS = [".mp4",".mkv",".avi",".mov",".webm"];
function isVideo(ext) { return VIDEO_EXTS.includes(ext); }

// ── Interface banner flash ────────────────────────
let bannerEl = null;
function flashBanner(iface) {
    if (bannerEl) bannerEl.remove();
    bannerEl = document.createElement("div");
    bannerEl.className = `interface-banner ${iface}`;
    bannerEl.textContent = iface === "lifi"
        ? "⚡ Switched to LiFi"
        : "📡 Switched to WiFi";
    document.body.appendChild(bannerEl);
    requestAnimationFrame(() => bannerEl.classList.add("show"));
    setTimeout(() => {
        bannerEl.classList.remove("show");
        setTimeout(() => { if (bannerEl) bannerEl.remove(); }, 300);
    }, 2500);
}

// ── File list ─────────────────────────────────────
function renderFiles(files) {
    if (!files || files.length === 0) {
        fileListEl.innerHTML = '<p class="muted">No files shared yet. Place files in the sender\'s <code>shared/</code> folder.</p>';
        return;
    }
    fileListEl.innerHTML = files.map(f => `
        <div class="file-item" data-name="${f.name}" data-ext="${f.ext}">
            <div class="file-icon">${fileIcon(f.ext)}</div>
            <div class="file-info">
                <div class="file-name" title="${f.name}">${f.name}</div>
                <div class="file-size">${formatBytes(f.size)}</div>
            </div>
            <button class="file-action" onclick="requestFile('${f.name}', '${f.ext}')">
                ${isVideo(f.ext) ? '▶ Stream' : '⬇ Download'}
            </button>
        </div>
    `).join("");
}

async function refreshFiles() {
    try {
        const r = await fetch("/api/refresh_files");
        const d = await r.json();
        renderFiles(d.files);
    } catch (e) {
        console.error("Refresh failed:", e);
    }
}
document.getElementById("btn-refresh").addEventListener("click", refreshFiles);

// ── Request file / stream video ───────────────────
async function requestFile(name, ext) {
    try {
        await fetch(`/api/download/${encodeURIComponent(name)}`, { method: "POST" });
        if (isVideo(ext)) {
            // Wait a moment for chunks to start arriving, then open video player
            setTimeout(() => openVideoModal(name), 1500);
        }
    } catch (e) {
        console.error("Request failed:", e);
    }
}

function openVideoModal(name) {
    const modal = document.getElementById("video-modal");
    const player = document.getElementById("video-player");
    const title = document.getElementById("video-title");
    player.src = `/api/stream/${encodeURIComponent(name)}`;
    title.textContent = name;
    modal.classList.remove("hidden");
}
function closeModal() {
    const modal = document.getElementById("video-modal");
    const player = document.getElementById("video-player");
    player.pause();
    player.src = "";
    modal.classList.add("hidden");
}

// ── Transfers ─────────────────────────────────────
function renderTransfers(transfers) {
    const entries = Object.entries(transfers || {});
    if (entries.length === 0) {
        transferEl.innerHTML = '<p class="muted">No active transfers</p>';
        return;
    }
    transferEl.innerHTML = entries.map(([sid, t]) => {
        const pct = (t.progress * 100).toFixed(1);
        const iface = currentInterface;
        return `
            <div class="transfer-item">
                <div class="transfer-header">
                    <span class="transfer-name">${t.filename}</span>
                    <span class="transfer-pct">${pct}%</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill ${iface === 'wifi' ? 'wifi' : ''}"
                         style="width:${pct}%"></div>
                </div>
                <div class="transfer-meta">
                    <span>${formatBytes(t.bytes || 0)} / ${formatBytes(t.file_size || 0)}</span>
                    <span>${t.received || 0} / ${t.total_chunks || 0} chunks</span>
                    <span>${t.completed ? '✓ Done' : '⟳ Transferring'}</span>
                </div>
            </div>
        `;
    }).join("");
}

// ── Events ────────────────────────────────────────
function renderEvents(events) {
    if (!events || events.length === 0) {
        eventLogEl.innerHTML = '<p class="muted">Waiting for events…</p>';
        return;
    }
    eventLogEl.innerHTML = events.slice().reverse().map(e => {
        let cls = "";
        if (e.msg.includes("Failover")) cls = "failover";
        else if (e.msg.includes("Switch back")) cls = "switchback";
        else if (e.msg.includes("Complete") || e.msg.includes("VERIFIED")) cls = "complete";
        return `
            <div class="event">
                <span class="event-time">${e.time}</span>
                <span class="event-msg ${cls}">${e.msg}</span>
            </div>
        `;
    }).join("");
}

// ── SSE: real-time updates ────────────────────────
function connectSSE() {
    const source = new EventSource("/api/events");

    source.onmessage = (ev) => {
        try {
            const s = JSON.parse(ev.data);
            updateDashboard(s);
        } catch (e) { /* ignore parse errors */ }
    };

    source.onerror = () => {
        source.close();
        lblConn.textContent = "Reconnecting…";
        document.querySelector("#pill-connection .pill-dot").className = "pill-dot dot-red";
        setTimeout(connectSSE, 2000);
    };
}

function updateDashboard(s) {
    // Interface
    const iface = s.active_interface || "lifi";
    if (iface !== currentInterface) {
        flashBanner(iface);
        currentInterface = iface;
    }
    dotIface.className = "pill-dot " + (iface === "lifi" ? "dot-lifi" : "dot-wifi");
    lblIface.textContent = iface === "lifi" ? "LiFi" : "WiFi";
    lblConn.textContent = "Connected";
    document.querySelector("#pill-connection .pill-dot").className = "pill-dot dot-green";

    // Uptime
    const uptime = Date.now() / 1000 - (s.start_time || Date.now() / 1000);
    lblUptime.textContent = formatTime(uptime);

    // Stats
    statBytes.textContent = formatBytes(s.bytes_received || 0);
    statChunks.textContent = s.chunks_received || 0;
    statFails.textContent = s.failover_count || 0;

    // Bandwidth
    const now = Date.now();
    const dt = (now - lastTime) / 1000;
    if (dt > 0.4) {
        const bw = ((s.bytes_received || 0) - lastBytes) / dt;
        statBw.textContent = bw > 0 ? formatBytes(bw) + "/s" : "—";
        lastBytes = s.bytes_received || 0;
        lastTime = now;
    }

    // Transfers
    renderTransfers(s.transfers);

    // Events
    renderEvents(s.events);
}

// ── Init ──────────────────────────────────────────
(async function init() {
    // Load initial file list
    try {
        const r = await fetch("/api/files");
        const d = await r.json();
        renderFiles(d.files);
    } catch (e) {
        fileListEl.innerHTML = '<p class="muted">Could not load files</p>';
    }

    // Start SSE
    connectSSE();
})();
