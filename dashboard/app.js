// ── State ──────────────────────────────────────────
let currentInterface = "lifi";
let lastBytes = 0;
let lastTime = Date.now();
let currentHls = null;         // hls.js instance
let currentStreamSessionId = null;

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

function formatDuration(secs) {
    secs = Math.floor(secs);
    const h = Math.floor(secs / 3600);
    const m = Math.floor((secs % 3600) / 60);
    const s = secs % 60;
    if (h > 0) return `${h}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;
    return `${m}:${String(s).padStart(2,'0')}`;
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
    fileListEl.innerHTML = files.map(f => {
        const isVid = isVideo(f.ext);
        return `
        <div class="file-item" data-name="${f.name}" data-ext="${f.ext}">
            <div class="file-icon">${fileIcon(f.ext)}</div>
            <div class="file-info">
                <div class="file-name" title="${f.name}">${f.name}</div>
                <div class="file-size">${formatBytes(f.size)}</div>
            </div>
            <div class="file-actions">
                ${isVid ? `<button class="file-action stream-btn" onclick="startStream('${f.name}')">▶ Stream</button>` : ''}
                <button class="file-action download-btn" onclick="requestFile('${f.name}', '${f.ext}')">⬇ Download</button>
            </div>
        </div>
        `;
    }).join("");
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

// ── Download file ─────────────────────────────────
async function requestFile(name, ext) {
    try {
        await fetch(`/api/download/${encodeURIComponent(name)}`, { method: "POST" });
    } catch (e) {
        console.error("Download request failed:", e);
    }
}

// ── HLS Streaming ─────────────────────────────────
async function startStream(name) {
    const streamBtn = document.querySelector(`.file-item[data-name="${name}"] .stream-btn`);
    if (streamBtn) {
        streamBtn.disabled = true;
        streamBtn.textContent = "⏳ Preparing...";
    }

    try {
        const r = await fetch(`/api/stream_start/${encodeURIComponent(name)}`, { method: "POST" });
        if (!r.ok) {
            alert("Stream failed. Check sender logs.\n\nPossible causes:\n• Video codec not compatible (needs H.264/AAC)\n• ffmpeg not installed on sender\n• File not found");
            return;
        }
        const data = await r.json();
        openStreamModal(data);
    } catch (e) {
        console.error("Stream start failed:", e);
        alert("Failed to start stream: " + e.message);
    } finally {
        if (streamBtn) {
            streamBtn.disabled = false;
            streamBtn.textContent = "▶ Stream";
        }
    }
}

function openStreamModal(streamData) {
    const modal = document.getElementById("video-modal");
    const player = document.getElementById("video-player");
    const title = document.getElementById("video-title");
    const streamInfo = document.getElementById("stream-info");
    const vlcInput = document.getElementById("vlc-url");
    const metaInfo = document.getElementById("stream-meta-info");

    currentStreamSessionId = streamData.session_id;
    const hlsUrl = `/api/hls/${streamData.session_id}/playlist.m3u8`;
    const vlcUrl = streamData.vlc_url || `http://localhost:${location.port}/api/hls/${streamData.session_id}/playlist.m3u8`;

    title.textContent = streamData.filename;
    vlcInput.value = vlcUrl;
    streamInfo.classList.remove("hidden");

    // Show metadata
    const res = streamData.width && streamData.height ? `${streamData.width}×${streamData.height}` : "—";
    const dur = streamData.duration ? formatDuration(streamData.duration) : "—";
    const segs = streamData.segment_count || "—";
    metaInfo.innerHTML = `
        <span>📐 ${res}</span>
        <span>⏱ ${dur}</span>
        <span>🧩 ${segs} segments</span>
    `;

    // Initialize HLS player
    if (Hls.isSupported()) {
        if (currentHls) {
            currentHls.destroy();
        }
        currentHls = new Hls({
            maxBufferLength: 30,
            maxMaxBufferLength: 60,
            maxBufferSize: 60 * 1024 * 1024,
            enableWorker: true,
        });
        currentHls.loadSource(hlsUrl);
        currentHls.attachMedia(player);
        currentHls.on(Hls.Events.MANIFEST_PARSED, () => {
            player.play().catch(() => {});
        });
        currentHls.on(Hls.Events.ERROR, (event, data) => {
            console.warn("HLS error:", data.type, data.details);
            if (data.fatal) {
                switch (data.type) {
                    case Hls.ErrorTypes.NETWORK_ERROR:
                        console.log("Network error, retrying...");
                        currentHls.startLoad();
                        break;
                    case Hls.ErrorTypes.MEDIA_ERROR:
                        console.log("Media error, recovering...");
                        currentHls.recoverMediaError();
                        break;
                    default:
                        console.error("Fatal HLS error, destroying...");
                        currentHls.destroy();
                        break;
                }
            }
        });
    } else if (player.canPlayType('application/vnd.apple.mpegurl')) {
        // Safari native HLS
        player.src = hlsUrl;
        player.addEventListener('loadedmetadata', () => player.play());
    } else {
        alert("HLS playback is not supported in this browser. Use VLC instead:\n\n" + vlcUrl);
    }

    modal.classList.remove("hidden");
}

function closeModal() {
    const modal = document.getElementById("video-modal");
    const player = document.getElementById("video-player");
    const streamInfo = document.getElementById("stream-info");

    player.pause();
    player.src = "";

    if (currentHls) {
        currentHls.destroy();
        currentHls = null;
    }

    // Close the streaming session on the server
    if (currentStreamSessionId) {
        fetch(`/api/stream_close/${currentStreamSessionId}`, { method: "POST" }).catch(() => {});
        currentStreamSessionId = null;
    }

    streamInfo.classList.add("hidden");
    modal.classList.add("hidden");
}

function copyVlcUrl() {
    const vlcInput = document.getElementById("vlc-url");
    vlcInput.select();
    navigator.clipboard.writeText(vlcInput.value).then(() => {
        const btn = document.getElementById("btn-copy-vlc");
        const orig = btn.textContent;
        btn.textContent = "✓ Copied!";
        setTimeout(() => { btn.textContent = orig; }, 1500);
    }).catch(() => {
        // Fallback for older browsers
        document.execCommand("copy");
    });
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
        else if (e.msg.includes("Stream")) cls = "stream";
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
