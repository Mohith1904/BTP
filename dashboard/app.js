// State
let currentInterface = "lifi";
let lastBytes = 0;
let lastTime = Date.now();
let currentStreamName = null;
let hlsPlayer = null;

// DOM refs
const dotIface = document.getElementById("dot-interface");
const lblIface = document.getElementById("label-interface");
const lblConn = document.getElementById("label-connection");
const lblUptime = document.getElementById("label-uptime");
const fileListEl = document.getElementById("file-list");
const transferEl = document.getElementById("transfer-list");
const eventLogEl = document.getElementById("event-log");
const statBytes = document.getElementById("stat-bytes");
const statChunks = document.getElementById("stat-chunks");
const statFails = document.getElementById("stat-failovers");
const statBw = document.getElementById("stat-bandwidth");

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
        ".mp4": "VID", ".mkv": "VID", ".avi": "VID", ".mov": "VID", ".webm": "VID",
        ".mp3": "AUD", ".wav": "AUD", ".flac": "AUD", ".aac": "AUD",
        ".jpg": "IMG", ".jpeg": "IMG", ".png": "IMG", ".gif": "IMG", ".webp": "IMG", ".bmp": "IMG",
        ".pdf": "PDF", ".doc": "DOC", ".docx": "DOC", ".txt": "TXT", ".csv": "CSV",
        ".zip": "ZIP", ".rar": "ZIP", ".7z": "ZIP", ".tar": "ZIP",
        ".py": "PY", ".js": "JS", ".html": "HTML", ".css": "CSS",
        ".exe": "EXE", ".msi": "MSI",
    };
    return map[ext] || "FILE";
}

const VIDEO_EXTS = [".mp4", ".mkv", ".avi", ".mov", ".webm"];
function isVideo(ext) { return VIDEO_EXTS.includes(ext); }

let bannerEl = null;
function flashBanner(iface) {
    if (bannerEl) bannerEl.remove();
    bannerEl = document.createElement("div");
    bannerEl.className = `interface-banner ${iface}`;
    bannerEl.textContent = iface === "lifi" ? "Switched to LiFi" : "Switched to WiFi";
    document.body.appendChild(bannerEl);
    requestAnimationFrame(() => bannerEl.classList.add("show"));
    setTimeout(() => {
        bannerEl.classList.remove("show");
        setTimeout(() => { if (bannerEl) bannerEl.remove(); }, 300);
    }, 2500);
}

function renderFiles(files) {
    if (!files || files.length === 0) {
        fileListEl.innerHTML = '<p class="muted">No files shared yet. Place files in the sender\'s <code>shared/</code> folder.</p>';
        return;
    }

    fileListEl.replaceChildren(...files.map(f => {
        const item = document.createElement("div");
        item.className = "file-item";

        const icon = document.createElement("div");
        icon.className = "file-icon";
        icon.textContent = fileIcon(f.ext);

        const info = document.createElement("div");
        info.className = "file-info";

        const name = document.createElement("div");
        name.className = "file-name";
        name.title = f.name;
        name.textContent = f.name;

        const size = document.createElement("div");
        size.className = "file-size";
        size.textContent = formatBytes(f.size);

        const action = document.createElement("button");
        action.className = "file-action";
        action.textContent = isVideo(f.ext) ? "Stream" : "Download";
        action.addEventListener("click", () => requestFile(f.name, f.ext));

        info.append(name, size);
        item.append(icon, info, action);
        return item;
    }));
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
const videoPlayer = document.getElementById("video-player");
videoPlayer.addEventListener("ended", closeModal);

window.addEventListener("beforeunload", () => {
    if (!currentStreamName) return;
    navigator.sendBeacon(`/api/stream/stop/${encodeURIComponent(currentStreamName)}`);
});

async function requestFile(name, ext) {
    if (isVideo(ext)) {
        startVideoStream(name);
        return;
    }

    try {
        await fetch(`/api/download/${encodeURIComponent(name)}`, { method: "POST" });
    } catch (e) {
        console.error("Request failed:", e);
    }
}

async function startVideoStream(name) {
    openVideoModal(name);
    setVideoStatus("Preparing HLS playlist...");

    try {
        const r = await fetch(`/api/stream/start/${encodeURIComponent(name)}`, { method: "POST" });
        if (!r.ok) throw new Error("Stream start failed");
        const status = await r.json();
        if (!status.playlist_url) throw new Error("Missing playlist URL");
        setVideoStatus("Playlist ready. Starting playback...");
        playHls(status.playlist_url);
    } catch (e) {
        console.error("Stream failed:", e);
        setVideoStatus("Could not start stream.");
    }
}

function openVideoModal(name) {
    const modal = document.getElementById("video-modal");
    const player = document.getElementById("video-player");
    const title = document.getElementById("video-title");
    if (hlsPlayer) {
        hlsPlayer.destroy();
        hlsPlayer = null;
    }
    currentStreamName = name;
    player.pause();
    player.removeAttribute("src");
    player.load();
    title.textContent = name;
    modal.classList.remove("hidden");
}

function setVideoStatus(text) {
    const status = document.getElementById("video-status");
    status.textContent = text;
    status.classList.remove("hidden");
}

function playHls(playlistUrl) {
    const player = document.getElementById("video-player");
    if (window.Hls && window.Hls.isSupported()) {
        hlsPlayer = new Hls({
            lowLatencyMode: false,
            backBufferLength: 30,
        });
        hlsPlayer.loadSource(playlistUrl);
        hlsPlayer.attachMedia(player);
        hlsPlayer.on(Hls.Events.MANIFEST_PARSED, () => {
            player.play().catch(() => {});
            document.getElementById("video-status").classList.add("hidden");
        });
        hlsPlayer.on(Hls.Events.ERROR, (_event, data) => {
            if (data.fatal) setVideoStatus("Playback error. Try reopening the stream.");
        });
        return;
    }

    if (player.canPlayType("application/vnd.apple.mpegurl")) {
        player.src = playlistUrl;
        player.load();
        player.play().catch(() => {});
        document.getElementById("video-status").classList.add("hidden");
        return;
    }

    setVideoStatus("HLS playback is not supported in this browser.");
}

function closeModal() {
    const modal = document.getElementById("video-modal");
    const player = document.getElementById("video-player");
    if (hlsPlayer) {
        hlsPlayer.destroy();
        hlsPlayer = null;
    }
    if (currentStreamName) {
        fetch(`/api/stream/stop/${encodeURIComponent(currentStreamName)}`, { method: "POST" }).catch(() => {});
        currentStreamName = null;
    }
    player.pause();
    player.removeAttribute("src");
    player.load();
    modal.classList.add("hidden");
}

function renderTransfers(transfers) {
    const entries = Object.entries(transfers || {});
    if (entries.length === 0) {
        transferEl.innerHTML = '<p class="muted">No active transfers</p>';
        return;
    }

    transferEl.innerHTML = entries.map(([, t]) => {
        const pct = ((t.progress || 0) * 100).toFixed(1);
        const iface = t.interface || currentInterface;
        return `
            <div class="transfer-item">
                <div class="transfer-header">
                    <span class="transfer-name">${t.filename}</span>
                    <span class="transfer-pct">${pct}%</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill ${iface === "wifi" ? "wifi" : ""}"
                         style="width:${pct}%"></div>
                </div>
                <div class="transfer-meta">
                    <span>${formatBytes(t.bytes || 0)} / ${formatBytes(t.file_size || 0)}</span>
                    <span>${t.received || 0} / ${t.total_chunks || 0} chunks</span>
                    <span>${t.completed ? "Done" : "Transferring"}</span>
                </div>
            </div>
        `;
    }).join("");
}

function renderEvents(events) {
    if (!events || events.length === 0) {
        eventLogEl.innerHTML = '<p class="muted">Waiting for events...</p>';
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

function connectSSE() {
    const source = new EventSource("/api/events");

    source.onmessage = (ev) => {
        try {
            const s = JSON.parse(ev.data);
            updateDashboard(s);
        } catch (e) {
            // Ignore partial or malformed SSE frames.
        }
    };

    source.onerror = () => {
        source.close();
        lblConn.textContent = "Reconnecting...";
        document.querySelector("#pill-connection .pill-dot").className = "pill-dot dot-red";
        setTimeout(connectSSE, 2000);
    };
}

function updateDashboard(s) {
    const iface = s.active_interface || "lifi";
    if (iface !== currentInterface) {
        flashBanner(iface);
        currentInterface = iface;
    }
    dotIface.className = "pill-dot " + (iface === "lifi" ? "dot-lifi" : "dot-wifi");
    lblIface.textContent = iface === "lifi" ? "LiFi" : "WiFi";
    lblConn.textContent = "Connected";
    document.querySelector("#pill-connection .pill-dot").className = "pill-dot dot-green";

    const uptime = Date.now() / 1000 - (s.start_time || Date.now() / 1000);
    lblUptime.textContent = formatTime(uptime);

    statBytes.textContent = formatBytes(s.bytes_received || 0);
    statChunks.textContent = s.chunks_received || 0;
    statFails.textContent = s.failover_count || 0;

    const now = Date.now();
    const dt = (now - lastTime) / 1000;
    if (dt > 0.4) {
        const bw = ((s.bytes_received || 0) - lastBytes) / dt;
        statBw.textContent = bw > 0 ? formatBytes(bw) + "/s" : "-";
        lastBytes = s.bytes_received || 0;
        lastTime = now;
    }

    renderTransfers(s.transfers);
    renderEvents(s.events);
}

(async function init() {
    try {
        const r = await fetch("/api/files");
        const d = await r.json();
        renderFiles(d.files);
    } catch (e) {
        fileListEl.innerHTML = '<p class="muted">Could not load files</p>';
    }

    connectSSE();
})();
