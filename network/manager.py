"""
Dual-interface network manager.

Manages two UDP sockets (data + control) across two network interfaces
(LiFi over Ethernet, WiFi). Routes packets to the correct interface
and tracks which interface is active.
"""

import socket
import threading
import time
import logging

import psutil

from protocol.packet import Packet, PType

log = logging.getLogger("network")


class NetworkManager:
    """Manages UDP sockets and interface switching."""

    def __init__(
        self,
        my_lifi_ip: str,
        my_wifi_ip: str,
        peer_lifi_ip: str,
        peer_wifi_ip: str,
        data_port: int,
        control_port: int,
        recv_buffer: int = 65536,
    ):
        self.my_lifi_ip = my_lifi_ip
        self.my_wifi_ip = my_wifi_ip
        self.peer_lifi_ip = peer_lifi_ip
        self.peer_wifi_ip = peer_wifi_ip
        self.data_port = data_port
        self.control_port = control_port
        self.recv_buffer = recv_buffer

        self.active_interface = self._detect_initial_interface()
        self._lock = threading.Lock()

        # ── sockets ─────────────────────────────────────────
        self.data_sock = self._make_socket(data_port)
        self.ctrl_sock = self._make_socket(control_port)

        # ── receive handlers  {packet_type: callback(packet, addr, iface)} ──
        self._data_handlers: dict = {}
        self._ctrl_handlers: dict = {}

        # ── threads ─────────────────────────────────────────
        self.running = False
        self._threads: list[threading.Thread] = []

    # ── socket helpers ──────────────────────────────────────
    @staticmethod
    def _make_socket(port: int) -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Increase OS receive buffer for high throughput
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        except OSError:
            pass
        s.bind(("0.0.0.0", port))
        s.settimeout(0.05)
        return s

    # ── interface logic ─────────────────────────────────────
    def peer_addr(self, interface: str | None = None) -> str:
        iface = interface or self.active_interface
        return self.peer_lifi_ip if iface == "lifi" else self.peer_wifi_ip

    def interface_available(self, interface: str) -> bool:
        """Return True if the local IP for an interface is present and up."""
        target_ip = self.my_lifi_ip if interface == "lifi" else self.my_wifi_ip
        try:
            addrs = psutil.net_if_addrs()
            stats = psutil.net_if_stats()
            for name, addr_list in addrs.items():
                stat = stats.get(name)
                for a in addr_list:
                    if a.family == socket.AF_INET and a.address == target_ip:
                        return bool(stat.isup) if stat else True
        except Exception:
            pass
        return False

    def _detect_initial_interface(self) -> str:
        """Prefer LiFi when present, otherwise start on WiFi if available."""
        if self.interface_available("lifi"):
            return "lifi"
        if self.interface_available("wifi"):
            return "wifi"
        return "lifi"

    def switch_to(self, interface: str):
        with self._lock:
            old = self.active_interface
            self.active_interface = interface
            log.warning("Interface switch: %s → %s", old, interface)

    # ── send ────────────────────────────────────────────────
    def send_data(self, packet: Packet, interface: str | None = None):
        dest = (self.peer_addr(interface), self.data_port)
        try:
            self.data_sock.sendto(packet.pack(), dest)
        except OSError as e:
            log.error("send_data error to %s: %s", dest, e)

    def send_ctrl(self, packet: Packet, interface: str | None = None):
        dest = (self.peer_addr(interface), self.control_port)
        try:
            self.ctrl_sock.sendto(packet.pack(), dest)
        except OSError as e:
            log.error("send_ctrl error to %s: %s", dest, e)

    # ── register handlers ───────────────────────────────────
    def on_data(self, ptype: int, handler):
        self._data_handlers[ptype] = handler

    def on_ctrl(self, ptype: int, handler):
        self._ctrl_handlers[ptype] = handler

    # ── receive loops ───────────────────────────────────────
    def _recv_loop(self, sock: socket.socket, handlers: dict, label: str):
        while self.running:
            try:
                raw, addr = sock.recvfrom(self.recv_buffer)
            except socket.timeout:
                continue
            except OSError:
                if self.running:
                    log.error("%s socket error", label)
                break

            try:
                pkt = Packet.unpack(raw)
            except ValueError as e:
                log.debug("%s bad packet from %s: %s", label, addr, e)
                continue

            iface = "lifi" if addr[0] == self.peer_lifi_ip else "wifi"
            handler = handlers.get(pkt.packet_type)
            if handler:
                try:
                    handler(pkt, addr, iface)
                except Exception:
                    log.exception("Handler error for %s", PType.name(pkt.packet_type))
            else:
                log.debug("%s unhandled %s from %s", label, PType.name(pkt.packet_type), addr)

    # ── lifecycle ───────────────────────────────────────────
    def start(self):
        self.running = True
        t1 = threading.Thread(
            target=self._recv_loop,
            args=(self.data_sock, self._data_handlers, "DATA"),
            daemon=True,
        )
        t2 = threading.Thread(
            target=self._recv_loop,
            args=(self.ctrl_sock, self._ctrl_handlers, "CTRL"),
            daemon=True,
        )
        t1.start()
        t2.start()
        self._threads = [t1, t2]
        log.info("NetworkManager started (data=%d, ctrl=%d, active=%s)",
                 self.data_port, self.control_port, self.active_interface)

    def stop(self):
        self.running = False
        for t in self._threads:
            t.join(timeout=2)
        self.data_sock.close()
        self.ctrl_sock.close()
        log.info("NetworkManager stopped")

    # ── utility ─────────────────────────────────────────────
    @staticmethod
    def check_ethernet_link() -> bool:
        """Check if any Ethernet interface has link UP (OS-level)."""
        try:
            stats = psutil.net_if_stats()
            for name, s in stats.items():
                lower = name.lower()
                if ("ethernet" in lower or "eth" in lower) and s.isup:
                    return True
        except Exception:
            pass
        return False

    @staticmethod
    def get_interface_info() -> dict:
        """Return dict of interface_name -> {ip, is_up, speed}."""
        info = {}
        addrs = psutil.net_if_addrs()
        stats = psutil.net_if_stats()
        for name, addr_list in addrs.items():
            for a in addr_list:
                if a.family == socket.AF_INET:
                    s = stats.get(name)
                    info[name] = {
                        "ip": a.address,
                        "is_up": s.isup if s else False,
                        "speed": s.speed if s else 0,
                    }
        return info
