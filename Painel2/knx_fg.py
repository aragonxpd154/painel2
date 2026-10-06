"""
KNX - Alarme de Incendio / Intrusao / Presenca da Fonte Grande
==============================================================
Cliente KNXnet/IP (tunelamento, UDP 3671) feito so com a biblioteca padrao.
Conecta na IP Interface MDRC (IPS/S3.1.1, 192.168.205.161), escuta TODOS os
telegramas de grupo do barramento e guarda o ultimo valor de cada endereco
de grupo cadastrado em knx_fg.json (gerado pelo importar_knx.py).

O painel SO LE o barramento. Nada e escrito no sistema de alarme, com uma
unica excecao manual: o botao "Solicitar status" da tela manda o comando
"Request Status" (1) para as centrais MT/S - so quando alguem clica.

Ao conectar, pede a leitura (GroupValueRead) dos objetos que aceitam
leitura (flag R no ETS), para a tela ja abrir com o estado atual.
"""
import json
import os
import socket
import struct
import threading
import time

KNX_PORT = 3671
HEARTBEAT_S = 55            # CONNECTIONSTATE_REQUEST (o padrao manda < 60 s)
RECONNECT_WAIT_S = 10
CONFIRM_S = 20           # estado novo precisa ficar 20 s para virar alerta (pulso/teste nao avisa)
BATCH_S = 10             # alertas que chegam juntos viram UMA mensagem

# servicos KNXnet/IP
CONNECT_REQ, CONNECT_RES = 0x0205, 0x0206
CONNSTATE_REQ, CONNSTATE_RES = 0x0207, 0x0208
DISCONNECT_REQ, DISCONNECT_RES = 0x0209, 0x020A
TUNNEL_REQ, TUNNEL_ACK = 0x0420, 0x0421

CONNECT_ERRORS = {
    0x22: "tipo de conexao nao suportado",
    0x23: "opcao de conexao nao suportada",
    0x24: "interface sem vaga de tunel (NETx/ETS usando todas as conexoes)",
    0x29: "camada KNX nao suportada",
}

TIPOS_ALARME = {"incendio": "INCÊNDIO", "intrusao_alarme": "INTRUSÃO", "alimentacao": "ALIMENTAÇÃO 12 V",
                "alarme_geral": "ALARME GERAL", "disjuntor": "DISJUNTOR", "intrusao_sensor": "SENSOR DE INTRUSÃO",
                "presenca": "PRESENÇA", "intrusao_armado": "ALARME ARMADO", "memoria": "MEMÓRIA DE ALARME",
                "outro": "KNX"}


def ga_to_int(ga):
    m, mid, sub = (int(x) for x in ga.split("/"))
    return ((m & 0x1F) << 11) | ((mid & 0x07) << 8) | (sub & 0xFF)


def int_to_ga(v):
    return f"{(v >> 11) & 0x1F}/{(v >> 8) & 0x07}/{v & 0xFF}"


def int_to_ia(v):
    return f"{(v >> 12) & 0x0F}.{(v >> 8) & 0x0F}.{v & 0xFF}"


def decode_dpt(dpt, small, data):
    """small = 6 bits do APCI (DPT 1.x); data = bytes seguintes (DPT 9.x etc.)."""
    dpt = str(dpt or "1.001")
    if dpt.startswith("1.") or not data:
        return small & 0x01 if dpt.startswith("1.") else small
    if dpt.startswith("9.") and len(data) >= 2:
        b0, b1 = data[0], data[1]
        m = ((b0 & 0x07) << 8) | b1
        if b0 & 0x80:
            m -= 2048
        e = (b0 >> 3) & 0x0F
        return round(0.01 * m * (2 ** e), 2)
    if dpt.startswith("5.") and data:
        return data[0]
    return int.from_bytes(data, "big")


class KnxTunnel:
    def __init__(self, gateway, port=KNX_PORT, on_value=None, log=None):
        self.gateway = gateway
        self.port = int(port or KNX_PORT)
        self.on_value = on_value or (lambda ga, value, src, kind: None)
        self.log = log or (lambda *a, **k: None)
        self.sock = None
        self.channel = None
        self.individual = None
        self.seq_out = 0
        self.connected = False
        self.error = None
        self.connected_since = None
        self.last_rx = None
        self.rx_count = 0
        self._send_lock = threading.Lock()
        self._ack_event = threading.Event()
        self._ack_seq = None
        self._connstate_ok = None
        self._stop = False
        self._down_since = None
        self._logged_up = False
        self._logged_down = False

    # ---------------- quadros ----------------
    @staticmethod
    def _frame(service, body):
        return struct.pack(">BBHH", 0x06, 0x10, service, 6 + len(body)) + body

    @staticmethod
    def _hpai_nat():
        return bytes([0x08, 0x01, 0, 0, 0, 0, 0, 0])   # 0.0.0.0:0 -> modo NAT (responde pra quem mandou)

    def _send_raw(self, data):
        self.sock.sendto(data, (self.gateway, self.port))

    # ---------------- conexao ----------------
    def _connect(self):
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("", 0))
        self.sock.settimeout(5)
        cri = bytes([0x04, 0x04, 0x02, 0x00])          # tunel, camada de enlace
        self._send_raw(self._frame(CONNECT_REQ, self._hpai_nat() + self._hpai_nat() + cri))
        data, _ = self.sock.recvfrom(1024)
        service = struct.unpack(">H", data[2:4])[0]
        if service != CONNECT_RES:
            raise ConnectionError("resposta inesperada da interface (0x%04x)" % service)
        channel, status = data[6], data[7]
        if status != 0:
            raise ConnectionError(CONNECT_ERRORS.get(status, "erro 0x%02x ao conectar" % status))
        self.channel = channel
        # CRD no fim: 04 04 <endereco individual>
        if len(data) >= 20:
            self.individual = int_to_ia(struct.unpack(">H", data[18:20])[0])
        self.seq_out = 0
        self.connected = True
        self.error = None
        self.connected_since = time.time()
        self._down_since = None
        self.sock.settimeout(1)
        if not getattr(self, "_logged_up", False):
            self.log("KNX CONECTADO", f"{self.gateway} canal {channel}", self.gateway,
                     detail=f"endereco do tunel {self.individual or '?'}")
            self._logged_up = True
            self._logged_down = False

    def _disconnect(self):
        if self.sock and self.channel is not None:
            try:
                body = bytes([self.channel, 0]) + self._hpai_nat()
                self._send_raw(self._frame(DISCONNECT_REQ, body))
            except OSError:
                pass
        self.connected = False

    # ---------------- envio de cEMI ----------------
    def send_cemi(self, cemi, retries=1):
        if not self.connected:
            raise ConnectionError("tunel KNX desconectado")
        with self._send_lock:
            for _ in range(retries + 1):
                seq = self.seq_out
                body = bytes([0x04, self.channel, seq, 0x00]) + cemi
                self._ack_event.clear()
                self._ack_seq = seq
                self._send_raw(self._frame(TUNNEL_REQ, body))
                if self._ack_event.wait(1.5):
                    self.seq_out = (seq + 1) & 0xFF
                    return True
            raise TimeoutError("interface KNX nao confirmou o envio")

    def group_read(self, ga):
        dst = ga_to_int(ga)
        cemi = bytes([0x11, 0x00, 0xBC, 0xE0, 0x00, 0x00]) + struct.pack(">H", dst) + bytes([0x01, 0x00, 0x00])
        return self.send_cemi(cemi)

    def group_write_bit(self, ga, value):
        dst = ga_to_int(ga)
        cemi = bytes([0x11, 0x00, 0xBC, 0xE0, 0x00, 0x00]) + struct.pack(">H", dst) + \
            bytes([0x01, 0x00, 0x80 | (1 if value else 0)])
        return self.send_cemi(cemi)

    # ---------------- recepcao ----------------
    def _handle(self, data):
        if len(data) < 6 or data[0] != 0x06:
            return
        service = struct.unpack(">H", data[2:4])[0]
        if service == TUNNEL_REQ:
            channel, seq = data[7], data[8]
            # confirma SEMPRE (senao a interface derruba o tunel)
            self._send_raw(self._frame(TUNNEL_ACK, bytes([0x04, channel, seq, 0x00])))
            self._parse_cemi(data[10:])
        elif service == TUNNEL_ACK:
            if data[8] == self._ack_seq:
                self._ack_event.set()
        elif service == CONNSTATE_RES:
            self._connstate_ok = (data[7] == 0)
        elif service == DISCONNECT_REQ:
            try:
                self._send_raw(self._frame(DISCONNECT_RES, bytes([data[6], 0])))
            except OSError:
                pass
            self.connected = False
            self.error = "a interface encerrou o tunel"

    def _parse_cemi(self, c):
        if len(c) < 2:
            return
        msg_code = c[0]
        if msg_code not in (0x29, 0x2E):        # L_Data.ind / L_Data.con
            return
        i = 2 + c[1]
        if len(c) < i + 8:
            return
        ctrl2 = c[i + 1]
        src = struct.unpack(">H", c[i + 2:i + 4])[0]
        dst = struct.unpack(">H", c[i + 4:i + 6])[0]
        npdu_len = c[i + 6]
        tpdu = c[i + 7:i + 8 + npdu_len]
        if not (ctrl2 & 0x80) or len(tpdu) < 2:     # so endereco de grupo
            return
        apci = ((tpdu[0] & 0x03) << 8) | (tpdu[1] & 0xC0)
        if apci not in (0x40, 0x80):                # GroupValueResponse / GroupValueWrite
            return
        self.last_rx = time.time()
        self.rx_count += 1
        kind = "resposta" if apci == 0x40 else "escrita"
        self.on_value(int_to_ga(dst), (tpdu[1] & 0x3F, bytes(tpdu[2:])), int_to_ia(src), kind)

    # ---------------- laco principal ----------------
    def run(self, after_connect=None):
        while not self._stop:
            try:
                self._connect()
                if after_connect:
                    threading.Thread(target=after_connect, daemon=True).start()
                next_hb = time.time() + HEARTBEAT_S
                while self.connected and not self._stop:
                    try:
                        data, _ = self.sock.recvfrom(1024)
                        self._handle(data)
                    except socket.timeout:
                        pass
                    if time.time() >= next_hb:
                        ok = False
                        for _ in range(3):
                            self._connstate_ok = None
                            self._send_raw(self._frame(CONNSTATE_REQ, bytes([self.channel, 0]) + self._hpai_nat()))
                            t_end = time.time() + 10
                            while time.time() < t_end and self._connstate_ok is None:
                                try:
                                    data, _ = self.sock.recvfrom(1024)
                                    self._handle(data)
                                except socket.timeout:
                                    pass
                            if self._connstate_ok:
                                ok = True
                                break
                        if not ok:
                            self.error = "interface KNX parou de responder"
                            self.connected = False
                        next_hb = time.time() + HEARTBEAT_S
            except Exception as e:
                self.connected = False
                self.error = str(e) or e.__class__.__name__
            if not self._stop:
                # reconexao rapida (ex: interface reiniciou) nao vira log; so
                # registra se ficar fora por mais de 5 min
                if self._down_since is None:
                    self._down_since = time.time()
                if not getattr(self, "_logged_down", False) and time.time() - self._down_since > 300:
                    self.log("KNX DESCONECTADO", self.gateway, self.gateway, detail=self.error or "")
                    self._logged_down = True
                    self._logged_up = False
                time.sleep(RECONNECT_WAIT_S)
        self._disconnect()


class KnxFireMonitor:
    """Liga o tunel KNX aos pontos cadastrados e aos alertas do Telegram."""

    def __init__(self, base_dir, notify=None, log=None):
        self.path = os.path.join(base_dir, "knx_fg.json")
        self.notify = notify or (lambda text: None)
        self.log = log or (lambda *a, **k: None)
        self.lock = threading.Lock()
        self._last_reset = {}
        self.cfg = self._load()
        self._migrar()
        self._guardar_nome_original()
        self.state = {}       # ga -> {"value", "ts", "src", "kind"}
        self.tunnel = None
        self.thread = None
        self.notified = {}    # ga -> ultimo estado avisado (True = alarme)
        self.pending = {}     # ga -> (alarme?, desde)
        self.batch = []       # (linha, alarme?, tipo)
        self.batch_since = None

    def _load(self):
        try:
            with open(self.path, encoding="utf-8") as f:
                cfg = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"gateway": "192.168.205.161", "port": KNX_PORT, "enabled": False, "points": []}
        if int(cfg.get("versao", 1)) < 2:
            for p in cfg.get("points", []):
                if p.get("editado"):
                    continue
                # "Status da Alimentacao Auxiliar 12V" (MT/S): 1 = em operacao, 0 = falha
                if p.get("tipo") == "alimentacao":
                    p["alarme_valor"] = 0
                # "Zona D - OFF / Zona H - ON Alarme Geral" e estado do sistema, nao alarme
                if p.get("tipo") == "alarme_geral":
                    p["alerta_telegram"] = False
            cfg["versao"] = 2
            try:
                tmp = self.path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(cfg, f, ensure_ascii=False, indent=2)
                os.replace(tmp, self.path)
            except OSError:
                pass
        return cfg

    def _guardar_nome_original(self):
        """O nome original (do NETx/ETS) fica guardado a parte: renomear um
        ponto na tela nao desfaz a ligacao detector de presenca <-> temperatura."""
        mudou = False
        for p in self.cfg.get("points", []):
            if not p.get("nome_original"):
                p["nome_original"] = p.get("nome")
                mudou = True
        if mudou:
            try:
                self._save()
            except OSError:
                pass

    def _migrar(self):
        """"Status da Alimentacao Auxiliar 12V" e o objeto "em operacao" da
        MT/S: 1 = alimentacao OK. Estava como 1 = falha e gerava alerta falso
        de 12 V - corrige uma vez (so os pontos que ainda estao no padrao)."""
        if self.cfg.get("migr_alim_12v"):
            return
        for p in self.cfg.get("points", []):
            if p.get("tipo") == "alimentacao" and p.get("alarme_valor", 1) == 1:
                p["alarme_valor"] = 0
        self.cfg["migr_alim_12v"] = True
        try:
            self._save()
        except OSError:
            pass

    def _save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.cfg, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    def points_by_ga(self):
        return {p["ga"]: p for p in self.cfg.get("points", [])}

    # --------------- valores recebidos ---------------
    def _on_value(self, ga, payload, src, kind):
        point = self.points_by_ga().get(ga)
        if not point:
            return
        small, data = payload
        value = decode_dpt(point.get("dpt"), small, data)
        with self.lock:
            prev = self.state.get(ga)
            prev_value = prev["value"] if prev else None
            changed = prev is None or prev_value != value
            self.state[ga] = {"value": value, "ts": time.time(),
                              "since": time.time() if changed else prev.get("since", time.time()),
                              "src": src, "kind": kind}
        if not point.get("alerta_telegram"):
            return
        alarm_now = self.is_alarm(point, value)
        with self.lock:
            if ga not in self.notified:
                # primeira leitura depois de (re)iniciar o painel: so registra o
                # estado atual, NAO avisa (evita a rajada de alertas a cada
                # atualizacao/reinicio - o estado aparece na tela normalmente)
                self.notified[ga] = alarm_now
                self.pending.pop(ga, None)
                return
            if alarm_now == self.notified[ga]:
                self.pending.pop(ga, None)          # voltou antes de confirmar: ignora
            elif ga not in self.pending or self.pending[ga][0] != alarm_now:
                self.pending[ga] = (alarm_now, time.time())

    def _alert_loop(self):
        """Confirma estados que ficaram CONFIRM_S segundos e manda os alertas
        agrupados (uma mensagem para varios pontos que mudaram juntos)."""
        while True:
            time.sleep(2)
            now = time.time()
            pts = self.points_by_ga()
            with self.lock:
                for ga, (alarm_now, since) in list(self.pending.items()):
                    if now - since < CONFIRM_S:
                        continue
                    del self.pending[ga]
                    p = pts.get(ga)
                    if not p or self.notified.get(ga) == alarm_now:
                        continue
                    self.notified[ga] = alarm_now
                    self.batch.append((p, alarm_now))
                    if self.batch_since is None:
                        self.batch_since = now
                ready = self.batch and now - self.batch_since >= BATCH_S
                batch = self.batch if ready else []
                if ready:
                    self.batch, self.batch_since = [], None
            if batch:
                self._send_batch(batch)

    def _send_batch(self, batch):
        alarms = [b for b in batch if b[1]]
        fire = any(p.get("tipo") == "incendio" for p, a in alarms)
        icon = "🔥" if fire else ("🚨" if alarms else "✅")
        tipos = sorted({TIPOS_ALARME.get(p.get("tipo"), "KNX") for p, a in batch})
        lines = []
        for p, a in batch:
            lines.append(f"{'⚠️' if a else '✅'} {p.get('grupo')} · {p.get('nome')} → {'ALARME' if a else 'NORMAL'}")
            self.log("KNX " + ("ALARME" if a else "NORMAL"), p.get("nome"), p.get("ga"),
                     detail=f"{TIPOS_ALARME.get(p.get('tipo'), 'KNX')} - {p.get('grupo')}")
        texto = f"{icon} ALERTA FONTE GRANDE\n\n{' / '.join(tipos)}\n\n" + "\n".join(lines)
        try:
            self.notify(texto)
        except Exception:
            pass

    @staticmethod
    def is_alarm(point, value):
        if value is None:
            return False
        try:
            return int(value) == int(point.get("alarme_valor", 1))
        except (TypeError, ValueError):
            return False

    # --------------- ciclo de vida ---------------
    def _initial_reads(self):
        time.sleep(1)
        for p in self.cfg.get("points", []):
            if not p.get("legivel") or p.get("tipo") in ("comando", "solicitar_status"):
                continue
            if not (self.tunnel and self.tunnel.connected):
                return
            try:
                self.tunnel.group_read(p["ga"])
            except Exception:
                pass
            time.sleep(0.08)   # nao sobrecarrega o barramento (9600 bit/s)

    def start(self):
        if not self.cfg.get("enabled", True) or not self.cfg.get("points"):
            return
        self.tunnel = KnxTunnel(self.cfg.get("gateway"), self.cfg.get("port", KNX_PORT),
                                on_value=self._on_value, log=self.log)
        self.thread = threading.Thread(target=self.tunnel.run, kwargs={"after_connect": self._initial_reads},
                                       daemon=True)
        self.thread.start()
        threading.Thread(target=self._alert_loop, daemon=True).start()

    def stop(self):
        if self.tunnel:
            self.tunnel._stop = True

    # --------------- API ---------------
    def snapshot(self):
        t = self.tunnel
        with self.lock:
            state = dict(self.state)
        pts = []
        for p in self.cfg.get("points", []):
            if p.get("tipo") in ("comando",):
                continue
            st = state.get(p["ga"])
            item = dict(p)
            if st:
                item.update(value=st["value"], ts=st["ts"], since=st.get("since"), src=st["src"])
                item["alarme"] = self.is_alarm(p, st["value"])
            pts.append(item)
        return {
            "gateway": self.cfg.get("gateway"),
            "port": self.cfg.get("port", KNX_PORT),
            "enabled": bool(self.cfg.get("enabled", True)),
            "connected": bool(t and t.connected),
            "error": (t.error if t else ("desativado" if not self.cfg.get("enabled", True) else "sem pontos cadastrados")),
            "connected_since": t.connected_since if t else None,
            "last_rx": t.last_rx if t else None,
            "rx_count": t.rx_count if t else 0,
            "tunnel_address": t.individual if t else None,
            "points": pts,
            "reset_groups": self.reset_groups(),
        }

    def update_point(self, ga, changes):
        with self.lock:
            for p in self.cfg.get("points", []):
                if p["ga"] == ga:
                    if "nome" in changes and str(changes["nome"]).strip():
                        p["nome"] = str(changes["nome"]).strip()[:120]
                    if "alarme_valor" in changes:
                        p["alarme_valor"] = 1 if int(changes["alarme_valor"]) else 0
                    if "alerta_telegram" in changes:
                        p["alerta_telegram"] = bool(changes["alerta_telegram"])
                    if "oculto" in changes:
                        p["oculto"] = bool(changes["oculto"])
                    p["editado"] = True
                    # mudou a logica: o estado atual vira a nova referencia (sem alerta)
                    st = self.state.get(ga)
                    if st:
                        self.notified[ga] = self.is_alarm(p, st["value"])
                    self.pending.pop(ga, None)
                    self._save()
                    return dict(p)
        return None

    def reset_groups(self):
        """centrais que tem objeto 'Reset' (a MT/S reseta a central inteira)."""
        return sorted({p.get("grupo") for p in self.cfg.get("points", [])
                       if p.get("tipo") == "comando" and str(p.get("nome", "")).strip().lower().startswith("reset")})

    def reset_central(self, grupo, origem=""):
        """Comando MANUAL (botao na tela, com confirmacao): manda 1 no objeto
        'Reset' da central. Fica registrado no log e avisa no Telegram."""
        if not (self.tunnel and self.tunnel.connected):
            raise ConnectionError("tunel KNX desconectado")
        now = time.time()
        if now - self._last_reset.get(grupo, 0) < 15:
            raise RuntimeError("reset desta central enviado ha menos de 15 s - aguarde")
        pts = self.cfg.get("points", [])
        reset = next((p for p in pts if p.get("grupo") == grupo and p.get("tipo") == "comando"
                      and str(p.get("nome", "")).strip().lower().startswith("reset")), None)
        if not reset:
            raise RuntimeError("esta central nao tem objeto de Reset no KNX")
        self.tunnel.group_write_bit(reset["ga"], 1)
        self._last_reset[grupo] = now
        ativos = [p.get("nome") for p in pts if p.get("grupo") == grupo and p.get("tipo") in ("incendio", "alarme_geral")
                  and self.is_alarm(p, (self.state.get(p["ga"]) or {}).get("value"))]
        self.log("KNX RESET", grupo, reset["ga"], detail=("pelo PC " + origem if origem else "") +
                 (" - zonas em alarme: " + ", ".join(ativos) if ativos else ""))
        try:
            self.notify(f"🔄 ALERTA FONTE GRANDE\n\nRESET DA CENTRAL {grupo}\nenviado pelo painel"
                        + (f" (PC {origem})" if origem else "")
                        + (f"\nzonas em alarme no momento: {', '.join(ativos)}" if ativos else ""))
        except Exception:
            pass

        def depois():
            # pede o estado de novo para a tela mostrar se a zona voltou ao normal
            time.sleep(3)
            req = next((p for p in pts if p.get("grupo") == grupo and p.get("tipo") == "solicitar_status"), None)
            try:
                if req and self.tunnel and self.tunnel.connected:
                    self.tunnel.group_write_bit(req["ga"], 1)
            except Exception:
                pass
            for p in pts:
                if p.get("grupo") == grupo and p.get("legivel") and p.get("tipo") not in ("comando", "solicitar_status"):
                    try:
                        self.tunnel.group_read(p["ga"])
                    except Exception:
                        pass
                    time.sleep(0.08)
        threading.Thread(target=depois, daemon=True).start()
        return reset["ga"]

    def request_status(self):
        """Comando manual: manda 1 nos objetos 'Request Status' das centrais MT/S
        (elas respondem reenviando o estado de todas as zonas)."""
        if not (self.tunnel and self.tunnel.connected):
            raise ConnectionError("tunel KNX desconectado")
        sent = 0
        for p in self.cfg.get("points", []):
            if p.get("tipo") == "solicitar_status":
                self.tunnel.group_write_bit(p["ga"], 1)
                sent += 1
                time.sleep(0.1)
        self.log("KNX SOLICITAR STATUS", f"{sent} centrais", self.cfg.get("gateway"))
        threading.Thread(target=self._initial_reads, daemon=True).start()
        return sent
