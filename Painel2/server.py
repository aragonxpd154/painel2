#!/usr/bin/env python3
"""
Sistema Painel - Rede Gazeta (Transmissao)
-------------------------------------------------------------------
Servidor de monitoramento em tempo real dos abrigos (Fonte Grande, Sede,
Serra, Morro do Ceu, Morro do Moreno, Viana, Domingos Martins, Pedra Azul).
Roda inteiramente com a biblioteca padrao do Python (sem pip install).
Faz, em background, para cada dispositivo cadastrado:
  - ping (reachable / latencia)                      -> dispositivos SNMP
  - SNMP GET (v1/v2c) de cada OID configurado
  - Modbus TCP (FC1/FC2/FC3/FC4) de cada registrador -> geradores, medidores,
    reles, CLPs (conexao TCP na porta 502 conta como "online")
e expoe o resultado em /api/status para o front-end consumir.

A lista de dispositivos vem das planilhas xio_* do NETx (pasta planilhas/),
importadas pelo script importar_planilhas.py.

Uso:
    python3 server.py [porta]

Depois abra no navegador:
    http://localhost:8080   (ou a porta escolhida)

Requisitos:
  - Python 3.7+
  - Rodar na mesma rede / maquina com acesso aos IPs dos dispositivos
  - UDP porta 161 liberada para os dispositivos que tem SNMP configurado
  - Permissao para executar o comando "ping" do sistema operacional
"""

import json
import mimetypes
import os
import re
import sqlite3
import secrets
import socket
import struct
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
import time
import urllib.request
import urllib.parse
import urllib.error
import uuid
import base64
import hashlib
from collections import deque
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEVICES_FILE = os.path.join(BASE_DIR, "network.json")
HTML_FILE = os.path.join(BASE_DIR, "mapa_interior.html")
CONFIG_HTML_FILE = os.path.join(BASE_DIR, "config.html")
LOGS_HTML_FILE = os.path.join(BASE_DIR, "logs.html")
DASHBOARD_HTML_FILE = os.path.join(BASE_DIR, "falhas.html")
RELATORIO_HTML_FILE = os.path.join(BASE_DIR, "relatorio.html")
RELATORIO_ANUAL_HTML_FILE = os.path.join(BASE_DIR, "relatorio_anual.html")
NOBREAK_HTML_FILE = os.path.join(BASE_DIR, "nobreak.html")
PLANNING_HTML_FILE = os.path.join(BASE_DIR, "planning.html")
ARQUIVOS_HTML_FILE = os.path.join(BASE_DIR, "arquivos.html")
REFRIGERACAO_HTML_FILE = os.path.join(BASE_DIR, "refrigeracao.html")
RELATORIOS_SITES_HTML_FILE = os.path.join(BASE_DIR, "relatorios_sites.html")
INVENTARIO_HTML_FILE = os.path.join(BASE_DIR, "inventario.html")
LIXEIRA_HTML_FILE = os.path.join(BASE_DIR, "lixeira.html")
TX_BE1001_HTML_FILE = os.path.join(BASE_DIR, "relatorio_tx_be1001.html")
TX_BE1001_MANUAL_PDF = os.path.join(BASE_DIR, "manual_tx_be1001.pdf")
TX_VITORIA_FG61_HTML_FILE = os.path.join(BASE_DIR, "relatorio_tx_vitoria_fg61.html")
TX_VITORIA_FG61_MANUAL_PDF = os.path.join(BASE_DIR, "manual_tx_vitoria_fg61.pdf")
TX_ODIA905FG_HTML_FILE = os.path.join(BASE_DIR, "relatorio_tx_odia905fg.html")
TX_ODIA905FG_MANUAL_PDF = os.path.join(BASE_DIR, "manual_tx_odia905fg.pdf")
TRANSMISSORES_FG_HTML_FILE = os.path.join(BASE_DIR, "relatorio_transmissores_fg.html")
GRAFICOS_HTML_FILE = os.path.join(BASE_DIR, "graficos_historico.html")
LOGIN_HTML_FILE = os.path.join(BASE_DIR, "login.html")
SELECTOR_HTML_FILE = os.path.join(BASE_DIR, "selecionar.html")
MAPA_FG_HTML_FILE = os.path.join(BASE_DIR, "mapa_fg.html")
MAPA_GENERICO_HTML_FILE = os.path.join(BASE_DIR, "mapa_generico.html")
ENERGIA_HTML_FILE = os.path.join(BASE_DIR, "energia.html")
CLIMATIZACAO_HTML_FILE = os.path.join(BASE_DIR, "climatizacao.html")
INCENDIO_HTML_FILE = os.path.join(BASE_DIR, "incendio.html")
KNX_MONITOR = None   # iniciado no main() (alarme de incendio da Fonte Grande via KNX)

# --------------------------------------------------------------------------
# ABRIGOS - cada um tem o seu proprio mapa (/mapa-abrigo/<slug>), no mesmo
# estilo do Mapa FG. Os dispositivos continuam no network.json (campo
# "site"); aqui so se define a ordem, o nome exibido e o nome usado nos
# alertas do Telegram ("ALERTA SERRA", "ALERTA GUARAPARI"...).
# --------------------------------------------------------------------------
ABRIGOS_FILE = os.path.join(BASE_DIR, "abrigos.json")
ABRIGOS_LOCK = threading.Lock()
DEFAULT_FG_INFO = {"nome": "Abrigo Fonte Grande", "alerta": "FONTE GRANDE", "sub": ""}
DEFAULT_ABRIGOS = [
    {"slug": "sede", "site": "SEDE", "nome": "Abrigo Sede", "alerta": "SEDE"},
    {"slug": "serra", "site": "SERRA", "nome": "Abrigo Serra", "alerta": "SERRA"},
    {"slug": "guarapari", "site": "MORRO DO CÉU", "nome": "Abrigo Guarapari", "sub": "Morro do Céu", "alerta": "GUARAPARI"},
    {"slug": "morro-do-moreno", "site": "MORRO DO MORENO", "nome": "Abrigo Morro do Moreno", "alerta": "MORRO DO MORENO"},
    {"slug": "viana", "site": "VIANA", "nome": "Abrigo Viana", "alerta": "VIANA"},
    {"slug": "domingos-martins", "site": "DOMINGOS MARTINS", "nome": "Abrigo Domingos Martins", "sub": "Campinho", "alerta": "DOMINGOS MARTINS"},
    {"slug": "pedra-azul", "site": "PEDRA AZUL", "nome": "Abrigo Pedra Azul", "alerta": "PEDRA AZUL"},
]
ABRIGOS = DEFAULT_ABRIGOS   # compatibilidade; use get_abrigos()

# Posicao (aproximada - ajuste na tela, arrastando o marcador) e codigo do
# cliente EDP (ESCELSA) de cada abrigo, para abrir chamado.
DEFAULT_LOCAIS = {
    "fg":               {"lat": -20.3053, "lon": -40.3381, "edp": "9502658"},
    "sede":             {"lat": -20.3069, "lon": -40.3128, "edp": "9500015"},
    "serra":            {"lat": -20.1633, "lon": -40.2962, "edp": "1281544"},
    "guarapari":        {"lat": -20.6578, "lon": -40.5023, "edp": "1033487"},
    "morro-do-moreno":  {"lat": -20.3303, "lon": -40.2771, "edp": "160152467"},
    "viana":            {"lat": -20.3895, "lon": -40.4960, "edp": "160569344"},
    "domingos-martins": {"lat": -20.4130, "lon": -40.6880, "edp": "160684274"},
    "pedra-azul":       {"lat": -20.4040, "lon": -40.9650, "edp": "152862"},
}
DEFAULT_EDP_EXTRAS = [
    {"local": "JABURUNA", "codigo": "1287211"},
    {"local": "ITANHENGA", "codigo": "150708"},
]
EDP_TELEFONE = "0800 721 0707"


def _fill_local(item, slug):
    d = DEFAULT_LOCAIS.get(slug) or {}
    if item.get("lat") in (None, "") or item.get("lon") in (None, ""):
        if d:
            item["lat"], item["lon"] = d["lat"], d["lon"]
            item.setdefault("coord_aprox", True)
    if not item.get("edp") and d.get("edp"):
        item["edp"] = d["edp"]
    return item


def load_abrigos_config():
    """abrigos.json: nomes exibidos (editaveis na Configuracao), nome usado
    no alerta e os abrigos novos que forem criados."""
    try:
        with open(ABRIGOS_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {}
    fg = dict(DEFAULT_FG_INFO)
    fg.update({k: v for k, v in (data.get("fg") or {}).items() if v})
    if fg.get("nome") == "Abrigo FG":      # nome padrao antigo -> "Abrigo Fonte Grande"
        fg["nome"] = "Abrigo Fonte Grande"
    if fg.get("sub") == "Fonte Grande":
        fg["sub"] = ""
    abrigos = data.get("abrigos")
    if not isinstance(abrigos, list) or not abrigos:
        abrigos = [dict(a) for a in DEFAULT_ABRIGOS]
    for ab in abrigos:   # nome antigo padrao da Sede -> "Abrigo Sede"
        if ab.get("slug") == "sede" and ab.get("nome") == "Abrigo Sede Rede Gazeta":
            ab["nome"] = "Abrigo Sede"
        _fill_local(ab, ab.get("slug"))
    _fill_local(fg, "fg")
    extras = data.get("edp_extras")
    if not isinstance(extras, list):
        extras = [dict(x) for x in DEFAULT_EDP_EXTRAS]
    return {"fg": fg, "abrigos": abrigos, "edp_extras": extras}


def save_abrigos_config(cfg):
    with ABRIGOS_LOCK:
        tmp = ABRIGOS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
        os.replace(tmp, ABRIGOS_FILE)


def get_abrigos():
    return load_abrigos_config()["abrigos"]


def _slugify(text):
    import unicodedata
    t = unicodedata.normalize("NFKD", str(text or "")).encode("ascii", "ignore").decode().lower()
    t = re.sub(r"^abrigo\s+", "", t.strip())
    return re.sub(r"[^a-z0-9]+", "-", t).strip("-") or "abrigo"
ABRIGO_CATEGORIES_ORDER = ["TRANSMISSORES", "GERADORES", "MEDIÇÃO DE ENERGIA", "NOBREAK",
                           "ENLACES", "INFRAESTRUTURA"]


def _norm_site(text):
    import unicodedata
    t = unicodedata.normalize("NFKD", str(text or "")).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", t).strip().upper()


def abrigo_by_slug(slug):
    return next((a for a in get_abrigos() if a["slug"] == slug), None)


def abrigo_for_site(site):
    key = _norm_site(site)
    return next((a for a in get_abrigos() if _norm_site(a["site"]) == key), None)


def abrigo_category(dev):
    """Categoria do dispositivo dentro do mapa do abrigo (mesmo padrao do FG)."""
    kind = (dev.get("kind") or "other").lower()
    text = " ".join([str(dev.get("name") or ""), str(dev.get("netx_id") or "")]).upper()
    if kind == "tx":
        return "TRANSMISSORES"
    if kind == "ups":
        return "NOBREAK"
    if kind == "link":
        return "ENLACES"
    if kind == "sensor" or re.search(r"FLEX|EITV|PLC|CLP|SELENIO|LANTRONIX", text):
        return "INFRAESTRUTURA"   # FLEX, EITV, CLP (mesmo que o nome tenha "GERADORES")
    if kind == "power" or re.search(r"GMG|GERADOR|MEDIDOR|MED\.|MULT|SEPAM", text):
        if re.search(r"GMG|GERADOR", text):
            return "GERADORES"
        if re.search(r"INVERSOR|_INV", text):
            return "INFRAESTRUTURA"
        return "MEDIÇÃO DE ENERGIA"
    return "INFRAESTRUTURA"


def build_abrigo_network(slug):
    """Monta, para um abrigo, a mesma estrutura do network_fg.json
    (categorias), a partir dos dispositivos do network.json."""
    ab = abrigo_by_slug(slug)
    if not ab:
        return None
    data = load_devices()
    key = _norm_site(ab["site"])
    devs = [dict(d) for d in data.get("devices", []) if _norm_site(d.get("site")) == key]
    layout = load_map_layout("abrigo-" + slug) or {}
    saved_order = layout.get("order") or {}
    cats = {}
    for d in devs:
        cat = abrigo_category(d)
        d["category"] = cat
        cats.setdefault(cat, []).append(d)
    for cat, lst in cats.items():
        order = [str(x) for x in (saved_order.get(cat) or [])]
        if order:
            lst.sort(key=lambda d: order.index(str(d.get("id"))) if str(d.get("id")) in order else 999)
    cats_order = [c for c in ABRIGO_CATEGORIES_ORDER if c in cats] + [c for c in cats if c not in ABRIGO_CATEGORIES_ORDER]
    titulo = ab["nome"]
    return {
        "map_name": titulo,
        "abrigo": ab,
        "caption": re.sub(r"^Abrigo\s+", "", ab["nome"]) + (" — " + ab["sub"] if ab.get("sub") else ""),
        "categories_order": cats_order,
        "categories": {c: cats[c] for c in cats_order},
        "devices": [d for c in cats_order for d in cats[c]],
        "local": dict(slug=slug, nome=ab["nome"], lat=ab.get("lat"), lon=ab.get("lon"), edp=ab.get("edp"),
                      coord_aprox=ab.get("coord_aprox", False), edp_tel=EDP_TELEFONE),
    }


def abrigo_alert_name(device):
    """Nome do abrigo que aparece no cabecalho do alerta do Telegram."""
    if device.get("map") == "fg":
        return load_abrigos_config()["fg"].get("alerta") or "FONTE GRANDE"
    ab = abrigo_for_site(device.get("site"))
    if ab:
        return ab["alerta"]
    return _norm_site(device.get("site")) or "GAZETA"
STATIC_DIR = os.path.join(BASE_DIR, "static")
FAVICON_FILE = os.path.join(BASE_DIR, "favicon.ico")
DEVICES_FG_FILE = os.path.join(BASE_DIR, "network_fg.json")

# --------------------------------------------------------------------------
# Registro de mapas - permite criar mapas NOVOS alem do Interior e do FG,
# cada um com seu proprio link (/mapa/<id>), lista de dispositivos e
# layout editavel pelo Editor de Mapa. Interior e FG sao mapas "legado"
# (paginas proprias, ja existentes); mapas novos usam a pagina generica
# mapa_generico.html.
# --------------------------------------------------------------------------
MAPS_REGISTRY_FILE = os.path.join(BASE_DIR, "maps.json")
MAPS_REGISTRY_LOCK = threading.Lock()

_DEFAULT_MAPS_REGISTRY = {
    "interior": {"name": "Abrigos (dispositivos)", "kind": "legacy", "url": "/selecionar"},
    "fg": {"name": "Abrigo Fonte Grande", "kind": "legacy", "url": "/mapa-fg"},
    "biblioteca": {"name": "Biblioteca de Dispositivos", "kind": "library", "url": "/mapa/biblioteca"},
}


def load_maps_registry():
    try:
        with open(MAPS_REGISTRY_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {}
    for map_id, entry in _DEFAULT_MAPS_REGISTRY.items():
        data.setdefault(map_id, entry)
    return data


def save_maps_registry(data):
    with MAPS_REGISTRY_LOCK:
        tmp = MAPS_REGISTRY_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, MAPS_REGISTRY_FILE)


def devices_file_for_map(map_id):
    """Caminho do arquivo de dispositivos de qualquer mapa (legado ou
    novo). Interior e FG usam seus arquivos historicos; mapas novos usam
    network_<id>.json."""
    if map_id == "interior":
        return DEVICES_FILE
    if map_id == "fg":
        return DEVICES_FG_FILE
    safe_id = re.sub(r"[^a-z0-9_-]", "", map_id.lower())
    return os.path.join(BASE_DIR, f"network_{safe_id}.json")


def load_devices_generic(map_id):
    """Le a lista de dispositivos de QUALQUER mapa no formato 'categorias'
    (mesmo formato do Mapa FG - categories_order/categories/devices).
    Interior usa formato proprio (sites/LAYOUT) e continua tendo sua
    propria rota especifica; esta funcao serve o FG e todos os mapas
    novos."""
    path = devices_file_for_map(map_id)
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {
            "map_name": load_maps_registry().get(map_id, {}).get("name", map_id),
            "categories_order": ["GERAL"],
            "devices": [],
            "categories": {"GERAL": []},
        }


def create_new_map(map_id, display_name):
    """Cria um mapa novo: registra no maps.json e cria o arquivo de
    dispositivos vazio (uma categoria 'GERAL' para comecar)."""
    safe_id = re.sub(r"[^a-z0-9_-]", "", map_id.lower().replace(" ", "-"))
    if not safe_id:
        return None, "id invalido"

    registry = load_maps_registry()
    if safe_id in registry:
        return None, "ja existe um mapa com esse id"

    registry[safe_id] = {
        "name": display_name or safe_id,
        "kind": "custom",
        "url": f"/mapa/{safe_id}",
    }
    save_maps_registry(registry)

    devices_path = devices_file_for_map(safe_id)
    empty_data = {
        "map_name": display_name or safe_id,
        "categories_order": ["GERAL"],
        "devices": [],
        "categories": {"GERAL": []},
    }
    with open(devices_path, "w", encoding="utf-8") as f:
        json.dump(empty_data, f, ensure_ascii=False, indent=2)

    return safe_id, None

# --------------------------------------------------------------------------
# Autenticacao (Sistema Painel) - protecao simples por sessao.
# Adequada para uso em rede interna/confiavel; nao substitui HTTPS/2FA
# se este servidor for exposto na internet.
# --------------------------------------------------------------------------
AUTH_USERNAME = "adminfg"
AUTH_PASSWORD = "@fg123"
SESSION_COOKIE_NAME = "painel_session"
SESSION_TTL_S = 30 * 24 * 3600  # 30 dias
SESSIONS_FILE = os.path.join(BASE_DIR, "sessions.json")

SESSIONS_LOCK = threading.Lock()
SESSIONS = {}  # token -> expira_em (timestamp)


def load_sessions():
    """Recarrega sessoes salvas em disco ao iniciar o servidor, para que
    um restart do server.py (ex: apos atualizar o codigo) nao obrigue
    todo mundo a logar de novo enquanto a sessao ainda for valida."""
    global SESSIONS
    try:
        with open(SESSIONS_FILE, encoding="utf-8") as f:
            raw = json.load(f)
        now = time.time()
        SESSIONS = {tok: exp for tok, exp in raw.items() if exp > now}
    except (FileNotFoundError, json.JSONDecodeError):
        SESSIONS = {}


def _save_sessions():
    try:
        tmp = SESSIONS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(SESSIONS, f)
        os.replace(tmp, SESSIONS_FILE)
    except Exception as e:
        print("[sessions] erro ao salvar sessions.json:", e, file=sys.stderr)


def create_session():
    token = secrets.token_hex(24)
    with SESSIONS_LOCK:
        SESSIONS[token] = time.time() + SESSION_TTL_S
        _save_sessions()
    return token


def is_valid_session(token):
    if not token:
        return False
    with SESSIONS_LOCK:
        expiry = SESSIONS.get(token)
        if expiry is None:
            return False
        if expiry < time.time():
            del SESSIONS[token]
            _save_sessions()
            return False
        return True


def destroy_session(token):
    with SESSIONS_LOCK:
        if token in SESSIONS:
            del SESSIONS[token]
            _save_sessions()


# --------------------------------------------------------------------------
# Faixas de IP extras liberadas para acessar o painel (alem da rede local
# de sempre). Por padrao NAO restringe nada (igual sempre foi); so quando
# o usuario liga "restringir acesso" e que o sistema passa a bloquear
# quem nao estiver numa faixa privada/local OU numa das faixas listadas.
# --------------------------------------------------------------------------
NETWORK_CONFIG_FILE = os.path.join(BASE_DIR, "network_access_config.json")
NETWORK_CONFIG_LOCK = threading.Lock()
_DEFAULT_NETWORK_CONFIG = {"restrict_enabled": False, "extra_ranges": []}


def load_network_config():
    try:
        with open(NETWORK_CONFIG_FILE, encoding="utf-8") as f:
            data = json.load(f)
        for k, v in _DEFAULT_NETWORK_CONFIG.items():
            data.setdefault(k, v)
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        return dict(_DEFAULT_NETWORK_CONFIG)


def save_network_config(data):
    with NETWORK_CONFIG_LOCK:
        tmp = NETWORK_CONFIG_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, NETWORK_CONFIG_FILE)


def _ip_in_cidr(ip_str, cidr_str):
    try:
        import ipaddress
        return ipaddress.ip_address(ip_str) in ipaddress.ip_network(cidr_str, strict=False)
    except ValueError:
        return False


def is_ip_allowed_network(ip_str):
    """Confere se o IP pode acessar o painel. Sem restricao ligada, sempre
    permite (comportamento de sempre). Com restricao ligada, permite redes
    privadas/locais (pra nunca travar o acesso da propria rede) + qualquer
    faixa extra cadastrada em Configuracoes."""
    cfg = load_network_config()
    if not cfg.get("restrict_enabled"):
        return True

    import ipaddress
    try:
        addr = ipaddress.ip_address(ip_str)
    except ValueError:
        return False

    if addr.is_private or addr.is_loopback:
        return True

    for cidr in cfg.get("extra_ranges", []):
        if _ip_in_cidr(ip_str, cidr):
            return True

    return False


# --------------------------------------------------------------------------
# Protecao contra forca bruta no login - essencial agora que o painel pode
# ficar acessivel pela internet. Bloqueia um IP temporariamente apos varias
# tentativas erradas seguidas.
# --------------------------------------------------------------------------
LOGIN_ATTEMPTS_LOCK = threading.Lock()
LOGIN_ATTEMPTS = {}  # ip -> {"count": N, "blocked_until": timestamp|None}
LOGIN_MAX_ATTEMPTS = 5
LOGIN_LOCKOUT_S = 300  # 5 minutos de bloqueio apos exceder as tentativas


def is_login_blocked(ip):
    with LOGIN_ATTEMPTS_LOCK:
        entry = LOGIN_ATTEMPTS.get(ip)
        if not entry:
            return False
        if entry.get("blocked_until") and entry["blocked_until"] > time.time():
            return True
        if entry.get("blocked_until") and entry["blocked_until"] <= time.time():
            LOGIN_ATTEMPTS.pop(ip, None)  # bloqueio expirou, libera
        return False


def register_login_failure(ip):
    with LOGIN_ATTEMPTS_LOCK:
        entry = LOGIN_ATTEMPTS.setdefault(ip, {"count": 0, "blocked_until": None})
        entry["count"] += 1
        if entry["count"] >= LOGIN_MAX_ATTEMPTS:
            entry["blocked_until"] = time.time() + LOGIN_LOCKOUT_S
            entry["count"] = 0


def register_login_success(ip):
    with LOGIN_ATTEMPTS_LOCK:
        LOGIN_ATTEMPTS.pop(ip, None)


def get_session_token(handler):
    cookie_header = handler.headers.get("Cookie")
    if not cookie_header:
        return None
    cookie = SimpleCookie()
    cookie.load(cookie_header)
    morsel = cookie.get(SESSION_COOKIE_NAME)
    return morsel.value if morsel else None


EVENTS_LOG_FILE = os.path.join(BASE_DIR, "eventos.log")
EVENTS_MAX = 1000
INCIDENTS_FILE = os.path.join(BASE_DIR, "incidentes.json")
OBSERVATIONS_FILE = os.path.join(BASE_DIR, "observations.json")
OBSERVATIONS_LOCK = threading.Lock()


def load_observations():
    try:
        with open(OBSERVATIONS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_observation(incident_id, text):
    with OBSERVATIONS_LOCK:
        data = load_observations()
        if text:
            data[incident_id] = text
        else:
            data.pop(incident_id, None)
        tmp = OBSERVATIONS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, OBSERVATIONS_FILE)


# --------------------------------------------------------------------------
# Exclusao de ocorrencias INDIVIDUAIS do relatorio/dashboard - diferente do
# "report_enabled" do dispositivo (que tira o dispositivo inteiro), isso
# aqui deixa excluir so uma ocorrencia especifica, sem afetar as outras do
# mesmo equipamento nem o monitoramento.
# --------------------------------------------------------------------------
EXCLUDED_INCIDENTS_FILE = os.path.join(BASE_DIR, "excluded_incidents.json")
EXCLUDED_INCIDENTS_LOCK = threading.Lock()


def load_excluded_incidents():
    try:
        with open(EXCLUDED_INCIDENTS_FILE, encoding="utf-8") as f:
            return set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


def set_incident_excluded(incident_id, excluded):
    with EXCLUDED_INCIDENTS_LOCK:
        ids = load_excluded_incidents()
        if excluded:
            ids.add(incident_id)
        else:
            ids.discard(incident_id)
        tmp = EXCLUDED_INCIDENTS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(sorted(ids), f, ensure_ascii=False, indent=2)
        os.replace(tmp, EXCLUDED_INCIDENTS_FILE)


# --------------------------------------------------------------------------
# Localidades monitoradas em OUTROS PCs (sem equipamento/SNMP no nosso
# sistema) - entram no relatorio mensal do Mapa Interior mesmo sem
# nenhum dispositivo real, para preenchimento manual das ocorrencias.
# --------------------------------------------------------------------------
EXTRA_INTERIOR_SITES = []

MANUAL_ENTRIES_FILE = os.path.join(BASE_DIR, "manual_report_entries.json")
MANUAL_ENTRIES_LOCK = threading.Lock()


# --------------------------------------------------------------------------
# Controle de manutencao de Nobreak e Refrigeracao - paginas simples de
# historico por localidade, com espaco pra ir adicionando eventos novos
# (troca de bateria, preventiva, etc.) sem precisar criar coluna nova.
# --------------------------------------------------------------------------
NOBREAK_FILE = os.path.join(BASE_DIR, "nobreak_maintenance.json")
NOBREAK_LOCK = threading.Lock()
REFRIG_FILE = os.path.join(BASE_DIR, "refrigeracao_maintenance.json")
REFRIG_LOCK = threading.Lock()

# --------------------------------------------------------------------------
# Planning - agenda/calendario de atividades (tipo um "planner"): cada
# atividade tem titulo, local, data, prioridade, responsavel e status.
# Lista simples (nao tem hierarquia de municipio/localidade como os outros
# modulos), entao a exclusao/restauracao pela lixeira e direta.
# --------------------------------------------------------------------------
PLANNING_FILE = os.path.join(BASE_DIR, "planning.json")
PLANNING_LOCK = threading.Lock()

# --------------------------------------------------------------------------
# Arquivos - upload/gestao de arquivos gerais (documentos, planilhas,
# manuais, fotos avulsas etc) que nao se encaixam nos modulos especificos
# (relatorios de sites, inventario, etc). Mesmo padrao de save_site_pdf:
# o arquivo em si vai pro disco, so o metadado fica no JSON.
# --------------------------------------------------------------------------
ARQUIVOS_FILE = os.path.join(BASE_DIR, "arquivos.json")
ARQUIVOS_LOCK = threading.Lock()
ARQUIVOS_DIR = os.path.join(BASE_DIR, "arquivos_upload")
os.makedirs(ARQUIVOS_DIR, exist_ok=True)
MAX_ARQUIVO_BYTES = 50 * 1024 * 1024  # 50MB

# --------------------------------------------------------------------------
# Relatorio de tarefas realizadas por site/municipio - historico de
# servicos feitos em campo, com controle de pendencia (aberta/finalizada),
# responsavel e foto anexada. Diferente do Nobreak/Refrigeracao (que sao
# por tipo de equipamento), aqui e por MUNICIPIO/local visitado.
# --------------------------------------------------------------------------
SITES_TASKS_FILE = os.path.join(BASE_DIR, "relatorios_sites.json")
SITES_TASKS_LOCK = threading.Lock()
SITE_PHOTOS_DIR = os.path.join(BASE_DIR, "site_photos")
os.makedirs(SITE_PHOTOS_DIR, exist_ok=True)
MAX_PHOTO_BYTES = 6 * 1024 * 1024  # 6MB - generoso o suficiente pra foto de celular ja comprimida no navegador


def load_maintenance_file(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def save_maintenance_file(path, lock, data):
    with lock:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)


def load_nobreak_data():
    data = load_maintenance_file(NOBREAK_FILE, {"locations": [], "battery_reference": [], "battery_info": {}})
    data.setdefault("battery_info", {})  # arquivos salvos antes dessa funcionalidade nao tem essa chave
    return data


def save_nobreak_data(data):
    save_maintenance_file(NOBREAK_FILE, NOBREAK_LOCK, data)


def load_refrig_data():
    return load_maintenance_file(REFRIG_FILE, {"locations": []})


def save_refrig_data(data):
    save_maintenance_file(REFRIG_FILE, REFRIG_LOCK, data)


def load_planning_data():
    return load_maintenance_file(PLANNING_FILE, {"activities": []})


def save_planning_data(data):
    save_maintenance_file(PLANNING_FILE, PLANNING_LOCK, data)


def load_arquivos_data():
    return load_maintenance_file(ARQUIVOS_FILE, {"arquivos": []})


def save_arquivos_data(data):
    save_maintenance_file(ARQUIVOS_FILE, ARQUIVOS_LOCK, data)


def save_uploaded_arquivo(base64_data, original_name):
    """Decodifica um arquivo em base64 (de qualquer tipo) e salva em disco
    com um nome unico, retornando (nome_no_disco, tamanho_em_bytes). O
    nome original e guardado a parte (no arquivos.json) pra exibir na
    lista e no download."""
    if "," in base64_data:
        base64_data = base64_data.split(",", 1)[1]
    raw = base64.b64decode(base64_data)
    if len(raw) > MAX_ARQUIVO_BYTES:
        raise ValueError(f"arquivo maior que {MAX_ARQUIVO_BYTES // (1024*1024)}MB")
    ext = os.path.splitext(original_name or "")[1]
    filename = f"{uuid.uuid4()}{ext}"
    with open(os.path.join(ARQUIVOS_DIR, filename), "wb") as f:
        f.write(raw)
    return filename, len(raw)


def load_sites_tasks_data():
    return load_maintenance_file(SITES_TASKS_FILE, {"sites": []})


def save_sites_tasks_data(data):
    save_maintenance_file(SITES_TASKS_FILE, SITES_TASKS_LOCK, data)


# --------------------------------------------------------------------------
# Inventario de equipamentos por municipio - cadastro livre (nao depende de
# SNMP/monitoramento), com modelo, patrimonio, numero de serie, fabricante,
# categoria, status e data de aquisicao/instalacao de cada equipamento, alem
# de observacao e fotos. Reaproveita o mesmo diretorio/endpoint de fotos
# ja usado pelo Relatorios Sites (SITE_PHOTOS_DIR / /api/relatorios-sites/photo).
# --------------------------------------------------------------------------
INVENTARIO_FILE = os.path.join(BASE_DIR, "inventario.json")
INVENTARIO_LOCK = threading.Lock()


def load_inventario_data():
    return load_maintenance_file(INVENTARIO_FILE, {"sites": []})


def save_inventario_data(data):
    save_maintenance_file(INVENTARIO_FILE, INVENTARIO_LOCK, data)


# --------------------------------------------------------------------------
# Lixeira geral do sistema - qualquer exclusao feita em qualquer pagina
# (Relatorios Sites, Inventario, Nobreak, Refrigeracao, ocorrencias manuais
# do Relatorio Mensal) passa por aqui antes de sumir de verdade, guardando
# o objeto inteiro + o contexto minimo necessario pra restaurar no lugar
# certo depois. So sai da lixeira quando restaurado ou excluido definitivo.
# --------------------------------------------------------------------------
LIXEIRA_FILE = os.path.join(BASE_DIR, "lixeira.json")
LIXEIRA_LOCK = threading.Lock()

TIPO_LABELS = {
    "relatorios_sites_site": "Município — Relatórios Sites",
    "relatorios_sites_task": "Tarefa — Relatórios Sites",
    "inventario_site": "Município — Inventário",
    "inventario_equipamento": "Equipamento — Inventário",
    "inventario_anexo": "Anexo — Inventário",
    "nobreak_location": "Localidade — Nobreak",
    "nobreak_event": "Evento — Nobreak",
    "nobreak_battery": "Referência de bateria — Nobreak",
    "refrigeracao_location": "Localidade — Refrigeração",
    "refrigeracao_event": "Evento — Refrigeração",
    "manual_report_entry": "Ocorrência manual — Relatório Mensal",
    "tx_be1001_leitura": "Leitura semanal — TX BE 100.1",
    "tx_be1001_troca_valvula": "Troca de válvula — TX BE 100.1",
    "planning_activity": "Atividade — Planning",
    "arquivo": "Arquivo",
}


def load_lixeira():
    return load_maintenance_file(LIXEIRA_FILE, [])


def save_lixeira(items):
    save_maintenance_file(LIXEIRA_FILE, LIXEIRA_LOCK, items)


def mover_para_lixeira(tipo, descricao, dados, contexto=None):
    """Guarda um objeto que acabou de ser excluido na lixeira, junto com o
    contexto minimo (ex: site_id) necessario pra devolver ele pro lugar
    certo depois, caso o usuario clique em "restaurar"."""
    items = load_lixeira()
    items.append({
        "id": str(uuid.uuid4()),
        "tipo": tipo,
        "descricao": descricao,
        "dados": dados,
        "contexto": contexto,
        "excluido_em": time.time(),
    })
    save_lixeira(items)


def restaurar_da_lixeira(trash_id):
    """Tenta devolver um item da lixeira pro lugar de onde ele saiu.
    Retorna (ok: bool, erro: str|None). Se der certo, remove da lixeira;
    se o "pai" (municipio/localidade) tiver sido excluido tambem nesse
    meio tempo, mantem o item na lixeira e avisa o motivo."""
    items = load_lixeira()
    item = next((i for i in items if i["id"] == trash_id), None)
    if not item:
        return False, "item não encontrado na lixeira"

    tipo = item["tipo"]
    dados = item["dados"]
    ctx = item.get("contexto") or {}

    try:
        if tipo == "relatorios_sites_site":
            data = load_sites_tasks_data()
            if any(s["id"] == dados["id"] for s in data["sites"]):
                return False, "já existe um município com esse id"
            data["sites"].append(dados)
            save_sites_tasks_data(data)
        elif tipo == "relatorios_sites_task":
            data = load_sites_tasks_data()
            site = next((s for s in data["sites"] if s["id"] == ctx.get("site_id")), None)
            if not site:
                return False, f"o município \"{ctx.get('site_nome', '?')}\" não existe mais"
            site["tarefas"].append(dados)
            save_sites_tasks_data(data)
        elif tipo == "inventario_site":
            data = load_inventario_data()
            if any(s["id"] == dados["id"] for s in data["sites"]):
                return False, "já existe um município com esse id"
            data["sites"].append(dados)
            save_inventario_data(data)
        elif tipo == "inventario_equipamento":
            data = load_inventario_data()
            site = next((s for s in data["sites"] if s["id"] == ctx.get("site_id")), None)
            if not site:
                return False, f"o município \"{ctx.get('site_nome', '?')}\" não existe mais"
            site["equipamentos"].append(dados)
            save_inventario_data(data)
        elif tipo == "inventario_anexo":
            # o arquivo PDF em si nunca sai do disco quando so vai pra
            # lixeira (so a referencia no JSON e removida) - restaurar e
            # apenas devolver essa referencia pro municipio
            data = load_inventario_data()
            site = next((s for s in data["sites"] if s["id"] == ctx.get("site_id")), None)
            if not site:
                return False, f"o município \"{ctx.get('site_nome', '?')}\" não existe mais"
            if "anexos" not in site:
                site["anexos"] = []
            site["anexos"].append(dados)
            save_inventario_data(data)
        elif tipo == "nobreak_location":
            data = load_nobreak_data()
            if any(l["id"] == dados["id"] for l in data["locations"]):
                return False, "já existe uma localidade com esse id"
            data["locations"].append(dados)
            save_nobreak_data(data)
        elif tipo == "nobreak_event":
            data = load_nobreak_data()
            loc = next((l for l in data["locations"] if l["id"] == ctx.get("location_id")), None)
            if not loc:
                return False, f"a localidade \"{ctx.get('location_nome', '?')}\" não existe mais"
            loc["eventos"].append(dados)
            save_nobreak_data(data)
        elif tipo == "nobreak_battery":
            data = load_nobreak_data()
            if any(b["id"] == dados["id"] for b in data["battery_reference"]):
                return False, "já existe uma referência com esse id"
            data["battery_reference"].append(dados)
            save_nobreak_data(data)
        elif tipo == "refrigeracao_location":
            data = load_refrig_data()
            if any(l["id"] == dados["id"] for l in data["locations"]):
                return False, "já existe uma localidade com esse id"
            data["locations"].append(dados)
            save_refrig_data(data)
        elif tipo == "refrigeracao_event":
            data = load_refrig_data()
            loc = next((l for l in data["locations"] if l["id"] == ctx.get("location_id")), None)
            if not loc:
                return False, f"a localidade \"{ctx.get('location_nome', '?')}\" não existe mais"
            loc["eventos"].append(dados)
            save_refrig_data(data)
        elif tipo == "manual_report_entry":
            entries = load_manual_entries()
            if any(e["id"] == dados["id"] for e in entries):
                return False, "já existe uma ocorrência com esse id"
            entries.append(dados)
            save_manual_entries(entries)
        elif tipo == "tx_be1001_leitura":
            data = load_tx_be1001_data()
            if any(l["id"] == dados["id"] for l in data["leituras"]):
                return False, "já existe uma leitura com esse id"
            data["leituras"].append(dados)
            data["leituras"].sort(key=lambda l: l["data"])
            save_tx_be1001_data(data)
        elif tipo == "tx_be1001_troca_valvula":
            data = load_tx_be1001_data()
            if any(t["id"] == dados["id"] for t in data["trocas_valvula"]):
                return False, "já existe um registro de troca com esse id"
            data["trocas_valvula"].append(dados)
            data["trocas_valvula"].sort(key=lambda t: t["data"])
            save_tx_be1001_data(data)
        elif tipo == "planning_activity":
            data = load_planning_data()
            if any(a["id"] == dados["id"] for a in data["activities"]):
                return False, "já existe uma atividade com esse id"
            data["activities"].append(dados)
            save_planning_data(data)
        elif tipo == "arquivo":
            data = load_arquivos_data()
            if any(a["id"] == dados["id"] for a in data["arquivos"]):
                return False, "já existe um arquivo com esse id"
            data["arquivos"].append(dados)
            save_arquivos_data(data)
        else:
            return False, f"tipo desconhecido: {tipo}"
    except Exception as e:
        return False, str(e)

    items = [i for i in items if i["id"] != trash_id]
    save_lixeira(items)
    return True, None


# --------------------------------------------------------------------------
# Relatorio TX BE 100.1 - transmissor FM valvulado (familia Broadcast
# Electronics FM-30B/FM-35B, tetrodo Eimac), leituras semanais de painel
# (tensoes/correntes de placa, tela, grade, filamento, potencias de IPA e
# excitador, sintonias) + controle de troca da valvula + diagnostico
# automatico comparando cada leitura nova com a faixa normal historica.
# --------------------------------------------------------------------------
TX_BE1001_FILE = os.path.join(BASE_DIR, "tx_be1001.json")
TX_BE1001_LOCK = threading.Lock()

# nominal de fabrica (Manual BE FM-30B/FM-35B, Tabela 5-1/5-2, "Typical
# Meter Indications") pra Filament Voltage - usado so pra citar no
# diagnostico quando a tensao de filamento foge muito do valor de
# referencia do fabricante (a "faixa normal" do dia-a-dia usada pra
# alarme de verdade e calculada a partir do historico REAL desse
# transmissor especifico, que opera com valores proprios).
FILAMENT_VOLTAGE_NOMINAL_FABRICANTE = 10.0


def load_tx_be1001_data():
    default = {
        "info": {
            "nome_equipamento": "TX BE 100.1",
            "descricao": "Transmissor FM valvulado (Broadcast Electronics FM-30B/FM-35B, tetrodo Eimac) - frequência 100.1 MHz",
        },
        "leituras": [],
        "trocas_valvula": [],
        "faixas_normais": {},
    }
    return load_maintenance_file(TX_BE1001_FILE, default)


def save_tx_be1001_data(data):
    save_maintenance_file(TX_BE1001_FILE, TX_BE1001_LOCK, data)


def diagnosticar_leitura_tx_be1001(metrics, faixas_normais, leituras_anteriores):
    """Compara uma leitura (dict metrica->valor) com a faixa normal
    historica de cada metrica (calculada a partir do proprio historico
    DESSE transmissor - cada equipamento tem sua propria "normalidade" de
    calibracao/operacao), mais duas regras especiais por TENDENCIA:
    output power (que tem queda historica conhecida) e a relacao
    refletida/direta (tratada como mais uma metrica com faixa historica,
    mais um teto absoluto de seguranca que sempre alerta independente do
    historico, ja que VSWR muito alto pode danificar a valvula).

    Isso e um PRIMEIRO NIVEL de diagnostico automatico - fica registrado
    junto da leitura pra dar uma leitura rapida de "como esta o
    equipamento", mas nao substitui a analise de quem manja de RF."""
    observacoes = []
    alerta = False

    # calcula a relacao refletida/direta pra tratar como mais uma metrica
    # (junto com as demais, na faixa_normais) - cada transmissor tem uma
    # relacao "normal" propria dependendo do casamento de antena/linha
    metrics_com_ratio = dict(metrics)
    fwd = metrics.get("combined_fwd_power")
    rfl = metrics.get("combined_rfl_power")
    ratio_pct = None
    if fwd and rfl is not None and fwd > 0:
        ratio_pct = round(rfl / fwd * 100, 1)
        metrics_com_ratio["rfl_fwd_ratio"] = ratio_pct

    for key, faixa in faixas_normais.items():
        if key not in metrics_com_ratio:
            continue
        v = metrics_com_ratio[key]
        vmin, vmax = faixa["min"], faixa["max"]
        label = faixa.get("label", key)
        unit = faixa.get("unit", "")
        if v < vmin or v > vmax:
            fora_pct = max((vmin - v) / (vmax - vmin + 1e-9), (v - vmax) / (vmax - vmin + 1e-9)) * 100
            severidade = "ALERTA" if fora_pct > 25 else "atenção"
            if severidade == "ALERTA":
                alerta = True
            observacoes.append(
                f"{severidade}: {label} em {v}{unit} - fora da faixa normal histórica ({vmin}–{vmax}{unit})."
            )

    # teto absoluto de seguranca pra relacao refletida/direta, INDEPENDENTE
    # do historico - um VSWR muito alto pode danificar a valvula mesmo que
    # esse transmissor especifico ja opere numa faixa naturalmente mais
    # alta que o "ideal" de manual
    if ratio_pct is not None and ratio_pct > 40:
        alerta = True
        observacoes.append(
            f"ALERTA: potência refletida em {ratio_pct}% da direta - acima do teto de segurança (40%), "
            f"risco de dano à válvula por VSWR alto independente do histórico deste transmissor."
        )

    # output power (leitura de painel) - nao tem faixa fixa (esse
    # transmissor mostra uma tendencia historica de queda), entao o
    # diagnostico aqui e por TENDENCIA: cai muito da ultima leitura, ou
    # esta bem abaixo do pico ja visto
    op = metrics.get("output_power")
    if op is not None and leituras_anteriores:
        valores_anteriores = [l["metrics"].get("output_power") for l in leituras_anteriores if l["metrics"].get("output_power") is not None]
        if valores_anteriores:
            ultima = valores_anteriores[-1]
            pico = max(valores_anteriores + [op])
            if ultima > 0:
                queda_pct = (ultima - op) / ultima * 100
                if queda_pct > 15:
                    alerta = True
                    observacoes.append(
                        f"ALERTA: Output Power caiu {queda_pct:.0f}% em relação à última leitura "
                        f"({ultima} → {op}) - queda abrupta, verificar válvula e estágios de RF."
                    )
            if pico > 0 and op < pico * 0.5:
                observacoes.append(
                    f"atenção: Output Power ({op}) está bem abaixo do maior valor já registrado "
                    f"({pico}) - possível degradação progressiva da válvula, considere avaliar troca."
                )

    if not observacoes:
        texto = "Leitura dentro da faixa normal histórica em todos os parâmetros monitorados."
    else:
        texto = " ".join(observacoes)
    return texto, alerta


def save_task_photo(base64_data, original_name):
    """Decodifica uma foto em base64 (enviada pelo navegador) e salva como
    arquivo separado em disco, retornando so o nome do arquivo pra guardar
    na tarefa - evita que o relatorios_sites.json fique gigante com fotos
    embutidas direto no JSON."""
    if "," in base64_data:
        base64_data = base64_data.split(",", 1)[1]  # remove o prefixo "data:image/...;base64,"
    raw = base64.b64decode(base64_data)
    if len(raw) > MAX_PHOTO_BYTES:
        raise ValueError(f"foto maior que {MAX_PHOTO_BYTES // (1024*1024)}MB")
    ext = os.path.splitext(original_name or "")[1].lower()
    if ext not in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
        ext = ".jpg"
    filename = f"{uuid.uuid4()}{ext}"
    with open(os.path.join(SITE_PHOTOS_DIR, filename), "wb") as f:
        f.write(raw)
    return filename


# --------------------------------------------------------------------------
# Anexos em PDF por municipio, na pagina de Inventario (manuais, notas
# fiscais, laudos, etc). Segue o mesmo padrao de save_task_photo: o
# arquivo vai pra disco, so o nome fica salvo no inventario.json.
# --------------------------------------------------------------------------
INVENTARIO_PDFS_DIR = os.path.join(BASE_DIR, "inventario_pdfs")
os.makedirs(INVENTARIO_PDFS_DIR, exist_ok=True)
MAX_ANEXO_BYTES = 15 * 1024 * 1024  # 15MB - manuais/laudos podem ser maiores que uma foto


def save_site_pdf(base64_data, original_name):
    """Decodifica um anexo em base64 (qualquer tipo de arquivo - manual,
    planilha, laudo, foto, etc) e salva em disco, retornando o nome do
    arquivo + o tamanho pra guardar no anexo do municipio. O nome do
    arquivo no disco usa a MESMA extensao do arquivo original (antes so
    aceitava PDF e sempre salvava como .pdf)."""
    if "," in base64_data:
        base64_data = base64_data.split(",", 1)[1]
    raw = base64.b64decode(base64_data)
    if len(raw) > MAX_ANEXO_BYTES:
        raise ValueError(f"arquivo maior que {MAX_ANEXO_BYTES // (1024*1024)}MB")
    ext = os.path.splitext(original_name or "")[1]
    filename = f"{uuid.uuid4()}{ext}"
    with open(os.path.join(INVENTARIO_PDFS_DIR, filename), "wb") as f:
        f.write(raw)
    return filename, len(raw)


# --------------------------------------------------------------------------
# Visor de camera (Mapa FG) - o navegador bloqueia usuario/senha embutidos
# na URL quando usados como origem de imagem (<img src>), so permite ao
# digitar direto na barra de endereco. Por isso o proprio servidor busca
# a foto da camera (aqui nao ha essa restricao) e devolve pro navegador -
# a pagina nunca ve nem usa a senha da camera diretamente.
# --------------------------------------------------------------------------
CAMERA_SNAPSHOT_CREDENTIALS = [
    ("admin", "admin123"),  # modelos Intelbras mais novos
    ("admin", "admin"),     # modelos mais antigos
]
CAMERA_SNAPSHOT_TIMEOUT_S = 4
# fica OFF por padrao: com o mapa aberto, cada camera busca um snapshot
# novo a cada poucos segundos, e cada tentativa de login pode gerar varias
# linhas (basic + digest, as vezes 2 credenciais) - isso enchia o
# eventos.log rapido e empurrava os eventos de verdade (quedas de link,
# alarmes) pra fora da tela. Troque pra True temporariamente se precisar
# depurar problema de autenticacao de alguma camera especifica de novo.
CAMERA_SNAPSHOT_LOG_EVENTOS = False
CAMERA_AUTH_CACHE = {}  # ip -> (user, pwd, "basic"|"digest"|"raw_digest") que funcionou da ultima vez

# Cameras cuja senha CERTA ja foi confirmada manualmente (testando no
# navegador) - para essas, o sistema NUNCA tenta outra credencial (nunca
# manda a senha errada), so varia o METODO (basic/digest/raw_digest) com
# a MESMA senha certa. Mandar a senha errada repetidamente - mesmo que
# so pra "descobrir" a certa - parece disparar um bloqueio de seguranca
# temporario nessas cameras baratas, que passa a rejeitar ATE a senha
# certa por um tempo (foi exatamente isso que aconteceu com a Serra: apos
# varias tentativas com senha errada nos ciclos de fallback, ela comecou
# a rejeitar com 401 ATE a senha certa em qualquer metodo).
CAMERA_KNOWN_CREDENTIALS = {}

# Algumas cameras baratas (ex: Serra) nao aguentam 2 conexoes simultaneas -
# se o polling automatico e a visualizacao ao vivo no mapa baterem nela ao
# mesmo tempo, uma das duas simplesmente nao recebe resposta nenhuma. Este
# lock (um por IP) garante que so uma requisicao por vez seja feita pra
# cada camera, enfileirando a segunda em vez de deixar ela falhar.
CAMERA_FETCH_LOCKS = {}
CAMERA_FETCH_LOCKS_META_LOCK = threading.Lock()


def _get_camera_fetch_lock(ip):
    with CAMERA_FETCH_LOCKS_META_LOCK:
        lock = CAMERA_FETCH_LOCKS.get(ip)
        if lock is None:
            lock = threading.Lock()
            CAMERA_FETCH_LOCKS[ip] = lock
        return lock


def _raw_http_get(host, port, path, headers, timeout, _retry=True):
    """Manda um GET via socket puro (sem http.client/urllib) e devolve
    (status_code, headers_dict, body_bytes). Le os bytes crus da conexao
    e separa cabecalho/corpo manualmente - evita o erro de parsing
    ("BadStatusLine", cabecalho HTTP misturado com bytes de JPEG) que a
    camera da Serra causa na biblioteca padrao do Python quando responde
    a autenticacao Digest.

    Se a conexao nao devolver NADA (0 bytes, reset/fechada na hora), tenta
    UMA vez de novo apos uma pequena pausa antes de desistir - esse padrao
    (resposta totalmente vazia) apareceu especificamente quando outra
    requisicao concorrente (o polling automatico e a visualizacao ao vivo
    no mapa, por exemplo) bate na mesma camera quase ao mesmo tempo - essa
    camera especifica parece nao aguentar 2 conexoes simultaneas e
    simplesmente nao responde uma delas."""
    req_lines = [f"GET {path} HTTP/1.1", f"Host: {host}"]
    for k, v in headers.items():
        req_lines.append(f"{k}: {v}")
    req_lines.append("Connection: close")
    req_lines.append("")
    req_lines.append("")
    request = "\r\n".join(req_lines).encode("latin-1")

    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(request)
        chunks = []
        try:
            while True:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                chunks.append(chunk)
        except socket.timeout:
            pass
        raw = b"".join(chunks)

    if not raw and _retry:
        time.sleep(0.4)
        return _raw_http_get(host, port, path, headers, timeout, _retry=False)

    # Essa camera (Serra), na resposta AUTENTICADA (depois do desafio
    # Digest), manda so os bytes crus do JPEG - sem nenhuma linha de
    # status, sem cabecalhos, sem "\r\n\r\n" nenhum. E exatamente esse
    # comportamento que causava o "BadStatusLine: ÿØÿàJFIF..." na
    # biblioteca padrao (ela tentava ler os bytes do JPEG como se fossem
    # a linha de status HTTP). Se a resposta comeca com a marca de
    # inicio de um JPEG (FF D8), trata ela toda como o corpo da imagem,
    # assumindo sucesso (200) mesmo sem cabecalho nenhum.
    if raw[:2] == b"\xff\xd8":
        return 200, {}, raw

    sep = raw.find(b"\r\n\r\n")
    if sep == -1:
        raise ValueError("resposta sem separador de cabecalho/corpo (\\r\\n\\r\\n)")
    header_bytes = raw[:sep]
    body = raw[sep + 4:]

    header_lines = header_bytes.decode("latin-1", errors="replace").split("\r\n")
    status_parts = header_lines[0].split(" ", 2)
    status_code = int(status_parts[1]) if len(status_parts) > 1 else 0

    resp_headers = {}
    for line in header_lines[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            resp_headers[k.strip().lower()] = v.strip()

    # se veio Content-Length, corta o corpo no tamanho certo - por
    # seguranca, caso sobre algum byte extra apos o JPEG
    if "content-length" in resp_headers:
        try:
            n = int(resp_headers["content-length"])
            body = body[:n]
        except ValueError:
            pass

    return status_code, resp_headers, body


def _parse_digest_challenge(www_authenticate):
    """Extrai os parametros (realm, nonce, qop, opaque, ...) do cabecalho
    WWW-Authenticate de uma resposta 401 com autenticacao Digest."""
    params = {}
    challenge = www_authenticate.split(" ", 1)[1] if " " in www_authenticate else ""
    for m in re.finditer(r'(\w+)=("([^"]*)"|([^,]*))', challenge):
        key = m.group(1)
        value = m.group(3) if m.group(3) is not None else m.group(4)
        params[key] = value
    return params


def _try_camera_auth_raw_digest(url, user, pwd):
    """Faz a autenticacao Digest inteira manualmente (desafio + resposta)
    via socket puro, sem usar http.client/urllib pra ler a resposta.

    Existe especificamente pra camera da Serra: ela usa Digest, a senha
    certa (admin/admin123) ja foi confirmada funcionando no navegador, mas
    o parser HTTP padrao do Python (usado por urllib.request/http.client)
    quebra com "BadStatusLine" contendo bytes de JPEG - um sinal de que
    essa camera especifica manda a resposta de um jeito que o parser
    estrito nao aceita. Lendo os bytes crus e separando cabecalho/corpo a
    mao, esse problema e contornado."""
    parsed = urllib.parse.urlsplit(url)
    host = parsed.hostname
    port = parsed.port or 80
    path = parsed.path + (("?" + parsed.query) if parsed.query else "")

    # 1a requisicao, sem autenticacao, so pra receber o desafio Digest (401)
    status, headers, _ = _raw_http_get(host, port, path, {}, CAMERA_SNAPSHOT_TIMEOUT_S)
    if status != 401 or "www-authenticate" not in headers:
        raise ValueError(f"esperava 401 com desafio Digest, recebeu {status}")

    challenge = _parse_digest_challenge(headers["www-authenticate"])
    realm = challenge.get("realm", "")
    nonce = challenge.get("nonce", "")
    qop = challenge.get("qop")
    opaque = challenge.get("opaque")

    ha1 = hashlib.md5(f"{user}:{realm}:{pwd}".encode()).hexdigest()
    ha2 = hashlib.md5(f"GET:{path}".encode()).hexdigest()

    if qop:
        qop = qop.split(",")[0].strip()
        nc = "00000001"
        cnonce = secrets.token_hex(8)
        response = hashlib.md5(f"{ha1}:{nonce}:{nc}:{cnonce}:{qop}:{ha2}".encode()).hexdigest()
        auth_header = (
            f'Digest username="{user}", realm="{realm}", nonce="{nonce}", '
            f'uri="{path}", qop={qop}, nc={nc}, cnonce="{cnonce}", response="{response}"'
        )
    else:
        response = hashlib.md5(f"{ha1}:{nonce}:{ha2}".encode()).hexdigest()
        auth_header = (
            f'Digest username="{user}", realm="{realm}", nonce="{nonce}", '
            f'uri="{path}", response="{response}"'
        )
    if opaque:
        auth_header += f', opaque="{opaque}"'

    # 2a requisicao, com o Authorization certo - conexao NOVA (a primeira
    # ja foi fechada), sem risco de dessincronia entre desafio e resposta
    status2, _headers2, body2 = _raw_http_get(
        host, port, path, {"Authorization": auth_header}, CAMERA_SNAPSHOT_TIMEOUT_S
    )
    if status2 != 200:
        raise ValueError(f"HTTP {status2} apos enviar credencial Digest (raw)")
    if not body2:
        raise ValueError("resposta 200 mas sem corpo de imagem (raw)")
    return body2


def _try_camera_auth(url, user, pwd, method):
    """Faz UMA tentativa de pegar o instantaneo, com o metodo (basic,
    digest ou raw_digest) e credencial indicados. Retorna os bytes da
    imagem, ou lanca a excecao original em caso de falha (quem chama
    decide o que fazer).

    Nao manda "Connection: close" explicitamente no basic - o firmware de
    algumas cameras (ex: erro 500 mesmo com a senha certa, independente
    de qual credencial foi usada) parece nao lidar bem com esse
    cabecalho."""
    if method == "basic":
        token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
        req = urllib.request.Request(url, headers={"Authorization": f"Basic {token}"})
        with urllib.request.urlopen(req, timeout=CAMERA_SNAPSHOT_TIMEOUT_S) as resp:
            return resp.read()
    elif method == "raw_digest":
        # Digest calculado e lido manualmente via socket puro - usado pra
        # cameras (ex: Serra) cuja resposta confunde o parser HTTP padrao
        # do Python quando se usa urllib/http.client normalmente.
        return _try_camera_auth_raw_digest(url, user, pwd)
    else:  # digest
        pwd_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        pwd_mgr.add_password(None, url, user, pwd)
        digest_handler = urllib.request.HTTPDigestAuthHandler(pwd_mgr)
        opener = urllib.request.build_opener(digest_handler)
        # aqui SIM manda "Connection: close" (diferente do basic) - a
        # troca de autenticacao digest exige duas requisicoes (desafio +
        # resposta), e algumas cameras parecem ficar fora de sincronia se
        # reaproveitarem a mesma conexao entre as duas, respondendo a
        # segunda pergunta com a imagem colada sem cabecalho HTTP direito
        # (erro de parsing tipo "BadStatusLine" com bytes de JPEG no meio)
        req = urllib.request.Request(url, headers={"Connection": "close"})
        with opener.open(req, timeout=CAMERA_SNAPSHOT_TIMEOUT_S) as resp:
            return resp.read()


def _fetch_camera_snapshot_impl(ip):
    """Busca o instantaneo (JPEG) de uma camera Intelbras/Dahua. Retorna
    os bytes da imagem, ou None se nenhuma credencial/metodo funcionou.

    Guarda em CAMERA_AUTH_CACHE qual combinacao (usuario/senha/metodo) deu
    certo da ultima vez pra cada IP, e tenta ELA PRIMEIRO nas proximas
    chamadas.

    Para cameras com senha ja confirmada manualmente (CAMERA_KNOWN_CREDENTIALS),
    o sistema NUNCA tenta outra credencial - so varia o metodo (raw_digest/
    digest/basic) com a senha certa, evitando o bloqueio de seguranca que
    tentativas com senha errada podem disparar em cameras baratas.

    Para as demais, tenta cada credencial da lista com basic e depois
    digest, igual antes.

    O log de cada tentativa (CAMERA_SNAPSHOT_LOG_EVENTOS, la em cima do
    arquivo) fica DESLIGADO por padrao - com o mapa aberto, cada camera
    busca um snapshot novo a cada poucos segundos, e cada busca pode gerar
    varias linhas - isso enchia o eventos.log rapido e empurrava os
    eventos de verdade (quedas de link, alarmes, etc) pra fora da tela.
    Troque a constante pra True temporariamente se precisar depurar
    autenticacao de alguma camera."""
    path = "/cgi-bin/snapshot.cgi?channel=1"
    url = f"http://{ip}{path}"

    known = CAMERA_KNOWN_CREDENTIALS.get(ip)
    if known:
        # Camera com senha ja confirmada manualmente - so varia o metodo,
        # NUNCA tenta outra senha (evita disparar bloqueio de seguranca).
        user, pwd = known
        cached = CAMERA_AUTH_CACHE.get(ip)
        methods_order = ["raw_digest", "digest", "basic"]
        if cached and cached[:2] == (user, pwd) and cached[2] in methods_order:
            # tenta o metodo que funcionou da ultima vez primeiro
            methods_order.remove(cached[2])
            methods_order.insert(0, cached[2])
        for method in methods_order:
            try:
                data = _try_camera_auth(url, user, pwd, method)
                if data:
                    CAMERA_AUTH_CACHE[ip] = (user, pwd, method)
                    if CAMERA_SNAPSHOT_LOG_EVENTOS:
                        log_event("CAMERA SNAPSHOT OK", ip, None, detail=f"login={user}/{pwd} ({method}), {len(data)} bytes")
                    return data
            except urllib.error.HTTPError as e:
                if CAMERA_SNAPSHOT_LOG_EVENTOS:
                    log_event("CAMERA SNAPSHOT FALHOU", ip, None, detail=f"login={user}/{pwd} ({method}) -> HTTP {e.code}")
            except Exception as e:
                if CAMERA_SNAPSHOT_LOG_EVENTOS:
                    log_event("CAMERA SNAPSHOT FALHOU", ip, None, detail=f"login={user}/{pwd} ({method}) -> {type(e).__name__}: {e}")
        return None

    cached = CAMERA_AUTH_CACHE.get(ip)
    if cached:
        user, pwd, method = cached
        try:
            data = _try_camera_auth(url, user, pwd, method)
            if data:
                if CAMERA_SNAPSHOT_LOG_EVENTOS:
                    log_event("CAMERA SNAPSHOT OK", ip, None, detail=f"login={user}/{pwd} ({method}, cache), {len(data)} bytes")
                return data
        except Exception as e:
            if CAMERA_SNAPSHOT_LOG_EVENTOS:
                log_event("CAMERA SNAPSHOT FALHOU", ip, None, detail=f"login={user}/{pwd} ({method}, cache) -> {type(e).__name__}: {e}")
            # antes de arriscar a senha ERRADA (que pode disparar bloqueio
            # temporario em algumas cameras), tenta a MESMA credencial que
            # ja sabemos ser certa, so trocando de metodo: basic<->digest,
            # e se a camera usa digest (ou raw_digest), tenta o outro tipo
            # de digest tambem (normal via urllib, ou manual via socket)
            if method == "basic":
                other_method = "digest"
            elif method == "digest":
                other_method = "raw_digest"
            else:  # raw_digest
                other_method = "digest"
            try:
                data = _try_camera_auth(url, user, pwd, other_method)
                if data:
                    CAMERA_AUTH_CACHE[ip] = (user, pwd, other_method)
                    if CAMERA_SNAPSHOT_LOG_EVENTOS:
                        log_event("CAMERA SNAPSHOT OK", ip, None, detail=f"login={user}/{pwd} ({other_method}, cache), {len(data)} bytes")
                    return data
            except Exception as e2:
                if CAMERA_SNAPSHOT_LOG_EVENTOS:
                    log_event("CAMERA SNAPSHOT FALHOU", ip, None, detail=f"login={user}/{pwd} ({other_method}, cache) -> {type(e2).__name__}: {e2} - descartando cache")
            CAMERA_AUTH_CACHE.pop(ip, None)  # nenhum dos dois metodos funcionou - esquece e tenta tudo de novo

    for user, pwd in CAMERA_SNAPSHOT_CREDENTIALS:
        for method in ("basic", "digest", "raw_digest"):
            try:
                data = _try_camera_auth(url, user, pwd, method)
                if data:
                    CAMERA_AUTH_CACHE[ip] = (user, pwd, method)
                    if CAMERA_SNAPSHOT_LOG_EVENTOS:
                        log_event("CAMERA SNAPSHOT OK", ip, None, detail=f"login={user}/{pwd} ({method}), {len(data)} bytes")
                    return data
            except urllib.error.HTTPError as e:
                if CAMERA_SNAPSHOT_LOG_EVENTOS:
                    log_event("CAMERA SNAPSHOT FALHOU", ip, None, detail=f"login={user}/{pwd} ({method}) -> HTTP {e.code}")
            except Exception as e:
                if CAMERA_SNAPSHOT_LOG_EVENTOS:
                    log_event("CAMERA SNAPSHOT FALHOU", ip, None, detail=f"login={user}/{pwd} ({method}) -> {type(e).__name__}: {e}")
    return None


def fetch_camera_snapshot(ip):
    """Busca o instantaneo da camera (veja _fetch_camera_snapshot_impl pra
    detalhes de autenticacao) - so que serializado por IP: se duas
    chamadas pra MESMA camera acontecerem quase juntas (polling
    automatico + visualizacao ao vivo no mapa, por exemplo), a segunda
    espera a primeira terminar em vez de bater na camera ao mesmo tempo.
    Cameras baratas como a da Serra nao aguentam 2 conexoes simultaneas e
    simplesmente nao respondem uma delas quando isso acontece."""
    lock = _get_camera_fetch_lock(ip)
    with lock:
        return _fetch_camera_snapshot_impl(ip)


# --------------------------------------------------------------------------
# Cidades acompanhadas por OUTRO SISTEMA (ex: Painel Seja Digital) - nao tem
# equipamento/SNMP monitorado por aqui, mas o usuario preenche manualmente
# a situacao (ok/Fora) e uma observacao, mes a mes, pra aparecer no final
# do relatorio mensal - igual a planilha manual que ja usavam antes.
# --------------------------------------------------------------------------
OTHER_SYSTEM_CITIES = []

OTHER_SYSTEM_FILE = os.path.join(BASE_DIR, "outros_sistemas_report.json")
OTHER_SYSTEM_LOCK = threading.Lock()


def load_other_system_status(year, month):
    """Retorna a situacao/observacao de cada cidade acompanhada por outro
    sistema, para o mes indicado. Cidades sem dado salvo ainda aparecem
    com situacao 'ok' e observacao vazia (estado padrao)."""
    key = f"{year:04d}-{month:02d}"
    try:
        with open(OTHER_SYSTEM_FILE, encoding="utf-8") as f:
            all_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        all_data = {}
    month_data = all_data.get(key, {})

    result = []
    for city in OTHER_SYSTEM_CITIES:
        entry = month_data.get(city, {})
        result.append({
            "city": city,
            "situacao": entry.get("situacao", "ok"),
            "observacao": entry.get("observacao", ""),
        })
    return result


def save_other_system_status(year, month, city, situacao, observacao):
    if city not in OTHER_SYSTEM_CITIES:
        raise ValueError("cidade desconhecida")
    key = f"{year:04d}-{month:02d}"
    with OTHER_SYSTEM_LOCK:
        try:
            with open(OTHER_SYSTEM_FILE, encoding="utf-8") as f:
                all_data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            all_data = {}
        all_data.setdefault(key, {})[city] = {"situacao": situacao, "observacao": observacao}
        tmp = OTHER_SYSTEM_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, OTHER_SYSTEM_FILE)


def load_other_system_status_annual(year):
    """Resume o ano inteiro pra cada cidade acompanhada por outro sistema:
    quantos meses ficou 'Fora' e a observacao mais recente preenchida."""
    try:
        with open(OTHER_SYSTEM_FILE, encoding="utf-8") as f:
            all_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        all_data = {}

    result = []
    for city in OTHER_SYSTEM_CITIES:
        months_fora = 0
        latest_observacao = ""
        latest_situacao = "ok"
        for month in range(1, 13):
            key = f"{year:04d}-{month:02d}"
            entry = all_data.get(key, {}).get(city)
            if not entry:
                continue
            if entry.get("situacao") == "Fora":
                months_fora += 1
            latest_situacao = entry.get("situacao", latest_situacao)
            if entry.get("observacao"):
                latest_observacao = entry["observacao"]
        result.append({
            "city": city,
            "months_fora": months_fora,
            "latest_situacao": latest_situacao,
            "latest_observacao": latest_observacao,
        })
    return result


def load_manual_entries():
    try:
        with open(MANUAL_ENTRIES_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def save_manual_entries(entries):
    with MANUAL_ENTRIES_LOCK:
        tmp = MANUAL_ENTRIES_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(entries, f, ensure_ascii=False, indent=2)
        os.replace(tmp, MANUAL_ENTRIES_FILE)


def add_manual_entry(map_id, site, date_str, description, start_time, end_time, observation):
    entries = load_manual_entries()
    entry = {
        "id": str(uuid.uuid4()),
        "map": map_id,
        "site": site,
        "date": date_str,
        "description": description,
        "start_time": start_time or "",
        "end_time": end_time or None,
        "observation": observation or "",
    }
    entries.append(entry)
    save_manual_entries(entries)
    return entry


def delete_manual_entry(entry_id):
    entries = load_manual_entries()
    entry_del = next((e for e in entries if e["id"] == entry_id), None)
    new_entries = [e for e in entries if e["id"] != entry_id]
    if len(new_entries) == len(entries):
        return False
    if entry_del:
        mover_para_lixeira(
            "manual_report_entry",
            f"{entry_del.get('site','?')}: {(entry_del.get('description') or '')[:60]}",
            entry_del
        )
    save_manual_entries(new_entries)
    return True


# --------------------------------------------------------------------------
# Resumo automatico de domingo - como ninguem fica de olho no fim de
# semana, o sistema manda um resumo pelo Telegram todo domingo as 8h com
# as falhas ocorridas de sabado 23h a domingo 8h. Opcao ligavel/desligavel
# em Configuracoes.
# --------------------------------------------------------------------------
SUNDAY_SUMMARY_FILE = os.path.join(BASE_DIR, "sunday_summary_config.json")
SUNDAY_SUMMARY_LOCK = threading.Lock()
_DEFAULT_SUNDAY_SUMMARY_CONFIG = {"enabled": False, "last_sent_date": None}


def load_sunday_summary_config():
    try:
        with open(SUNDAY_SUMMARY_FILE, encoding="utf-8") as f:
            data = json.load(f)
        for k, v in _DEFAULT_SUNDAY_SUMMARY_CONFIG.items():
            data.setdefault(k, v)
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        return dict(_DEFAULT_SUNDAY_SUMMARY_CONFIG)


def save_sunday_summary_config(data):
    with SUNDAY_SUMMARY_LOCK:
        tmp = SUNDAY_SUMMARY_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, SUNDAY_SUMMARY_FILE)


def build_sunday_summary_text(now=None):
    """Monta o texto do resumo com as falhas de sabado 23h a domingo 8h
    (sempre olhando para o fim de semana mais recente que ja passou)."""
    now = now or time.localtime()
    today_start = time.mktime((now.tm_year, now.tm_mon, now.tm_mday, 0, 0, 0, 0, 0, -1))
    window_start = today_start - 1 * 3600   # sabado 23h = domingo 00h menos 1 hora
    window_end = today_start + 8 * 3600     # domingo 8h = domingo 00h mais 8 horas

    dev_lookup = build_device_lookup()
    items = []
    for inc in INCIDENTS:
        if inc["start_ts"] < window_start or inc["start_ts"] >= window_end:
            continue
        dev_info = dev_lookup.get(inc.get("device_id"))
        if dev_info and dev_info.get("report_enabled") is False:
            continue
        start_txt = time.strftime("%H:%M", time.localtime(inc["start_ts"]))
        end_txt = time.strftime("%H:%M", time.localtime(inc["end_ts"])) if inc.get("end_ts") else "em aberto"
        site = inc.get("site") or "?"
        items.append(f"• [{site}] {inc.get('device_name','?')}: {inc.get('reason','')} ({start_txt}-{end_txt})")

    header = "RESUMO DE FIM DE SEMANA\n\nFalhas de sábado 23h às domingo 8h:\n\n"
    if not items:
        body = "Nenhuma ocorrência registrada nesse período. ✅"
    else:
        body = "\n".join(items) + f"\n\nTotal: {len(items)} ocorrência(s)."
    return header + body


def sunday_summary_loop():
    """Roda em segundo plano, verificando a cada minuto se e domingo por
    volta das 8h e se o resumo ainda nao foi enviado hoje."""
    while True:
        try:
            cfg = load_sunday_summary_config()
            if cfg.get("enabled"):
                now = time.localtime()
                today_str = time.strftime("%Y-%m-%d", now)
                # tm_wday: 0=segunda ... 6=domingo
                if now.tm_wday == 6 and now.tm_hour == 8 and cfg.get("last_sent_date") != today_str:
                    text = build_sunday_summary_text()
                    send_telegram_message(text)
                    cfg["last_sent_date"] = today_str
                    save_sunday_summary_config(cfg)
                    log_event("RESUMO DE DOMINGO ENVIADO", "Sistema", None)
        except Exception as e:
            print("[resumo de domingo] erro:", e, file=sys.stderr)
        time.sleep(60)


# --------------------------------------------------------------------------
# Resumo diario da Agenda (Planning) - manda uma mensagem no Telegram todo
# dia, num horario ajustavel, com as atividades planejadas pra aquele dia.
# Mesmo padrao do resumo de domingo (config em JSON, liga/desliga pela
# tela de Configuracoes), mas com horario configuravel em vez de fixo.
# --------------------------------------------------------------------------
AGENDA_SUMMARY_FILE = os.path.join(BASE_DIR, "agenda_summary_config.json")
AGENDA_SUMMARY_LOCK = threading.Lock()
_DEFAULT_AGENDA_SUMMARY_CONFIG = {"enabled": False, "hour": 7, "minute": 0, "skip_if_empty": False, "last_sent_date": None}
_PRIO_EMOJI = {"alta": "🔴", "media": "🟡", "baixa": "🟢"}


def load_agenda_summary_config():
    try:
        with open(AGENDA_SUMMARY_FILE, encoding="utf-8") as f:
            data = json.load(f)
        for k, v in _DEFAULT_AGENDA_SUMMARY_CONFIG.items():
            data.setdefault(k, v)
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        return dict(_DEFAULT_AGENDA_SUMMARY_CONFIG)


def save_agenda_summary_config(data):
    with AGENDA_SUMMARY_LOCK:
        tmp = AGENDA_SUMMARY_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, AGENDA_SUMMARY_FILE)


_DIAS_SEMANA_PT = ["segunda-feira", "terça-feira", "quarta-feira", "quinta-feira", "sexta-feira", "sábado", "domingo"]
_MESES_PT = ["", "janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho",
             "agosto", "setembro", "outubro", "novembro", "dezembro"]


def build_agenda_summary_text(now=None, day_offset=0):
    """Monta o texto com as atividades do Planning marcadas para o dia de
    hoje (concluidas aparecem riscadas, no fim da lista), ou pra amanha
    se day_offset=1 (usado pelo aviso "um dia antes")."""
    now = now or time.localtime()
    alvo = time.localtime(time.mktime(now) + day_offset * 86400)
    alvo_str = time.strftime("%Y-%m-%d", alvo)
    data_extenso = f"{alvo.tm_mday} de {_MESES_PT[alvo.tm_mon]}, {_DIAS_SEMANA_PT[alvo.tm_wday]}"
    titulo_dia = "AMANHÃ" if day_offset == 1 else "HOJE"
    sem_atividade_texto = "amanhã" if day_offset == 1 else "hoje"

    planning = load_planning_data()
    atividades_do_dia = [a for a in planning.get("activities", []) if a.get("data") == alvo_str]
    pendentes = [a for a in atividades_do_dia if a.get("status") != "concluida"]
    concluidas = [a for a in atividades_do_dia if a.get("status") == "concluida"]
    # prioridade alta primeiro
    ordem = {"alta": 0, "media": 1, "baixa": 2}
    pendentes.sort(key=lambda a: ordem.get(a.get("prioridade"), 1))

    header = f"📅 AGENDA DE {titulo_dia} — {data_extenso}\n\n"
    if not atividades_do_dia:
        return header + f"Nenhuma atividade planejada para {sem_atividade_texto}."

    linhas = []
    for a in pendentes:
        emoji = _PRIO_EMOJI.get(a.get("prioridade"), "⚪")
        meta = " · ".join(x for x in (a.get("local"), a.get("responsavel")) if x)
        linhas.append(f"{emoji} {a.get('titulo','(sem título)')}" + (f" — {meta}" if meta else ""))
    for a in concluidas:
        linhas.append(f"✅ {a.get('titulo','(sem título)')} (concluída)")

    resumo = f"\n\n{len(pendentes)} pendente(s), {len(concluidas)} concluída(s)."
    return header + "\n".join(linhas) + resumo


def agenda_summary_loop():
    """Roda em segundo plano, verificando a cada minuto se e a hora
    configurada e se o resumo do dia ainda nao foi enviado."""
    while True:
        try:
            cfg = load_agenda_summary_config()
            if cfg.get("enabled"):
                now = time.localtime()
                today_str = time.strftime("%Y-%m-%d", now)
                if (now.tm_hour == int(cfg.get("hour", 7)) and now.tm_min == int(cfg.get("minute", 0))
                        and cfg.get("last_sent_date") != today_str):
                    text = build_agenda_summary_text(now)
                    is_empty = "Nenhuma atividade planejada" in text
                    if not (is_empty and cfg.get("skip_if_empty")):
                        send_telegram_message(text)
                        log_event("RESUMO DE AGENDA ENVIADO", "Sistema", None)
                    cfg["last_sent_date"] = today_str
                    save_agenda_summary_config(cfg)
        except Exception as e:
            print("[resumo de agenda] erro:", e, file=sys.stderr)
        time.sleep(30)


# --------------------------------------------------------------------------
# Aviso da Agenda "um dia antes" - igual o resumo diario de hoje, mas
# manda a lista de atividades marcadas para AMANHA, num horario proprio
# (normalmente a tarde/noite do dia anterior), com liga/desliga proprio -
# independente do resumo de hoje, os dois convivem sem conflito.
# --------------------------------------------------------------------------
AGENDA_SUMMARY_AMANHA_FILE = os.path.join(BASE_DIR, "agenda_summary_amanha_config.json")
AGENDA_SUMMARY_AMANHA_LOCK = threading.Lock()
_DEFAULT_AGENDA_SUMMARY_AMANHA_CONFIG = {"enabled": False, "hour": 18, "minute": 0, "skip_if_empty": False, "last_sent_date": None}


def load_agenda_summary_amanha_config():
    try:
        with open(AGENDA_SUMMARY_AMANHA_FILE, encoding="utf-8") as f:
            data = json.load(f)
        for k, v in _DEFAULT_AGENDA_SUMMARY_AMANHA_CONFIG.items():
            data.setdefault(k, v)
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        return dict(_DEFAULT_AGENDA_SUMMARY_AMANHA_CONFIG)


def save_agenda_summary_amanha_config(data):
    with AGENDA_SUMMARY_AMANHA_LOCK:
        tmp = AGENDA_SUMMARY_AMANHA_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, AGENDA_SUMMARY_AMANHA_FILE)


def agenda_summary_amanha_loop():
    """Mesma logica do resumo de hoje, mas guarda o "last_sent_date" pela
    data de HOJE (dia do disparo), nao pela data da atividade - senao ele
    tentaria mandar de novo todo santo minuto depois de passar da hora,
    porque a atividade "de amanha" so vira "hoje" no dia seguinte."""
    while True:
        try:
            cfg = load_agenda_summary_amanha_config()
            if cfg.get("enabled"):
                now = time.localtime()
                today_str = time.strftime("%Y-%m-%d", now)
                if (now.tm_hour == int(cfg.get("hour", 18)) and now.tm_min == int(cfg.get("minute", 0))
                        and cfg.get("last_sent_date") != today_str):
                    text = build_agenda_summary_text(now, day_offset=1)
                    is_empty = "Nenhuma atividade planejada" in text
                    if not (is_empty and cfg.get("skip_if_empty")):
                        send_telegram_message(text)
                        log_event("AVISO DE AGENDA (AMANHA) ENVIADO", "Sistema", None)
                    cfg["last_sent_date"] = today_str
                    save_agenda_summary_amanha_config(cfg)
        except Exception as e:
            print("[aviso de agenda - amanha] erro:", e, file=sys.stderr)
        time.sleep(30)


# --------------------------------------------------------------------------
# Arquivo anual permanente - os incidentes automaticos so ficam guardados
# por 180 dias, entao para o Relatorio Anual continuar funcionando com o
# passar do tempo, arquivamos um RESUMO de cada mes (por mapa/site e por
# dispositivo) assim que ele fecha, de forma PERMANENTE (nunca e apagado).
# --------------------------------------------------------------------------
ANNUAL_ARCHIVE_FILE = os.path.join(BASE_DIR, "annual_archive.json")
ANNUAL_ARCHIVE_LOCK = threading.Lock()


def load_annual_archive():
    try:
        with open(ANNUAL_ARCHIVE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_annual_archive(data):
    with ANNUAL_ARCHIVE_LOCK:
        tmp = ANNUAL_ARCHIVE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, ANNUAL_ARCHIVE_FILE)


def archive_month_if_needed(year, month):
    """Se o mes indicado ainda nao estiver arquivado permanentemente,
    calcula o resumo dele (a partir dos incidentes ainda disponiveis) e
    guarda pra sempre - assim ele nao se perde quando os incidentes
    crus forem apagados apos 180 dias."""
    key = f"{year:04d}-{month:02d}"
    archive = load_annual_archive()
    if key in archive:
        return False

    report = build_monthly_report(year, month)
    device_summary = build_device_summary(year, month)
    total = sum(site_data["failure_count"] for m in report.values() for site_data in m["sites"].values())

    # so arquiva se o mes ja tiver fechado (nao arquiva o mes corrente,
    # que ainda esta em andamento) - compara com o mes/ano atual
    now = time.localtime()
    if year == now.tm_year and month == now.tm_mon:
        return False

    archive[key] = {
        "year": year, "month": month, "total_incidents": total,
        "maps": {
            map_id: {
                "name": data["name"],
                "sites": {site: {"failure_count": s["failure_count"]} for site, s in data["sites"].items()},
            }
            for map_id, data in report.items()
        },
        "device_summary": device_summary,
        "archived_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    save_annual_archive(archive)
    if total:
        log_event("MES ARQUIVADO PERMANENTEMENTE", key, None, detail=f"{total} ocorrencias")
    return True


def annual_archive_loop():
    """Roda em segundo plano, verificando periodicamente se algum mes
    fechado ainda precisa ser arquivado permanentemente (cobre o mes que
    acabou de passar, e qualquer mes anterior que por algum motivo ainda
    nao tenha sido arquivado, dentro da janela de retencao de 180 dias)."""
    while True:
        try:
            now = time.localtime()
            # tenta arquivar os ultimos 6 meses (cobre a janela de retencao
            # inteira, garantindo que nada fique pra tras)
            y, m = now.tm_year, now.tm_mon
            for _ in range(6):
                m -= 1
                if m == 0:
                    m = 12
                    y -= 1
                archive_month_if_needed(y, m)
        except Exception as e:
            print("[arquivo anual] erro:", e, file=sys.stderr)
        time.sleep(6 * 3600)  # confere a cada 6 horas


def build_annual_report(year):
    """Monta o relatorio anual (Janeiro a Dezembro) combinando o arquivo
    permanente (meses ja fechados e arquivados) com os dados ainda vivos
    dos incidentes (mes corrente e meses recentes dentro da janela de
    retencao que ainda nao foram arquivados)."""
    archive = load_annual_archive()
    now = time.localtime()
    month_names_pt = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
                       "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]

    months_out = []
    combined_device_totals = {}  # (map_id, site, device_name) -> {failures, minutes}
    combined_map_site_totals = {}  # map_id -> site -> failures

    for month in range(1, 13):
        key = f"{year:04d}-{month:02d}"
        is_future = (year > now.tm_year) or (year == now.tm_year and month > now.tm_mon)
        if is_future:
            months_out.append({"month": month, "name": month_names_pt[month-1], "total_incidents": None, "status": "futuro"})
            continue

        if year == now.tm_year and month == now.tm_mon:
            # mes corrente: calcula ao vivo
            report = build_monthly_report(year, month)
            device_summary = build_device_summary(year, month)
            total = sum(s["failure_count"] for m in report.values() for s in m["sites"].values())
            entry = {
                "total_incidents": total,
                "maps": {mid: {"name": d["name"], "sites": {s: {"failure_count": sd["failure_count"]} for s, sd in d["sites"].items()}} for mid, d in report.items()},
                "device_summary": device_summary,
            }
            source = "ao vivo (mes em andamento)"
        else:
            # tenta calcular AO VIVO primeiro - assim, se o usuario incluir/
            # excluir uma ocorrencia depois do mes ja ter passado, o relatorio
            # anual sempre reflete a escolha mais recente. So cai pro arquivo
            # permanente (congelado) quando os incidentes crus desse mes ja
            # saíram da janela de retencao de 180 dias (foram apagados).
            report = build_monthly_report(year, month)
            has_any_incident = any(s["incidents"] for m in report.values() for s in m["sites"].values())

            if has_any_incident:
                device_summary = build_device_summary(year, month)
                total = sum(s["failure_count"] for m in report.values() for s in m["sites"].values())
                entry = {
                    "total_incidents": total,
                    "maps": {mid: {"name": d["name"], "sites": {s: {"failure_count": sd["failure_count"]} for s, sd in d["sites"].items()}} for mid, d in report.items()},
                    "device_summary": device_summary,
                }
                source = "ao vivo (ainda dentro da janela de retencao)"
            elif key in archive:
                entry = archive[key]
                source = "arquivado"
            else:
                entry = {"total_incidents": None, "maps": {}, "device_summary": {}}
                source = "indisponivel (fora da janela de retencao e nao arquivado)"

        months_out.append({
            "month": month, "name": month_names_pt[month-1],
            "total_incidents": entry.get("total_incidents"),
            "status": source,
        })

        for map_id, map_data in entry.get("maps", {}).items():
            combined_map_site_totals.setdefault(map_id, {"name": map_data["name"], "sites": {}})
            for site, site_data in map_data.get("sites", {}).items():
                combined_map_site_totals[map_id]["sites"].setdefault(site, 0)
                combined_map_site_totals[map_id]["sites"][site] += site_data["failure_count"]

        for map_id, entries in entry.get("device_summary", {}).items():
            for e in entries:
                dkey = (map_id, e["site"], e["device_name"])
                acc = combined_device_totals.setdefault(dkey, {"map": map_id, "site": e["site"], "device_name": e["device_name"], "total_failures": 0, "total_minutes": 0.0})
                acc["total_failures"] += e["total_failures"]
                acc["total_minutes"] += e["total_minutes"]

    top_devices = sorted(combined_device_totals.values(), key=lambda e: -e["total_failures"])[:20]
    grand_total = sum(m["total_incidents"] for m in months_out if m["total_incidents"] is not None)

    return {
        "year": year,
        "months": months_out,
        "grand_total": grand_total,
        "map_site_totals": combined_map_site_totals,
        "top_devices": top_devices,
    }


def update_manual_entry_observation(entry_id, text):
    entries = load_manual_entries()
    for e in entries:
        if e["id"] == entry_id:
            e["observation"] = text
            save_manual_entries(entries)
            return True
    return False


INCIDENTS_MAX_AGE_S = 180 * 24 * 3600  # 180 dias

# --------------------------------------------------------------------------
# Horario de disparo de alarmes (por mapa) - fora da janela configurada,
# o sistema continua monitorando e registrando (logs/incidentes/historico)
# normalmente, mas NAO envia a notificacao no Telegram.
# --------------------------------------------------------------------------
ALARM_SCHEDULE_FILE = os.path.join(BASE_DIR, "alarm_schedule.json")
ALARM_SCHEDULE_LOCK = threading.Lock()
_DEFAULT_ALARM_SCHEDULE = {
    "interior": {"start": "00:00", "end": "23:59"},
    "fg": {"start": "00:00", "end": "23:59"},
}


def load_alarm_schedule():
    try:
        with open(ALARM_SCHEDULE_FILE, encoding="utf-8") as f:
            data = json.load(f)
        for key, default in _DEFAULT_ALARM_SCHEDULE.items():
            data.setdefault(key, default)
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        return dict(_DEFAULT_ALARM_SCHEDULE)


def save_alarm_schedule(data):
    with ALARM_SCHEDULE_LOCK:
        tmp = ALARM_SCHEDULE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, ALARM_SCHEDULE_FILE)


def _parse_hhmm(text, fallback=(0, 0)):
    try:
        h, m = text.split(":")
        return int(h), int(m)
    except Exception:
        return fallback


def is_within_alarm_window(map_name):
    schedule = load_alarm_schedule()
    cfg = schedule.get(map_name, _DEFAULT_ALARM_SCHEDULE.get(map_name, {"start": "00:00", "end": "23:59"}))
    start_h, start_m = _parse_hhmm(cfg.get("start", "00:00"), (0, 0))
    end_h, end_m = _parse_hhmm(cfg.get("end", "23:59"), (23, 59))

    now = time.localtime()
    now_minutes = now.tm_hour * 60 + now.tm_min
    start_minutes = start_h * 60 + start_m
    end_minutes = end_h * 60 + end_m

    if start_minutes <= end_minutes:
        return start_minutes <= now_minutes <= end_minutes
    else:
        # janela cruza a meia-noite (ex: 22:00 - 06:00)
        return now_minutes >= start_minutes or now_minutes <= end_minutes


REFRESH_INTERVAL = 30      # segundos entre rodadas de checagem
PING_TIMEOUT_S = 1.2
SNMP_TIMEOUT_S = 1.5

# --------------------------------------------------------------------------
# Editor de mapa (layout personalizado por mapa) - guarda posicoes,
# tamanhos, conexoes, rotulos de cidade e cores customizadas. Enquanto
# nao existir arquivo salvo para um mapa, ele continua sendo desenhado
# do jeito padrao (nada muda ate o usuario editar e salvar pelo menos
# uma vez na aba Configuracoes).
# --------------------------------------------------------------------------
LAYOUT_LOCK = threading.Lock()


def layout_file_for_map(map_name):
    safe_id = re.sub(r"[^a-z0-9_-]", "", map_name.lower())
    return os.path.join(BASE_DIR, f"layout_{safe_id}.json")

# --------------------------------------------------------------------------
# Modelo da mensagem enviada ao Telegram (editavel por mapa). Os textos
# padrao abaixo reproduzem exatamente o formato que ja estava sendo usado
# ("Telegran Grande Vitoria" do proprio Dude). Marcadores disponiveis:
#   {equipamento}  {status}  {ip}  {metrica}  {valor}
# O modelo de PING nao tem {metrica} nem {valor} (nao se aplicam).
# --------------------------------------------------------------------------
MESSAGE_TEMPLATE_FILE = os.path.join(BASE_DIR, "message_templates.json")
MESSAGE_TEMPLATE_LOCK = threading.Lock()

DEFAULT_PING_TEMPLATE = "ALERTA {abrigo}\n\n Ping \nEQUIPAMENTO: {equipamento}\nSTATUS: {status}\nIP={ip}"
DEFAULT_METRIC_TEMPLATE = "ALERTA {abrigo}\n\n {metrica} \nEQUIPAMENTO: {equipamento}\nSTATUS: {status}\nIP={ip}\nVALOR ATUAL: {valor}"

_DEFAULT_MESSAGE_TEMPLATES = {
    "interior": {"ping": DEFAULT_PING_TEMPLATE, "metric": DEFAULT_METRIC_TEMPLATE},
    "fg": {"ping": DEFAULT_PING_TEMPLATE, "metric": DEFAULT_METRIC_TEMPLATE},
}


def load_message_templates():
    try:
        with open(MESSAGE_TEMPLATE_FILE, encoding="utf-8") as f:
            data = json.load(f)
        for map_name, defaults in _DEFAULT_MESSAGE_TEMPLATES.items():
            entry = data.setdefault(map_name, {})
            entry.setdefault("ping", defaults["ping"])
            entry.setdefault("metric", defaults["metric"])
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        return json.loads(json.dumps(_DEFAULT_MESSAGE_TEMPLATES))  # copia profunda


def get_message_template_for_map(map_name):
    """Como load_message_templates(), mas funciona pra QUALQUER mapa
    (incluindo mapas novos que ainda nao tem nada customizado salvo -
    nesse caso cai no modelo padrao generico)."""
    templates = load_message_templates()
    return templates.get(map_name) or {"ping": DEFAULT_PING_TEMPLATE, "metric": DEFAULT_METRIC_TEMPLATE}


def save_message_templates(data):
    with MESSAGE_TEMPLATE_LOCK:
        tmp = MESSAGE_TEMPLATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, MESSAGE_TEMPLATE_FILE)


def render_message_template(template, **fields):
    # coloca um simbolo na frente da mensagem: vermelho quando for alarme,
    # verde quando o equipamento voltar ao normal - baseado no valor de
    # {status} que cada chamada ja preenche (UP/DOWN para ping,
    # ALARME/OK/SEM LEITURA SNMP para metricas).
    status_value = str(fields.get("status", "")).upper()
    alarm_values = {"DOWN", "ALARME", "SEM LEITURA SNMP", "SEM LEITURA MODBUS"}
    ok_values = {"UP", "OK"}
    if status_value in alarm_values:
        prefix = "\U0001F534 "  # circulo vermelho
    elif status_value in ok_values:
        prefix = "\u2705 "  # check verde
    else:
        prefix = ""

    # cabecalho por abrigo: modelos antigos ("ALERTA ABRIGOS", "ALERTA FONTE
    # GRANDE", "ALERTA GAZETA") passam a mostrar o abrigo de verdade
    if fields.get("abrigo"):
        template = re.sub(r"^(\s*)ALERTA (ABRIGOS|FONTE GRANDE|GAZETA|FG)\b", r"\1ALERTA {abrigo}", template)
    else:
        fields.setdefault("abrigo", "GAZETA")
    try:
        return prefix + template.format(**fields)
    except (KeyError, IndexError, ValueError):
        # marcador invalido no modelo customizado: cai para o padrao
        # correspondente em vez de travar o envio do alerta
        fallback = DEFAULT_PING_TEMPLATE if "metrica" not in fields else DEFAULT_METRIC_TEMPLATE
        return prefix + fallback.format(**fields)


def load_map_layout(map_name):
    path = layout_file_for_map(map_name)
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def save_map_layout(map_name, data):
    path = layout_file_for_map(map_name)
    with LAYOUT_LOCK:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    return True

MAX_WORKERS = 40  # antes 16 - com poucos "workers", um punhado de equipamentos
                   # lentos (ex: atras de enlace de radio instavel) prendiam os
                   # espacos disponiveis e atrasavam a leitura de todos os outros
                   # dispositivos na fila. Com mais workers, cada dispositivo tem
                   # sua propria "vez" quase ao mesmo tempo, sem esperar os lentos.

# --------------------------------------------------------------------------
# Notificacao Telegram - DESLIGADA por padrao.
# Para ligar, crie/edite o arquivo telegram_config.json (mesma pasta):
#   {"enabled": true, "bot_token": "123456:ABC...", "chat_id": "-100123..."}
# e reinicie o server.py. Assim o token nao fica gravado no codigo.
# --------------------------------------------------------------------------
TELEGRAM_CONFIG_FILE = os.path.join(BASE_DIR, "telegram_config.json")
TELEGRAM_ENABLED = False
TELEGRAM_BOT_TOKEN = ""
TELEGRAM_CHAT_ID = ""
try:
    with open(TELEGRAM_CONFIG_FILE, encoding="utf-8") as _tf:
        _tg = json.load(_tf)
    TELEGRAM_BOT_TOKEN = str(_tg.get("bot_token") or "").strip()
    TELEGRAM_CHAT_ID = str(_tg.get("chat_id") or "").strip()
    TELEGRAM_ENABLED = bool(_tg.get("enabled")) and bool(TELEGRAM_BOT_TOKEN) and bool(TELEGRAM_CHAT_ID)
except (FileNotFoundError, json.JSONDecodeError, OSError):
    pass
NOTIFY_CONFIRM_COUNT_DEFAULT = 8   # metricas SNMP: so notifica apos N leituras seguidas confirmando a mudanca
PING_CONFIRM_COUNT_DEFAULT = 12    # ping/conectividade: exige mais leituras seguidas (alarme mais robusto)
VISUAL_CONFIRM_COUNT_DEFAULT = 2   # cor da caixa/som no mapa: confirmacao mais rapida que o Telegram
VISUAL_CONFIRM_COUNT_MAX = 8

ALARM_CONFIRM_FILE = os.path.join(BASE_DIR, "alarm_confirm_config.json")
ALARM_CONFIRM_LOCK = threading.Lock()


def load_alarm_confirm_config():
    """Numero de leituras seguidas confirmando um problema antes de
    disparar o alarme de verdade - ajustavel em Configuracoes, pra dar
    mais ou menos margem contra alarme falso (oscilacao passageira de
    rede) sem precisar mexer no codigo."""
    try:
        with open(ALARM_CONFIRM_FILE, encoding="utf-8") as f:
            data = json.load(f)
        return {
            "metric_confirm_count": int(data.get("metric_confirm_count", NOTIFY_CONFIRM_COUNT_DEFAULT)),
            "ping_confirm_count": int(data.get("ping_confirm_count", PING_CONFIRM_COUNT_DEFAULT)),
            "visual_confirm_count": min(VISUAL_CONFIRM_COUNT_MAX, max(1, int(data.get("visual_confirm_count", VISUAL_CONFIRM_COUNT_DEFAULT)))),
        }
    except (FileNotFoundError, json.JSONDecodeError, ValueError, TypeError):
        return {
            "metric_confirm_count": NOTIFY_CONFIRM_COUNT_DEFAULT,
            "ping_confirm_count": PING_CONFIRM_COUNT_DEFAULT,
            "visual_confirm_count": VISUAL_CONFIRM_COUNT_DEFAULT,
        }


def save_alarm_confirm_config(metric_confirm_count, ping_confirm_count, visual_confirm_count=None):
    with ALARM_CONFIRM_LOCK:
        if visual_confirm_count is None:
            visual_confirm_count = load_alarm_confirm_config()["visual_confirm_count"]
        data = {
            "metric_confirm_count": int(metric_confirm_count),
            "ping_confirm_count": int(ping_confirm_count),
            "visual_confirm_count": min(VISUAL_CONFIRM_COUNT_MAX, max(1, int(visual_confirm_count))),
        }
        tmp = ALARM_CONFIRM_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, ALARM_CONFIRM_FILE)
        return data

# --------------------------------------------------------------------------
# Estado compartilhado (atualizado pela thread de polling, lido pelo HTTP)
# --------------------------------------------------------------------------
STATUS_LOCK = threading.Lock()
STATUS = {}          # ip -> {reachable, latency_ms, snmp_ok, snmp_value, snmp_display, updated_at}

# --------------------------------------------------------------------------
# Calculo de TAXA para metricas do tipo "contador" (ex: ifInOctets/Rx de
# portas de rede) - o SNMP so devolve o total acumulado de bytes desde que
# o equipamento ligou, entao pra mostrar Mbps de verdade precisamos guardar
# a leitura anterior de cada metrica e calcular a variacao ao longo do
# tempo: (atual - anterior) * 8 bits / segundos decorridos / 1e6 = Mbps.
# Trata tambem o "estouro" do contador de 32 bits (Counter32), que zera e
# recomeca de 0 ao passar de ~4.29 bilhoes.
# --------------------------------------------------------------------------
COUNTER_RATE_LOCK = threading.Lock()
COUNTER_RATE_STATE = {}   # (dev_id, label) -> {"value": raw_anterior, "ts": timestamp_anterior}
COUNTER32_MAX = 2 ** 32
MAX_PLAUSIBLE_RATE_MBPS = 10_000  # 10 Gbps - bem acima de qualquer feed de TV/radio esperado aqui


def compute_counter_rate_mbps(dev_id, label, raw_value, now):
    """Retorna a taxa em Mbps com base na variacao do contador desde a
    ultima leitura, ou None se ainda nao houver uma leitura anterior pra
    comparar (primeira amostra apos o servidor iniciar)."""
    key = (dev_id, label)
    with COUNTER_RATE_LOCK:
        prev = COUNTER_RATE_STATE.get(key)
        COUNTER_RATE_STATE[key] = {"value": raw_value, "ts": now}

    if not prev:
        return None
    dt = now - prev["ts"]
    if dt < 2:
        # tempo entre leituras muito curto (ex: releitura imediata disparada
        # ao salvar uma edicao no Config, poucos instantes depois do ciclo
        # normal de coleta) - dividir por um dt quase zero gera taxas
        # absurdas mesmo com uma variacao normal do contador, entao ignora
        # essa amostra em vez de arriscar um valor errado
        return None

    delta = raw_value - prev["value"]
    if delta < 0:
        # contador de 32 bits reiniciou (estourou) - soma o ciclo completo
        delta += COUNTER32_MAX
        if delta < 0 or delta > COUNTER32_MAX:
            return None  # variacao implausivel (ex: dispositivo trocado/reiniciado)

    rate = (delta * 8) / dt / 1_000_000
    if rate > MAX_PLAUSIBLE_RATE_MBPS:
        # numero implausivel pra esse tipo de link (ex: o contador do
        # equipamento reiniciou/zerou de verdade, em vez de ter dado a
        # volta por estouro - nesse caso a "correcao de estouro" acima
        # inventa uma diferenca gigante) - descarta em vez de mostrar
        # um valor absurdo, e esta amostra passa a servir de base pra
        # a proxima comparacao
        return None
    return rate

LAST_CYCLE_TS = 0

# --------------------------------------------------------------------------
# Historico (grafico) - guardado em memoria E persistido em disco
# periodicamente, pra sobreviver a reinicios do servidor. Mantem 7 dias
# de retencao: pontos das ultimas 24h ficam com a granularidade normal
# do polling (a cada ciclo), pontos mais antigos que isso sao compactados
# pra 1 ponto a cada 10 minutos, senao o arquivo cresceria sem limite com
# centenas de metricas monitoradas.
# --------------------------------------------------------------------------
HISTORY_LOCK = threading.Lock()
HISTORY = {}   # dev_id -> {"latency": [[ts,val],...], "metrics": {label: [[ts,val],...]}}
HISTORY_MAX_AGE_S = 7 * 24 * 3600      # 7 dias
HISTORY_FULL_RES_WINDOW_S = 24 * 3600  # mantem granularidade total nas ultimas 24h
HISTORY_BUCKET_S = 10 * 60             # fora dessa janela, compacta pra 1 ponto/10min
HISTORY_FILE = os.path.join(BASE_DIR, "history_7d.json")
HISTORY_SAVE_INTERVAL_S = 5 * 60       # salva em disco a cada 5 minutos


def _downsample_series(series, now):
    """Mantem granularidade total nas ultimas 24h; fora disso, guarda so
    1 ponto por bucket de 10 minutos (o mais recente de cada bucket) -
    limita o tamanho do historico de 7 dias sem perder a tendencia geral."""
    cutoff_full = now - HISTORY_FULL_RES_WINDOW_S
    cutoff_max = now - HISTORY_MAX_AGE_S
    recent = [p for p in series if p[0] >= cutoff_full]
    older = [p for p in series if cutoff_max <= p[0] < cutoff_full]

    buckets = {}
    for ts, val in older:
        bucket_key = int(ts // HISTORY_BUCKET_S)
        buckets[bucket_key] = [ts, val]  # sobrescreve - fica so o ultimo de cada bucket
    compacted_older = sorted(buckets.values(), key=lambda p: p[0])

    return compacted_older + recent


HISTORY_MIN_STEP_S = 30   # leitura pode ser de 5 s; o grafico guarda 1 ponto a cada 30 s
_HISTORY_LAST_TS = {}


def record_history(dev_id, result):
    now = result.get("updated_at", time.time())
    if now - _HISTORY_LAST_TS.get(dev_id, 0) < HISTORY_MIN_STEP_S:
        return
    _HISTORY_LAST_TS[dev_id] = now
    with HISTORY_LOCK:
        h = HISTORY.setdefault(dev_id, {"latency": [], "metrics": {}})

        if result.get("latency_ms") is not None:
            h["latency"].append([now, result["latency_ms"]])
            if len(h["latency"]) % 50 == 0:  # poda/compactacao periodica, nao a cada ponto
                h["latency"] = _downsample_series(h["latency"], now)

        for label, entry in (result.get("metrics") or {}).items():
            scaled = entry.get("scaled")
            if scaled is None:
                continue
            series = h["metrics"].setdefault(label, [])
            series.append([now, scaled])
            if len(series) % 50 == 0:
                h["metrics"][label] = _downsample_series(series, now)


def load_history_from_disk():
    global HISTORY
    try:
        with open(HISTORY_FILE, encoding="utf-8") as f:
            raw = json.load(f)
        # as chaves de dev_id sao salvas como string no JSON - converte de volta pra int
        HISTORY = {int(k): v for k, v in raw.items()}
        print(f"[historico] carregado do disco: {len(HISTORY)} dispositivos")
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        HISTORY = {}


def save_history_to_disk():
    with HISTORY_LOCK:
        # compacta tudo antes de salvar, pra nao gravar pontos que ja
        # deveriam ter sido reduzidos desde a ultima leitura
        now = time.time()
        snapshot = {}
        for dev_id, h in HISTORY.items():
            snapshot[dev_id] = {
                "latency": _downsample_series(h["latency"], now),
                "metrics": {label: _downsample_series(series, now) for label, series in h["metrics"].items()},
            }
    tmp = HISTORY_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(snapshot, f)
    os.replace(tmp, HISTORY_FILE)


def history_autosave_loop():
    while True:
        time.sleep(HISTORY_SAVE_INTERVAL_S)
        try:
            save_history_to_disk()
        except Exception as e:
            log_event("ERRO AO SALVAR HISTORICO", str(e), None)


# --------------------------------------------------------------------------
# Log de eventos (ultimos 1000, gravados em arquivo texto de facil leitura)
# --------------------------------------------------------------------------
EVENTS_LOCK = threading.Lock()
EVENTS = deque(maxlen=EVENTS_MAX)  # cada item: dict com ts, tipo, dispositivo, ip, detalhe


def _format_event_line(ev):
    ts_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ev["ts"]))
    parts = [f"[{ts_str}]", f"{ev['type']:<12}", "|", ev["device"]]
    if ev.get("detail"):
        parts += ["|", ev["detail"]]
    if ev.get("ip"):
        parts += ["|", f"IP={ev['ip']}"]
    return " ".join(parts)


def _rewrite_events_file():
    try:
        lines = [_format_event_line(ev) for ev in EVENTS]
        with open(EVENTS_LOG_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + ("\n" if lines else ""))
    except Exception as e:
        print("[events] erro ao gravar eventos.log:", e, file=sys.stderr)


def log_event(event_type, device_name, ip=None, detail=None):
    ev = {
        "ts": time.time(),
        "type": event_type,
        "device": device_name,
        "ip": ip,
        "detail": detail,
    }
    with EVENTS_LOCK:
        EVENTS.append(ev)
        _rewrite_events_file()


# --------------------------------------------------------------------------
# Incidentes (dashboard de falhas dos ultimos 30 dias)
# Cada falha vira um "incidente": abre quando a leitura confirma o problema,
# fica atualizando enquanto persistir (mesmo que troque entre alarme/erro),
# e fecha quando o dispositivo volta ao normal - com duracao calculada.
# Persistido em arquivo (sobrevive a reinicio do servidor).
# --------------------------------------------------------------------------
INCIDENTS_LOCK = threading.Lock()
INCIDENTS = []


def load_incidents():
    global INCIDENTS
    try:
        with open(INCIDENTS_FILE, encoding="utf-8") as f:
            INCIDENTS = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        INCIDENTS = []
    changed = False
    for inc in INCIDENTS:
        if "id" not in inc:
            inc["id"] = str(uuid.uuid4())
            changed = True
    if changed:
        _save_incidents()


def _save_incidents():
    try:
        tmp = INCIDENTS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(INCIDENTS, f, ensure_ascii=False, indent=2)
        os.replace(tmp, INCIDENTS_FILE)
    except Exception as e:
        print("[incidents] erro ao gravar incidentes.json:", e, file=sys.stderr)


def _prune_incidents():
    global INCIDENTS
    cutoff = time.time() - INCIDENTS_MAX_AGE_S
    INCIDENTS = [i for i in INCIDENTS if (i.get("end_ts") or time.time()) >= cutoff]


def open_or_update_incident(device, kind, label, reason, value_display):
    with INCIDENTS_LOCK:
        existing = next(
            (i for i in INCIDENTS
             if i["device_id"] == device.get("id") and i["kind"] == kind
             and i["label"] == label and i["end_ts"] is None),
            None,
        )
        if existing:
            existing["reason"] = reason
            existing["value_at_failure"] = value_display
        else:
            INCIDENTS.append({
                "id": str(uuid.uuid4()),
                "device_id": device.get("id"),
                "device_name": device.get("name"),
                "site": device.get("site"),
                "ip": device.get("ip"),
                "kind": kind,
                "label": label,
                "reason": reason,
                "value_at_failure": value_display,
                "start_ts": time.time(),
                "end_ts": None,
            })
        _prune_incidents()
        _save_incidents()


def close_incident(device_id, kind, label):
    with INCIDENTS_LOCK:
        for inc in reversed(INCIDENTS):
            if (inc["device_id"] == device_id and inc["kind"] == kind
                    and inc["label"] == label and inc["end_ts"] is None):
                inc["end_ts"] = time.time()
                break
        _save_incidents()


def build_device_lookup():
    """Monta um dicionario device_id -> {name, site, map, report_enabled}
    combinando Interior, FG e todos os mapas customizados - usado pelo
    relatorio mensal para saber de qual mapa cada dispositivo e, e se ele
    deve ou nao entrar no relatorio."""
    lookup = {}
    for dev in load_devices().get("devices", []):
        lookup[dev["id"]] = {
            "name": dev.get("name"), "site": dev.get("site"),
            "map": "interior", "report_enabled": dev.get("report_enabled", True),
        }
    for dev in load_devices_fg().get("devices", []):
        lookup[dev["id"]] = {
            "name": dev.get("name"), "site": dev.get("site") or dev.get("category"),
            "map": "fg", "report_enabled": dev.get("report_enabled", True),
        }
    for map_id, entry in load_maps_registry().items():
        if entry.get("kind") != "custom":
            continue
        for dev in load_devices_generic(map_id).get("devices", []):
            lookup[dev["id"]] = {
                "name": dev.get("name"), "site": dev.get("site") or dev.get("category"),
                "map": map_id, "report_enabled": dev.get("report_enabled", True),
            }
    return lookup


def build_events_for_site(site, map_id=None, limit=15):
    """Retorna os eventos mais recentes de dispositivos que pertencem a
    uma cidade/categoria especifica - usado no popup que aparece ao
    passar o mouse sobre o nome da cidade/categoria no mapa."""
    lookup = build_device_lookup()
    matching_names = set()
    for dev_id, info in lookup.items():
        if info.get("site") == site and (map_id is None or info.get("map") == map_id):
            if info.get("name"):
                matching_names.add(info["name"])

    if not matching_names:
        return []

    results = []
    with EVENTS_LOCK:
        for ev in reversed(EVENTS):
            if ev.get("device") in matching_names:
                results.append(ev)
                if len(results) >= limit:
                    break
    return results


def build_report_device_list():
    """Lista todos os dispositivos de todos os mapas, agrupados por mapa,
    com o estado atual de inclusao no relatorio - para a tela de gestao
    'Dispositivos incluidos no relatorio'."""
    result = {}
    interior = load_devices().get("devices", [])
    result["interior"] = {
        "name": "Abrigos",
        "devices": [{"id": d["id"], "name": d["name"], "site": d.get("site"),
                     "report_enabled": d.get("report_enabled", True)} for d in interior],
    }
    fg = load_devices_fg().get("devices", [])
    result["fg"] = {
        "name": "Abrigo Fonte Grande",
        "devices": [{"id": d["id"], "name": d["name"], "site": d.get("site") or d.get("category"),
                     "report_enabled": d.get("report_enabled", True)} for d in fg],
    }
    for map_id, entry in load_maps_registry().items():
        if entry.get("kind") != "custom":
            continue
        devs = load_devices_generic(map_id).get("devices", [])
        result[map_id] = {
            "name": entry.get("name", map_id),
            "devices": [{"id": d["id"], "name": d["name"], "site": d.get("site") or d.get("category"),
                         "report_enabled": d.get("report_enabled", True)} for d in devs],
        }
    return result


def build_monthly_report(year, month):
    """Monta o relatorio mensal de ocorrencias, agrupado por cidade/site -
    mesmo formato da planilha manual (Data / Descricao / Horario de Inicio /
    Horario de Normalizacao / Observacao), preenchido automaticamente a
    partir dos incidentes reais registrados pelo sistema (Interior + FG)."""
    if month == 12:
        next_year, next_month = year + 1, 1
    else:
        next_year, next_month = year, month + 1
    start_ts = time.mktime((year, month, 1, 0, 0, 0, 0, 0, -1))
    end_ts = time.mktime((next_year, next_month, 1, 0, 0, 0, 0, 0, -1))

    observations = load_observations()
    excluded_ids = load_excluded_incidents()
    dev_lookup = build_device_lookup()

    # estrutura: mapa -> site -> lista de ocorrencias
    by_map_site = {}

    for inc in INCIDENTS:
        if inc["start_ts"] < start_ts or inc["start_ts"] >= end_ts:
            continue
        dev_info = dev_lookup.get(inc.get("device_id"))
        if dev_info and dev_info.get("report_enabled") is False:
            continue  # dispositivo excluido do relatorio manualmente

        map_id = dev_info.get("map") if dev_info else "interior"
        site = inc.get("site") or "Sem site definido"
        start_local = time.localtime(inc["start_ts"])
        row = {
            "id": inc["id"],
            "date": time.strftime("%Y-%m-%d", start_local),
            "date_display": time.strftime("%d/%m/%Y", start_local),
            "description": f"{inc.get('device_name','?')}: {inc.get('reason','')}",
            "start_time": time.strftime("%H:%M", start_local),
            "end_time": time.strftime("%H:%M", time.localtime(inc["end_ts"])) if inc.get("end_ts") else None,
            "ongoing": inc.get("end_ts") is None,
            "observation": observations.get(inc["id"], ""),
            "excluded": inc["id"] in excluded_ids,
        }
        by_map_site.setdefault(map_id, {}).setdefault(site, []).append(row)

    # mescla as ocorrencias adicionadas MANUALMENTE (localidades sem
    # equipamento monitorado, tipo Sao Mateus/Castelo/etc) que caem
    # dentro do mes selecionado
    for entry in load_manual_entries():
        try:
            entry_date = time.strptime(entry["date"], "%Y-%m-%d")
        except (ValueError, KeyError):
            continue
        entry_ts = time.mktime(entry_date)
        if entry_ts < start_ts or entry_ts >= end_ts:
            continue
        row = {
            "id": entry["id"],
            "date": entry["date"],
            "date_display": time.strftime("%d/%m/%Y", entry_date),
            "description": entry["description"],
            "start_time": entry.get("start_time", ""),
            "end_time": entry.get("end_time"),
            "ongoing": not entry.get("end_time"),
            "observation": entry.get("observation", ""),
            "is_manual": True,
            "excluded": entry["id"] in excluded_ids,
        }
        by_map_site.setdefault(entry["map"], {}).setdefault(entry["site"], []).append(row)

    # inclui tambem sites/mapas que existem no sistema mas nao tiveram
    # nenhuma ocorrencia no mes (aparecem como "Sem Ocorrencias")
    map_names = {"interior": "Abrigos", "fg": "Abrigo Fonte Grande"}
    for map_id, entry in load_maps_registry().items():
        if entry.get("kind") == "custom":
            map_names[map_id] = entry.get("name", map_id)

    all_sites_by_map = {}
    for dev_id, info in dev_lookup.items():
        if info.get("report_enabled") is False:
            continue
        if info.get("site"):
            all_sites_by_map.setdefault(info["map"], set()).add(info["site"])

    # localidades monitoradas em outros PCs: sempre aparecem no Mapa
    # Interior, mesmo sem nenhum dispositivo real no sistema
    all_sites_by_map.setdefault("interior", set()).update(EXTRA_INTERIOR_SITES)

    for map_id, site_set in all_sites_by_map.items():
        for site in site_set:
            by_map_site.setdefault(map_id, {}).setdefault(site, [])

    for map_sites in by_map_site.values():
        for site_rows in map_sites.values():
            site_rows.sort(key=lambda r: (r["date"], r["start_time"] or ""))

    # monta o resultado final, com nome do mapa, sites ordenados e
    # contagem de falhas por site (pra mostrar em destaque)
    result = {}
    for map_id, sites in sorted(by_map_site.items(), key=lambda kv: (kv[0] != "interior", kv[0] != "fg", kv[0])):
        sites_out = {}
        for site_name, rows in sorted(sites.items()):
            active_count = sum(1 for r in rows if not r.get("excluded"))
            sites_out[site_name] = {"failure_count": active_count, "incidents": rows}
        result[map_id] = {"name": map_names.get(map_id, map_id), "sites": sites_out}

    return result


def build_powerbi_rows(year=None, month=None):
    """Monta os incidentes como uma tabela PLANA (uma linha por ocorrencia,
    sem agrupar por cidade) - formato ideal para o conector 'Web' do Power
    BI, que espera uma lista de registros com os mesmos campos, e nao uma
    estrutura aninhada por site.

    Se year/month forem omitidos, retorna TODOS os incidentes disponiveis
    (ate 180 dias, conforme a retencao) - uso recomendado para conectar o
    Power BI uma unica vez e deixar ele atualizar sozinho, filtrando por
    data dentro do proprio Power BI."""
    observations = load_observations()
    excluded_ids = load_excluded_incidents()
    dev_lookup = build_device_lookup()

    if year is not None and month is not None:
        if month == 12:
            next_year, next_month = year + 1, 1
        else:
            next_year, next_month = year, month + 1
        start_ts = time.mktime((year, month, 1, 0, 0, 0, 0, 0, -1))
        end_ts = time.mktime((next_year, next_month, 1, 0, 0, 0, 0, 0, -1))
    else:
        start_ts, end_ts = None, None

    rows = []
    for inc in INCIDENTS:
        if start_ts is not None and (inc["start_ts"] < start_ts or inc["start_ts"] >= end_ts):
            continue
        if inc["id"] in excluded_ids:
            continue
        dev_info = dev_lookup.get(inc.get("device_id"))
        if dev_info and dev_info.get("report_enabled") is False:
            continue
        start_local = time.localtime(inc["start_ts"])
        rows.append({
            "id": inc["id"],
            "map": dev_info.get("map") if dev_info else "interior",
            "site": inc.get("site") or "Sem site definido",
            "device_name": inc.get("device_name", "?"),
            "ip": inc.get("ip"),
            "kind": inc.get("kind"),
            "label": inc.get("label"),
            "reason": inc.get("reason", ""),
            "value_at_failure": inc.get("value_at_failure"),
            "date": time.strftime("%Y-%m-%d", start_local),
            "start_datetime": time.strftime("%Y-%m-%d %H:%M:%S", start_local),
            "end_datetime": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(inc["end_ts"])) if inc.get("end_ts") else None,
            "ongoing": inc.get("end_ts") is None,
            "duration_minutes": round((( inc.get("end_ts") or time.time()) - inc["start_ts"]) / 60, 1),
            "observation": observations.get(inc["id"], ""),
        })

    rows.sort(key=lambda r: r["start_datetime"])
    return rows


def build_device_summary(year, month):
    """Resumo por dispositivo no mes: total de falhas, tempo total fora do
    ar/em alarme, e as observacoes ja escritas para aquele periodo -
    agrupado por mapa (Interior/FG/mapas novos), respeitando dispositivos
    excluidos do relatorio."""
    if month == 12:
        next_year, next_month = year + 1, 1
    else:
        next_year, next_month = year, month + 1
    start_ts = time.mktime((year, month, 1, 0, 0, 0, 0, 0, -1))
    end_ts = time.mktime((next_year, next_month, 1, 0, 0, 0, 0, 0, -1))
    observations = load_observations()
    excluded_ids = load_excluded_incidents()
    dev_lookup = build_device_lookup()

    by_map = {}
    for inc in INCIDENTS:
        if inc["start_ts"] < start_ts or inc["start_ts"] >= end_ts:
            continue
        if inc["id"] in excluded_ids:
            continue  # ocorrencia excluida manualmente do relatorio
        dev_info = dev_lookup.get(inc.get("device_id"))
        if dev_info and dev_info.get("report_enabled") is False:
            continue
        map_id = dev_info.get("map") if dev_info else "interior"

        key = (inc.get("site") or "Sem site definido", inc.get("device_name") or "?")
        by_device = by_map.setdefault(map_id, {})
        entry = by_device.setdefault(key, {
            "site": key[0], "device_name": key[1],
            "total_failures": 0, "total_minutes": 0.0, "observations": [],
        })
        entry["total_failures"] += 1
        end_ref = inc.get("end_ts") or time.time()
        entry["total_minutes"] += max((end_ref - inc["start_ts"]) / 60, 0)
        obs = observations.get(inc["id"])
        if obs:
            entry["observations"].append(obs)

    result = {}
    for map_id, by_device in by_map.items():
        result[map_id] = sorted(by_device.values(), key=lambda e: (e["site"], e["device_name"]))
    return result


# --------------------------------------------------------------------------
# Exclusao PROPRIA do relatorio "Tempo Fora do Ar - Transmissores" - de
# proposito SEPARADA da lista de excluidos do relatorio mensal/logs (essa
# combinacao foi o que causou aquele bug de tabela vazia antes: exclusoes
# feitas por outro motivo escondiam quedas de transmissor sem querer). Aqui
# quem exclui um incidente so afeta ESSE relatorio especifico.
# --------------------------------------------------------------------------
DOWNTIME_EXCLUDED_FILE = os.path.join(BASE_DIR, "downtime_excluded_incidents.json")
DOWNTIME_EXCLUDED_LOCK = threading.Lock()


def load_downtime_excluded():
    try:
        with open(DOWNTIME_EXCLUDED_FILE, encoding="utf-8") as f:
            return set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


def save_downtime_excluded(ids_set):
    with DOWNTIME_EXCLUDED_LOCK:
        tmp = DOWNTIME_EXCLUDED_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(sorted(ids_set), f, ensure_ascii=False, indent=2)
        os.replace(tmp, DOWNTIME_EXCLUDED_FILE)


def build_transmitter_downtime(year):
    """Calcula quanto tempo os TRANSMISSORES (kind='tx' no Interior,
    category='TRANSMISSORES' na FG) ficaram REALMENTE fora do ar (sem
    responder ping) no ano pedido, com duas regras extras pedidas:

    1) Ignora quedas mais curtas que MIN_DOWNTIME_MINUTES - essas sao
       consideradas oscilacao passageira de ping/SNMP (rede piscou,
       um pacote se perdeu), nao uma queda de verdade do transmissor.
       Isso e ALEM da confirmacao por N leituras seguidas que ja existe
       antes de um incidente ser aberto (ping_confirm_count) - aqui e
       um filtro de DURACAO TOTAL do incidente, nao de confirmacao.

    2) Classifica cada queda como "energia" ou "tecnico", cruzando o
       horario da queda do TX com alarmes de "Rede AC" (Interior) ou
       Fase R/S/T / Fonte de Saida (FG) nos nobreaks (kind='ups' /
       category='NOBREAK') do MESMO local, dentro de uma tolerancia de
       tempo - se a rede eletrica tambem deu problema nesse intervalo,
       classifica como "energia"; senao, como "tecnico" (defeito no
       proprio equipamento, cabo, clima, etc).

    So cobre incidentes ainda dentro da janela de retencao (INCIDENTS em
    memoria) - meses mais antigos que ja saíram dessa janela e nao tem
    esse detalhe no arquivo permanente nao entram na conta."""
    MIN_DOWNTIME_MINUTES = 6  # abaixo disso, conta como oscilacao passageira
    OVERLAP_TOLERANCE_S = 5 * 60  # tolerancia pra casar o horario da queda de energia com a do TX

    dev_lookup = build_device_lookup()
    interior_devices = load_devices().get("devices", [])
    fg_devices = load_devices_fg().get("devices", [])

    transmitter_ids = set()
    for dev in interior_devices:
        if dev.get("kind") == "tx":
            transmitter_ids.add(dev["id"])
    for dev in fg_devices:
        if dev.get("category") == "TRANSMISSORES":
            transmitter_ids.add(dev["id"])
    for map_id, entry in load_maps_registry().items():
        if entry.get("kind") != "custom":
            continue
        for dev in load_devices_generic(map_id).get("devices", []):
            if dev.get("category") == "TRANSMISSORES" or dev.get("kind") == "tx":
                transmitter_ids.add(dev["id"])

    # nobreaks - usados so pra achar quedas de rede eletrica correlacionadas
    ups_ids_interior = {dev["id"] for dev in interior_devices if dev.get("kind") == "ups"}
    ups_ids_fg = {dev["id"] for dev in fg_devices if dev.get("category") == "NOBREAK"}

    # ------- janelas de queda de energia por local -------
    # Interior: uma lista de (start,end) POR MUNICIPIO (mesmo "site" do TX).
    # FG: uma lista unica com chave fixa "FG" (os TX da FG nao tem
    # municipio proprio - sao so um grupo, "TRANSMISSORES").
    power_windows_by_site = {}
    for inc in INCIDENTS:
        if inc.get("kind") != "metric":
            continue
        dev_id = inc.get("device_id")
        site_key = None
        if dev_id in ups_ids_interior and inc.get("label") == "Rede AC":
            site_key = inc.get("site")
        elif dev_id in ups_ids_fg and inc.get("label") in ("Fase R", "Fase S", "Fase T", "Fonte Saída"):
            site_key = "FG"
        if not site_key:
            continue
        end_ref = inc.get("end_ts") or time.time()
        power_windows_by_site.setdefault(site_key, []).append((inc["start_ts"], end_ref))

    def houve_queda_de_energia(site_key, start, end):
        for w_start, w_end in power_windows_by_site.get(site_key, []):
            if (start - OVERLAP_TOLERANCE_S) <= w_end and (end + OVERLAP_TOLERANCE_S) >= w_start:
                return True
        return False

    start_ts = time.mktime((year, 1, 1, 0, 0, 0, 0, 0, -1))
    end_ts = time.mktime((year + 1, 1, 1, 0, 0, 0, 0, 0, -1))

    # tempo TOTAL de falta de energia ja registrado nos nobreaks daquele
    # local NESTE ANO (independente de ter ou nao coincidido com uma queda
    # do transmissor) - e o numero que aparece na coluna "Falta de Energia
    # Registrada" da tabela, pedido separado do tempo fora do ar do TX.
    power_registered_minutes_by_site = {}
    for site_key, janelas in power_windows_by_site.items():
        for w_start, w_end in janelas:
            if w_start < start_ts or w_start >= end_ts:
                continue
            power_registered_minutes_by_site[site_key] = power_registered_minutes_by_site.get(site_key, 0.0) + max((w_end - w_start) / 60, 0)

    oldest_incident_ts = None
    ignored_short = 0
    ignored_open = 0
    ignored_manual = 0
    by_group = {}
    cause_totals = {
        "energia": {"minutes": 0.0, "quedas": 0},
        "tecnico": {"minutes": 0.0, "quedas": 0},
    }
    downtime_excluded = load_downtime_excluded()

    for inc in INCIDENTS:
        if inc.get("kind") != "ping":
            continue
        if inc["device_id"] not in transmitter_ids:
            continue
        # Nota: NAO respeita a lista de "excluidos" do relatorio mensal/logs
        # de proposito - esse relatorio mede tempo real de indisponibilidade,
        # que e um proposito diferente de "quero que isso nao conte como
        # falha no relatorio mensal". Uma queda excluida dali ainda e uma
        # queda de verdade pra essa conta aqui. Em vez disso, usa sua PROPRIA
        # lista de exclusao (botao de excluir direto nessa tabela).
        if inc["id"] in downtime_excluded:
            ignored_manual += 1
            continue
        if oldest_incident_ts is None or inc["start_ts"] < oldest_incident_ts:
            oldest_incident_ts = inc["start_ts"]
        if inc["start_ts"] < start_ts or inc["start_ts"] >= end_ts:
            continue

        if inc.get("end_ts") is None:
            # ainda em andamento (sem confirmacao de quando voltou) - pode
            # ser uma manobra/reset que ainda vai se resolver rapido, entao
            # so entra na conta quando fechar de vez (fica de fora por
            # enquanto, sem risco de subestimar uma queda passageira como
            # se fosse uma queda real e prolongada)
            ignored_open += 1
            continue

        end_ref = inc["end_ts"]
        duration_min = max((end_ref - inc["start_ts"]) / 60, 0)
        if duration_min < MIN_DOWNTIME_MINUTES:
            ignored_short += 1
            continue  # oscilacao passageira de ping/SNMP - nao conta

        dev_info = dev_lookup.get(inc.get("device_id"))
        if dev_info and dev_info.get("report_enabled") is False:
            continue
        map_id = dev_info.get("map") if dev_info else "interior"
        site = inc.get("site") or (dev_info.get("site") if dev_info else None) or "Fonte Grande"
        power_site_key = site if map_id == "interior" else "FG"

        causa = "energia" if houve_queda_de_energia(power_site_key, inc["start_ts"], end_ref) else "tecnico"

        key = (map_id, site)
        entry = by_group.setdefault(key, {
            "map": map_id, "site": site, "total_minutes": 0.0, "quedas": 0,
            "energia_minutes": 0.0, "energia_quedas": 0,
            "tecnico_minutes": 0.0, "tecnico_quedas": 0,
            "energia_registrada_minutes": power_registered_minutes_by_site.get(power_site_key, 0.0),
            "incidentes": [],
        })
        entry["total_minutes"] += duration_min
        entry["quedas"] += 1
        entry[causa + "_minutes"] += duration_min
        entry[causa + "_quedas"] += 1
        entry["incidentes"].append({
            "id": inc["id"],
            "device_name": inc.get("device_name") or (dev_info.get("name") if dev_info else None) or "?",
            "start_ts": inc["start_ts"],
            "end_ts": inc.get("end_ts"),
            "duration_min": duration_min,
            "causa": causa,
        })

        cause_totals[causa]["minutes"] += duration_min
        cause_totals[causa]["quedas"] += 1

    for entry in by_group.values():
        entry["incidentes"].sort(key=lambda i: -i["start_ts"])

    groups = sorted(by_group.values(), key=lambda e: -e["total_minutes"])
    return {
        "year": year,
        "groups": groups,
        "cause_totals": cause_totals,
        "data_since": oldest_incident_ts,
        "min_downtime_minutes": MIN_DOWNTIME_MINUTES,
        "ignored_short_count": ignored_short,
        "ignored_open_count": ignored_open,
        "ignored_manual_count": ignored_manual,
    }


def export_monthly_report_pdf(year, month):
    """Gera o relatorio mensal em PDF: resumo por dispositivo (total de
    falhas, tempo fora do ar, observacoes) seguido do detalhamento por
    mapa (Interior / FG / mapas novos) e cidade - facil de ler, imprimir
    e compartilhar."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import cm
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from io import BytesIO

    month_names_pt = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
                       "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]

    report = build_monthly_report(year, month)
    device_summary = build_device_summary(year, month)
    total_incidents = sum(
        site_data["failure_count"]
        for map_data in report.values()
        for site_data in map_data["sites"].values()
    )

    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
                             topMargin=1.6*cm, bottomMargin=1.6*cm,
                             leftMargin=1.6*cm, rightMargin=1.6*cm)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("TituloRel", parent=styles["Title"], fontSize=17, spaceAfter=4)
    sub_style = ParagraphStyle("SubRel", parent=styles["Normal"], fontSize=10, textColor=colors.grey)
    section_style = ParagraphStyle("SecaoRel", parent=styles["Heading2"], fontSize=13,
                                    textColor=colors.HexColor("#1c3fae"), spaceBefore=14, spaceAfter=6)
    map_style = ParagraphStyle("MapaRel", parent=styles["Heading1"], fontSize=14,
                                textColor=colors.HexColor("#1c3fae"), spaceBefore=16, spaceAfter=8,
                                borderWidth=0, borderColor=colors.HexColor("#1c3fae"),
                                borderPadding=0)
    city_style = ParagraphStyle("CidadeRel", parent=styles["Heading3"], fontSize=11,
                                 textColor=colors.white, backColor=colors.HexColor("#1c3fae"),
                                 spaceBefore=10, spaceAfter=4, leftIndent=4, borderPadding=4)
    cell_style = ParagraphStyle("CelulaRel", parent=styles["Normal"], fontSize=8.5, leading=11)
    header_style = ParagraphStyle("HeaderCel", parent=styles["Normal"], fontSize=8.5,
                                   leading=10, fontName="Helvetica-Bold")

    story = []
    story.append(Paragraph("Relatório Mensal de Ocorrências", title_style))
    story.append(Paragraph(f"{month_names_pt[month-1]} de {year} — Rede Gazeta", sub_style))
    story.append(Paragraph(f"{total_incidents} ocorrência(s) registrada(s) no período", sub_style))
    story.append(Spacer(1, 14))

    # ---- resumo por dispositivo (por mapa) ----
    story.append(Paragraph("Resumo por Dispositivo", section_style))
    if not device_summary:
        story.append(Paragraph("Nenhuma ocorrência registrada neste período.", cell_style))
    else:
        for map_id, entries in device_summary.items():
            map_label = report.get(map_id, {}).get("name", map_id)
            story.append(Paragraph(map_label, ParagraphStyle(
                "MapaSubRel", parent=styles["Heading3"], fontSize=10.5,
                textColor=colors.HexColor("#1c3fae"), spaceBefore=8, spaceAfter=4)))
            data = [[
                Paragraph("Ocorrências", header_style),
                Paragraph("Cidade / Local", header_style),
                Paragraph("Dispositivo", header_style),
                Paragraph("Tempo Inativo", header_style),
                Paragraph("Observações", header_style),
            ]]
            for e in entries:
                hours = int(e["total_minutes"] // 60)
                mins = int(e["total_minutes"] % 60)
                duration_txt = f"{hours}h{mins:02d}min" if e["total_minutes"] > 0 else "—"
                obs_txt = " / ".join(e["observations"]) if e["observations"] else "—"
                data.append([
                    Paragraph(f"<b>{e['total_failures']}</b>", cell_style),
                    Paragraph(e["site"], cell_style),
                    Paragraph(e["device_name"], cell_style),
                    Paragraph(duration_txt, cell_style),
                    Paragraph(obs_txt, cell_style),
                ])
            tbl = Table(data, colWidths=[2.2*cm, 2.6*cm, 3.4*cm, 2.4*cm, 5.4*cm], repeatRows=1)
            tbl.setStyle(TableStyle([
                ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#d9e2f3")),
                ("GRID", (0,0), (-1,-1), 0.5, colors.HexColor("#cccccc")),
                ("VALIGN", (0,0), (-1,-1), "TOP"),
                ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#f7f8fb")]),
            ]))
            story.append(tbl)
            story.append(Spacer(1, 8))

    story.append(PageBreak())

    # ---- detalhamento por mapa -> cidade ----
    story.append(Paragraph("Detalhamento por Mapa e Cidade", section_style))
    for map_id, map_data in report.items():
        story.append(Paragraph(map_data["name"], map_style))
        for site_name, site_data in map_data["sites"].items():
            rows = [r for r in site_data["incidents"] if not r.get("excluded")]
            count = site_data["failure_count"]
            story.append(Paragraph(f"[{count} ocorrência{'s' if count != 1 else ''}] {site_name}", city_style))
            if not rows:
                story.append(Paragraph("Sem Ocorrências", cell_style))
                story.append(Spacer(1, 8))
                continue
            data = [[
                Paragraph("Data", header_style),
                Paragraph("Descrição da Ocorrência", header_style),
                Paragraph("Início", header_style),
                Paragraph("Normalização", header_style),
                Paragraph("Observação", header_style),
            ]]
            for r in rows:
                end_txt = r["end_time"] if r["end_time"] else "em aberto"
                data.append([
                    Paragraph(r["date_display"], cell_style),
                    Paragraph(r["description"], cell_style),
                    Paragraph(r["start_time"], cell_style),
                    Paragraph(end_txt, cell_style),
                    Paragraph(r["observation"] or "—", cell_style),
                ])
            tbl = Table(data, colWidths=[2.0*cm, 5.6*cm, 1.8*cm, 2.4*cm, 4.2*cm], repeatRows=1)
            tbl.setStyle(TableStyle([
                ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#d9e2f3")),
                ("GRID", (0,0), (-1,-1), 0.5, colors.HexColor("#cccccc")),
                ("VALIGN", (0,0), (-1,-1), "TOP"),
                ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#f7f8fb")]),
            ]))
            story.append(tbl)
            story.append(Spacer(1, 10))

    # ---- situacao em outras localidades (acompanhadas por outro sistema) ----
    other_cities = load_other_system_status(year, month)
    story.append(PageBreak())
    story.append(Paragraph("Situação em Outras Localidades", section_style))
    story.append(Paragraph(
        "Cidades acompanhadas por outro sistema (sem equipamento monitorado por aqui).",
        ParagraphStyle("OtherSysNote", parent=styles["Normal"], fontSize=8.5, textColor=colors.grey, spaceAfter=8)))
    data = [[
        Paragraph("Localidade", header_style), Paragraph("Situação", header_style), Paragraph("Observação", header_style),
    ]]
    for c in other_cities:
        sit_color = "#c62838" if c["situacao"] == "Fora" else "#1a9c42"
        data.append([
            Paragraph(c["city"], cell_style),
            Paragraph(f'<font color="{sit_color}"><b>{c["situacao"]}</b></font>', cell_style),
            Paragraph(c["observacao"] or "—", cell_style),
        ])
    tbl = Table(data, colWidths=[4.5*cm, 2.5*cm, 9*cm], repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#d9e2f3")),
        ("GRID", (0,0), (-1,-1), 0.5, colors.HexColor("#cccccc")),
        ("VALIGN", (0,0), (-1,-1), "TOP"),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#f7f8fb")]),
    ]))
    story.append(tbl)

    doc.build(story)
    return buf.getvalue()


def export_annual_report_pdf(year):
    """Gera o relatorio anual em PDF: total por mes, top dispositivos com
    mais falhas no ano, e detalhamento por mapa/cidade - facil de ler,
    imprimir e compartilhar."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import cm
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from io import BytesIO

    report = build_annual_report(year)

    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
                             topMargin=1.6*cm, bottomMargin=1.6*cm,
                             leftMargin=1.6*cm, rightMargin=1.6*cm)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("TituloRelAnual", parent=styles["Title"], fontSize=17, spaceAfter=4)
    sub_style = ParagraphStyle("SubRelAnual", parent=styles["Normal"], fontSize=10, textColor=colors.grey)
    section_style = ParagraphStyle("SecaoRelAnual", parent=styles["Heading2"], fontSize=13,
                                    textColor=colors.HexColor("#1c3fae"), spaceBefore=14, spaceAfter=6)
    cell_style = ParagraphStyle("CelulaRelAnual", parent=styles["Normal"], fontSize=8.5, leading=11)
    header_style = ParagraphStyle("HeaderCelAnual", parent=styles["Normal"], fontSize=8.5,
                                   leading=10, fontName="Helvetica-Bold")

    story = []
    story.append(Paragraph("Relatório Anual de Ocorrências", title_style))
    story.append(Paragraph(f"Ano de {year} — Rede Gazeta", sub_style))
    story.append(Paragraph(f"{report['grand_total']} ocorrência(s) registrada(s) no ano (dados disponíveis)", sub_style))
    story.append(Spacer(1, 14))

    # ---- total por mes ----
    story.append(Paragraph("Ocorrências por Mês", section_style))
    data = [[
        Paragraph("Mês", header_style), Paragraph("Ocorrências", header_style), Paragraph("Situação do dado", header_style),
    ]]
    for m in report["months"]:
        val = m["total_incidents"]
        val_txt = str(val) if val is not None else ("—" if m["status"] == "futuro" else "?")
        data.append([
            Paragraph(m["name"], cell_style),
            Paragraph(f"<b>{val_txt}</b>", cell_style),
            Paragraph(m["status"], cell_style),
        ])
    tbl = Table(data, colWidths=[4*cm, 3*cm, 9.5*cm], repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#d9e2f3")),
        ("GRID", (0,0), (-1,-1), 0.5, colors.HexColor("#cccccc")),
        ("VALIGN", (0,0), (-1,-1), "TOP"),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#f7f8fb")]),
    ]))
    story.append(tbl)
    story.append(PageBreak())

    # ---- top dispositivos ----
    story.append(Paragraph("Top Dispositivos com Mais Falhas no Ano", section_style))
    if not report["top_devices"]:
        story.append(Paragraph("Nenhum dado disponível para este ano.", cell_style))
    else:
        data = [[
            Paragraph("#", header_style), Paragraph("Cidade/Local", header_style),
            Paragraph("Dispositivo", header_style), Paragraph("Falhas", header_style),
            Paragraph("Tempo Inativo", header_style),
        ]]
        for i, d in enumerate(report["top_devices"], 1):
            hours = int(d["total_minutes"] // 60)
            mins = int(d["total_minutes"] % 60)
            data.append([
                Paragraph(str(i), cell_style),
                Paragraph(d["site"], cell_style),
                Paragraph(d["device_name"], cell_style),
                Paragraph(f"<b>{d['total_failures']}</b>", cell_style),
                Paragraph(f"{hours}h{mins:02d}min", cell_style),
            ])
        tbl = Table(data, colWidths=[1*cm, 3.5*cm, 5.5*cm, 2.5*cm, 4*cm], repeatRows=1)
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#d9e2f3")),
            ("GRID", (0,0), (-1,-1), 0.5, colors.HexColor("#cccccc")),
            ("VALIGN", (0,0), (-1,-1), "TOP"),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#f7f8fb")]),
        ]))
        story.append(tbl)
    story.append(PageBreak())

    # ---- falhas por mapa/cidade no ano ----
    story.append(Paragraph("Falhas por Mapa e Cidade/Categoria no Ano", section_style))
    for map_id, map_data in report["map_site_totals"].items():
        story.append(Paragraph(map_data["name"], ParagraphStyle(
            "MapaAnualRel", parent=styles["Heading3"], fontSize=11,
            textColor=colors.HexColor("#1c3fae"), spaceBefore=10, spaceAfter=4)))
        sites_sorted = sorted(map_data["sites"].items(), key=lambda kv: -kv[1])
        data = [[Paragraph("Cidade/Categoria", header_style), Paragraph("Falhas no ano", header_style)]]
        for site, count in sites_sorted:
            data.append([Paragraph(site, cell_style), Paragraph(f"<b>{count}</b>", cell_style)])
        tbl = Table(data, colWidths=[13*cm, 3*cm], repeatRows=1)
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#d9e2f3")),
            ("GRID", (0,0), (-1,-1), 0.5, colors.HexColor("#cccccc")),
            ("VALIGN", (0,0), (-1,-1), "TOP"),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#f7f8fb")]),
        ]))
        story.append(tbl)
        story.append(Spacer(1, 10))

    doc.build(story)
    return buf.getvalue()



def build_metric_reason(label, state, value_display, compare_method, compare_value, unit):
    """Monta o texto explicando por que uma metrica esta em alarme, no
    mesmo estilo usado nas notificacoes do Telegram e no relatorio
    (ex: "Potencia abaixo do limite (45W < 50W configurado no Dude)")."""
    unit = unit or ""
    if state == "error":
        return "Sem leitura SNMP (falha de comunicação com o equipamento)"
    cv_display = compare_value
    if compare_method == 4:
        return f"{label} acima do limite ({value_display} > {cv_display}{unit} configurado no Dude)"
    if compare_method == 6:
        return f"{label} abaixo do limite ({value_display} < {cv_display}{unit} configurado no Dude)"
    if compare_method == 3:
        return f"{label} abaixo do limite ({value_display} < {cv_display}{unit})"
    if compare_method == 5:
        return f"{label} acima do limite ({value_display} > {cv_display}{unit})"
    if compare_method == 1:
        return f"{label} atingiu o valor de alarme ({cv_display}{unit})"
    if compare_method == 2:
        return f"{label} diferente do valor esperado"
    return f"{label} fora do valor esperado (valor atual: {value_display})"


# --------------------------------------------------------------------------
# PING (usa o utilitario do sistema operacional - funciona sem privilegios
# de root/admin, ao contrario de um ping ICMP "cru" em socket)
# --------------------------------------------------------------------------
def ping_host(ip, timeout_s=PING_TIMEOUT_S):
    is_windows = os.name == "nt"
    if is_windows:
        timeout_ms = int(timeout_s * 1000)
        cmd = ["ping", "-n", "1", "-w", str(timeout_ms), ip]
    else:
        cmd = ["ping", "-c", "1", "-W", str(max(1, int(timeout_s))), ip]

    start = time.time()
    try:
        out = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_s + 1.5,
        )
        elapsed_ms = (time.time() - start) * 1000
        text = out.stdout.decode(errors="replace")

        if out.returncode != 0:
            return False, None

        # tenta extrair a latencia relatada pelo proprio ping
        m = re.search(r"time[=<]\s*([\d.]+)\s*ms", text, re.IGNORECASE)
        if m:
            return True, round(float(m.group(1)), 1)
        m = re.search(r"tempo[=<]\s*([\d.]+)\s*ms", text, re.IGNORECASE)
        if m:
            return True, round(float(m.group(1)), 1)
        return True, round(elapsed_ms, 1)
    except FileNotFoundError:
        # comando "ping" nao disponivel neste sistema -> usa fallback TCP
        return _tcp_probe(ip)
    except Exception:
        return False, None


_MAC_CACHE = {}          # ip -> (mac, timestamp)
_MAC_CACHE_TTL_S = 300    # recheca a tabela ARP no maximo a cada 5 minutos por IP
_MAC_CACHE_LOCK = threading.Lock()


def get_mac_from_ip(ip):
    """Descobre o endereco MAC de um IP consultando a tabela ARP do
    proprio sistema operacional (Windows: 'arp -a', Linux: 'ip neigh' ou
    'arp -n'). So funciona para equipamentos na mesma rede local/segmento
    - a tabela ARP so tem entradas de IPs que ja foram contactados
    recentemente (por isso chamamos isso logo apos o ping, que garante
    uma entrada fresca)."""
    with _MAC_CACHE_LOCK:
        cached = _MAC_CACHE.get(ip)
        if cached and (time.time() - cached[1]) < _MAC_CACHE_TTL_S:
            return cached[0]

    mac = None
    try:
        if os.name == "nt":
            out = subprocess.run(["arp", "-a", ip], stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, timeout=2)
            text = out.stdout.decode(errors="replace")
            m = re.search(r"([0-9a-fA-F]{2}[-:]){5}[0-9a-fA-F]{2}", text)
            if m:
                mac = m.group(0).replace("-", ":").upper()
        else:
            out = subprocess.run(["ip", "neigh", "show", ip], stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, timeout=2)
            text = out.stdout.decode(errors="replace")
            m = re.search(r"([0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}", text)
            if m:
                mac = m.group(0).upper()
            else:
                out = subprocess.run(["arp", "-n", ip], stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE, timeout=2)
                text = out.stdout.decode(errors="replace")
                m = re.search(r"([0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}", text)
                if m:
                    mac = m.group(0).upper()
    except Exception:
        mac = None

    with _MAC_CACHE_LOCK:
        _MAC_CACHE[ip] = (mac, time.time())
    return mac


def _tcp_probe(ip, ports=(80, 443, 22, 8080), timeout_s=1.0):
    """Fallback quando o binario 'ping' do sistema nao esta disponivel:
    tenta abrir uma conexao TCP em portas comuns para inferir se o host
    esta ativo na rede. Menos preciso que ICMP, mas nao depende de nada
    externo."""
    for port in ports:
        start = time.time()
        try:
            with socket.create_connection((ip, port), timeout=timeout_s):
                return True, round((time.time() - start) * 1000, 1)
        except ConnectionRefusedError:
            # conexao recusada ainda indica que o host respondeu
            return True, round((time.time() - start) * 1000, 1)
        except (socket.timeout, OSError):
            continue
    return False, None


# --------------------------------------------------------------------------
# SNMP GET (v1 / v2c) - implementacao minima de BER/ASN.1, sem dependencias
# --------------------------------------------------------------------------
def _ber_len(n):
    if n < 0x80:
        return bytes([n])
    b = []
    while n:
        b.insert(0, n & 0xFF)
        n >>= 8
    return bytes([0x80 | len(b)]) + bytes(b)


def _ber_tlv(tag, value):
    return bytes([tag]) + _ber_len(len(value)) + value


def _ber_int(n):
    if n == 0:
        return _ber_tlv(0x02, b"\x00")
    b = []
    neg = n < 0
    val = n
    while True:
        byte = val & 0xFF
        b.insert(0, byte)
        val >>= 8
        if val == 0 or val == -1:
            break
    if not neg and b[0] & 0x80:
        b.insert(0, 0x00)
    return _ber_tlv(0x02, bytes(b))


def _ber_oid(oid_str):
    parts = [int(p) for p in oid_str.strip(".").split(".")]
    if len(parts) < 2:
        raise ValueError("OID invalido: " + oid_str)
    first = parts[0] * 40 + parts[1]
    out = bytearray([first])
    for p in parts[2:]:
        if p == 0:
            out.append(0)
            continue
        chunk = []
        while p:
            chunk.insert(0, p & 0x7F)
            p >>= 7
        for i in range(len(chunk) - 1):
            chunk[i] |= 0x80
        out.extend(chunk)
    return _ber_tlv(0x06, bytes(out))


def _ber_oid_decode(data):
    if not data:
        return ""
    first = data[0]
    parts = [first // 40, first % 40]
    i = 1
    while i < len(data):
        val = 0
        while True:
            b = data[i]
            i += 1
            val = (val << 7) | (b & 0x7F)
            if not (b & 0x80):
                break
        parts.append(val)
    return ".".join(str(p) for p in parts)


def _snmp_get_request(community, oid, version=0, request_id=1):
    oid_ber = _ber_oid(oid)
    null_ber = _ber_tlv(0x05, b"")
    varbind = _ber_tlv(0x30, oid_ber + null_ber)
    varbindlist = _ber_tlv(0x30, varbind)

    pdu_body = (
        _ber_int(request_id)
        + _ber_int(0)  # error-status
        + _ber_int(0)  # error-index
        + varbindlist
    )
    pdu = _ber_tlv(0xA0, pdu_body)  # GetRequest-PDU

    message = (
        _ber_int(version)  # 0 = v1, 1 = v2c
        + _ber_tlv(0x04, community.encode("utf-8"))
        + pdu
    )
    return _ber_tlv(0x30, message)


def _snmp_getnext_request(community, oid, version=0, request_id=1):
    oid_ber = _ber_oid(oid)
    null_ber = _ber_tlv(0x05, b"")
    varbind = _ber_tlv(0x30, oid_ber + null_ber)
    varbindlist = _ber_tlv(0x30, varbind)

    pdu_body = _ber_int(request_id) + _ber_int(0) + _ber_int(0) + varbindlist
    pdu = _ber_tlv(0xA1, pdu_body)  # GetNextRequest-PDU

    message = _ber_int(version) + _ber_tlv(0x04, community.encode("utf-8")) + pdu
    return _ber_tlv(0x30, message)


class _BerReader:
    def __init__(self, data):
        self.data = data
        self.pos = 0

    def read_tlv(self):
        tag = self.data[self.pos]
        self.pos += 1
        length = self.data[self.pos]
        self.pos += 1
        if length & 0x80:
            n_bytes = length & 0x7F
            length = int.from_bytes(self.data[self.pos:self.pos + n_bytes], "big")
            self.pos += n_bytes
        value = self.data[self.pos:self.pos + length]
        self.pos += length
        return tag, value


def _parse_int(value):
    return int.from_bytes(value, "big", signed=True) if value else 0


def _parse_uint(value):
    return int.from_bytes(value, "big", signed=False) if value else 0


def _snmp_parse_response(data):
    """Retorna (tag_do_valor, valor_decodificado) do primeiro varbind."""
    r = _BerReader(data)
    tag, seq = r.read_tlv()          # SEQUENCE (Message)
    inner = _BerReader(seq)
    inner.read_tlv()                 # version
    inner.read_tlv()                 # community
    pdu_tag, pdu_body = inner.read_tlv()   # PDU (0xA2 = GetResponse)

    p = _BerReader(pdu_body)
    p.read_tlv()                     # request-id
    err_tag, err_val = p.read_tlv()  # error-status
    p.read_tlv()                     # error-index
    _, vbl = p.read_tlv()            # varbind list SEQUENCE

    vbl_reader = _BerReader(vbl)
    _, vb = vbl_reader.read_tlv()    # first varbind SEQUENCE
    vb_reader = _BerReader(vb)
    vb_reader.read_tlv()             # OID
    val_tag, val_bytes = vb_reader.read_tlv()

    if _parse_int(err_val) != 0:
        return None, None

    if val_tag == 0x02:                                   # INTEGER
        return val_tag, _parse_int(val_bytes)
    if val_tag in (0x41, 0x42, 0x43, 0x46):                # Counter32/Gauge32/TimeTicks/Counter64
        return val_tag, _parse_uint(val_bytes)
    if val_tag == 0x04:                                    # OCTET STRING
        try:
            return val_tag, val_bytes.decode("utf-8")
        except UnicodeDecodeError:
            return val_tag, val_bytes.hex()
    if val_tag == 0x40:                                     # IpAddress
        return val_tag, ".".join(str(b) for b in val_bytes)
    if val_tag in (0x80, 0x81, 0x82):                        # noSuchObject/Instance/endOfMibView
        return val_tag, None
    return val_tag, None


def _decode_snmp_value(val_tag, val_bytes):
    if val_tag == 0x02:
        return _parse_int(val_bytes)
    if val_tag in (0x41, 0x42, 0x43, 0x46):
        return _parse_uint(val_bytes)
    if val_tag == 0x04:
        try:
            return val_bytes.decode("utf-8")
        except UnicodeDecodeError:
            return val_bytes.hex()
    if val_tag == 0x40:
        return ".".join(str(b) for b in val_bytes)
    if val_tag == 0x06:
        return _ber_oid_decode(val_bytes)
    if val_tag in (0x80, 0x81, 0x82):
        return None
    return None


def _snmp_parse_full_message(data):
    r = _BerReader(data)
    _, seq = r.read_tlv()
    inner = _BerReader(seq)
    inner.read_tlv()               # version
    inner.read_tlv()               # community
    _, pdu_body = inner.read_tlv()  # PDU

    p = _BerReader(pdu_body)
    p.read_tlv()                   # request-id
    err_tag, err_val = p.read_tlv()
    p.read_tlv()                   # error-index
    _, vbl = p.read_tlv()

    vbl_reader = _BerReader(vbl)
    _, vb = vbl_reader.read_tlv()
    vb_reader = _BerReader(vb)
    oid_tag, oid_bytes = vb_reader.read_tlv()
    val_tag, val_bytes = vb_reader.read_tlv()

    if _parse_int(err_val) != 0:
        return None, None, None

    oid_str = _ber_oid_decode(oid_bytes)
    value = _decode_snmp_value(val_tag, val_bytes)
    return oid_str, val_tag, value


# --------------------------------------------------------------------------
# Espelho do The Dude (le direto do dude.db ATIVO, ao inves de consultar
# SNMP por conta propria). Util para metricas cujo OID exato nao foi
# identificado, mas que o proprio Dude ja monitora - lemos o mesmo valor
# que ele coletou, direto da tabela chart_values_raw.
#
# Formato descoberto no banco: cada linha da chart_values_raw tem
#   sourceIDandTime = (dataSourceID << 32) | timestamp_unix
#   value           = valor bruto coletado naquele instante
#
# DESATIVADO por padrao (mude para True para religar). Enquanto False,
# qualquer metrica com "dude_datasource_id" configurado cai direto para
# a leitura SNMP normal (se tiver "oid"), sem tentar acessar o dude.db.
# --------------------------------------------------------------------------
DUDE_MIRROR_ENABLED = False

DUDE_DB_CANDIDATES = [
    r"C:\Program Files\Dude\dude.db",
    r"C:\Program Files (x86)\Dude\dude.db",
    r"C:\Program Files (x86)\Dude\data\dude.db",
    r"C:\Program Files\Dude\data\dude.db",
    r"C:\Users\Public\Dude\dude.db",
    r"C:\Dude\dude.db",
    r"C:\ProgramData\Dude\dude.db",
    "/opt/dude/dude.db",
    "/var/lib/dude/dude.db",
]
# Se nenhum caminho acima bater com a instalacao real, defina o caminho
# certo aqui manualmente (sobrescreve a deteccao automatica):
DUDE_DB_PATH_OVERRIDE = None

DUDE_DB_MAX_AGE_S = 300  # se o valor mais recente no banco do Dude tiver
                          # mais de 5 minutos, consideramos desatualizado


def find_dude_db_path():
    if DUDE_DB_PATH_OVERRIDE and os.path.exists(DUDE_DB_PATH_OVERRIDE):
        return DUDE_DB_PATH_OVERRIDE

    # Windows redireciona gravacoes de apps antigos (32-bit, sem elevacao)
    # que tentam escrever dentro de "Program Files" para o VirtualStore
    # do usuario. E onde o dude.db ATIVO normalmente acaba ficando.
    local_appdata = os.environ.get("LOCALAPPDATA")
    if local_appdata:
        virtualstore_candidates = [
            os.path.join(local_appdata, "VirtualStore", "Program Files (x86)", "Dude", "data", "dude.db"),
            os.path.join(local_appdata, "VirtualStore", "Program Files (x86)", "Dude", "dude.db"),
            os.path.join(local_appdata, "VirtualStore", "Program Files", "Dude", "data", "dude.db"),
            os.path.join(local_appdata, "VirtualStore", "Program Files", "Dude", "dude.db"),
        ]
        for path in virtualstore_candidates:
            if os.path.exists(path):
                return path

    for path in DUDE_DB_CANDIDATES:
        if os.path.exists(path):
            return path
    return None


_DUDE_DB_PATH = None
_DUDE_DB_WARNED = False


def read_dude_datasource_value(datasource_id):
    """Le o valor mais recente de um dataSourceID direto do dude.db ativo.
    Retorna None se o arquivo nao existir, estiver bloqueado, ou nao
    houver leitura recente o suficiente."""
    if not DUDE_MIRROR_ENABLED:
        return None

    global _DUDE_DB_PATH, _DUDE_DB_WARNED
    if _DUDE_DB_PATH is None:
        _DUDE_DB_PATH = find_dude_db_path()
        if _DUDE_DB_PATH is None:
            if not _DUDE_DB_WARNED:
                print("[dude-mirror] dude.db ativo nao encontrado nos caminhos padrao. "
                      "Ajuste DUDE_DB_PATH_OVERRIDE no server.py.", file=sys.stderr)
                _DUDE_DB_WARNED = True
            return None

    low = datasource_id << 32
    high = (datasource_id + 1) << 32

    DUDE_DB_RETRY_COUNT = 4
    DUDE_DB_RETRY_DELAY_S = 0.4

    last_error = None
    for attempt in range(DUDE_DB_RETRY_COUNT):
        try:
            uri = f"file:{_DUDE_DB_PATH}?mode=ro"
            con = sqlite3.connect(uri, uri=True, timeout=5)
            con.execute("PRAGMA busy_timeout = 5000")
            cur = con.cursor()
            cur.execute(
                "SELECT sourceIDandTime, value FROM chart_values_raw "
                "WHERE sourceIDandTime >= ? AND sourceIDandTime < ? "
                "ORDER BY sourceIDandTime DESC LIMIT 1",
                (low, high),
            )
            row = cur.fetchone()
            con.close()
            if not row:
                return None
            sid_time, value = row
            break
        except sqlite3.OperationalError as e:
            last_error = e
            try:
                con.close()
            except Exception:
                pass
            if attempt < DUDE_DB_RETRY_COUNT - 1:
                time.sleep(DUDE_DB_RETRY_DELAY_S)
            continue
        except Exception as e:
            print("[dude-mirror] erro ao ler dude.db:", e, file=sys.stderr)
            return None
    else:
        print(f"[dude-mirror] dude.db ocupado apos {DUDE_DB_RETRY_COUNT} tentativas:", last_error, file=sys.stderr)
        return None

    ts = sid_time & 0xFFFFFFFF
    if time.time() - ts > DUDE_DB_MAX_AGE_S:
        return None  # leitura muito antiga, provavelmente parado
    return value


SNMP_RETRY_COUNT = 2  # antes 3 - equipamentos atras de enlace de radio podem
                       # perder pacote UDP isolado, entao ainda vale a pena
                       # tentar de novo uma vez. Mas 3 tentativas ficava caro
                       # demais quando um dispositivo tem varias metricas (elas
                       # sao lidas uma de cada vez): 4 metricas x 3 tentativas x
                       # 1.5s podia levar quase 18s so nesse dispositivo,
                       # atrasando a leitura dos outros na fila.


def snmp_get(ip, community, oid, version="1", port=161, timeout_s=SNMP_TIMEOUT_S, _tried_instance=False):
    ver_num = 1 if version in ("2", "2c", 1) else 0
    oid = (oid or "").strip().strip(".")

    last_error = None
    for attempt in range(SNMP_RETRY_COUNT):
        req_id = (int(time.time() * 1000) + attempt) & 0x7FFFFFFF
        packet = _snmp_get_request(community, oid, version=ver_num, request_id=req_id)

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(timeout_s)
        try:
            sock.sendto(packet, (ip, port))
            data, _addr = sock.recvfrom(4096)
            _tag, value = _snmp_parse_response(data)
            if value is None and not _tried_instance and not oid.endswith(".0"):
                # o equipamento RESPONDEU, mas sem valor (noSuchName/noSuchObject):
                # varias planilhas trazem OID escalar sem o ".0" final
                # (ex: .1.3.6.1.2.1.33.1.2.1) - tenta de novo com a instancia
                sock.close()
                return snmp_get(ip, community, oid + ".0", version=version, port=port,
                                timeout_s=timeout_s, _tried_instance=True)
            return value
        except Exception as e:
            last_error = e
            continue
        finally:
            sock.close()

    return None


def snmp_walk(ip, community, base_oid, version="1", port=161,
               max_results=40, timeout_s=1.2, total_budget_s=60.0):
    """Percorre (GETNEXT sucessivos) a partir de base_oid, retornando os
    OIDs e valores encontrados dentro dessa subarvore. Usado pela
    ferramenta de descoberta SNMP na pagina de configuracao."""
    ver_num = 1 if version in ("2", "2c", 1) else 0
    results = []
    current_oid = base_oid.strip(".")
    prefix = current_oid + "."
    start_time = time.time()
    seen = set()

    while len(results) < max_results and (time.time() - start_time) < total_budget_s:
        req_id = int(time.time() * 1000) & 0x7FFFFFFF
        packet = _snmp_getnext_request(community, current_oid, version=ver_num, request_id=req_id)

        oid_str = val_tag = value = None
        for attempt in range(3):
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(timeout_s)
            try:
                sock.sendto(packet, (ip, port))
                data, _addr = sock.recvfrom(4096)
                oid_str, val_tag, value = _snmp_parse_full_message(data)
                break  # sucesso, sai do retry
            except Exception:
                if attempt == 2:
                    oid_str = None
            finally:
                sock.close()

        if not oid_str:
            break  # esgotou as tentativas nesse passo: encerra a varredura aqui
        if not oid_str.startswith(prefix):
            break  # saiu da subarvore pedida: fim da varredura
        if oid_str in seen:
            break  # agente respondendo em loop: encerra por seguranca
        seen.add(oid_str)
        results.append({"oid": oid_str, "value": value, "tag": val_tag})
        current_oid = oid_str

    return results



# --------------------------------------------------------------------------
# MODBUS TCP - cliente proprio (so biblioteca padrao)
# --------------------------------------------------------------------------
# Suporta: coil (FC1), discrete input (FC2), holding register (FC3) e
# input register (FC4). Tipos: bool, int16, uint16, int32, uint32, float
# (32 bits), int64, uint64, double. Enderecos no mesmo padrao das planilhas
# do NETx (endereco de protocolo, base 0).
#
# Ordem das palavras (igual ao NETx):
#   word_swap  True  (padrao) -> palavra mais significativa primeiro (Schneider PM3xxx/iEM, DSE)
#   word_swap  False          -> palavra menos significativa primeiro (Conzerv PM1000, CLP Modicon)
#   dword_swap False          -> (so 64 bits) inverte a ordem dos dois blocos de 32 bits
#
# Varios medidores costumam ficar atras do MESMO gateway (ex: 172.10.15.2,
# um Slave ID por medidor). Um lock por gateway garante que so uma leitura
# por vez va para ele - gateways Modbus normalmente nao aguentam leituras
# paralelas. Leituras de registradores vizinhos sao agrupadas num unico
# pedido (menos trafego); se o equipamento recusar o bloco, le um por um.
# --------------------------------------------------------------------------
MODBUS_DEFAULT_PORT = 502
MODBUS_DEFAULT_TIMEOUT_S = 5.0
MODBUS_MIN_TIMEOUT_S = 4.0         # gateways seriais (RS-485) respondem devagar
MODBUS_MAX_TIMEOUT_S = 15.0
MODBUS_FAIL_TOLERANCE = 3          # so marca OFFLINE depois de 3 ciclos seguidos sem leitura
MODBUS_HOLD_S = 300                # ate quanto tempo mostra o ultimo valor valido
_MODBUS_CLIENTS = {}               # (ip, porta) -> conexao TCP mantida aberta
_MODBUS_LAST_GOOD = {}             # dev_id -> {rotulo: (valor, timestamp)}
_MODBUS_FAILS = {}                 # dev_id -> ciclos seguidos sem nenhuma leitura
MODBUS_BLOCK_MAX_WORDS = 60
MODBUS_BLOCK_MAX_GAP = 6

MODBUS_REGISTER_TYPES = {"coil": 1, "discrete": 2, "holding": 3, "input": 4}
MODBUS_DATA_WORDS = {
    "bool": 1, "uint16": 1, "int16": 1,
    "uint32": 2, "int32": 2, "float": 2,
    "uint64": 4, "int64": 4, "double": 4,
}
MODBUS_EXCEPTIONS = {
    1: "funcao nao suportada", 2: "endereco invalido", 3: "valor invalido",
    4: "falha no escravo", 5: "confirmado (ack)", 6: "escravo ocupado",
    10: "gateway sem caminho", 11: "escravo nao respondeu ao gateway",
}

_MODBUS_LOCKS = {}
_MODBUS_LOCKS_META = threading.Lock()
_MODBUS_ENDPOINT_DOWN_UNTIL = {}   # (ip, port) -> timestamp: falha recente de conexao
MODBUS_DOWN_HOLD_S = 15            # evita 10 medidores esperando o mesmo gateway fora do ar


def _modbus_endpoint_lock(ip, port):
    key = (ip, int(port))
    with _MODBUS_LOCKS_META:
        lock = _MODBUS_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _MODBUS_LOCKS[key] = lock
        return lock


def normalize_register_type(text):
    t = (str(text or "")).strip().lower()
    if t.startswith("coil"):
        return "coil"
    if t.startswith("discrete") or t.startswith("digital input") or t == "di":
        return "discrete"
    if t.startswith("input"):
        return "input"
    if t.startswith("holding") or t in ("hr", "register", ""):
        return "holding"
    return t if t in MODBUS_REGISTER_TYPES else "holding"


def normalize_data_type(text):
    t = (str(text or "")).strip().lower().replace(" ", "")
    aliases = {"float32": "float", "real": "float", "single": "float", "float64": "double",
               "boolean": "bool", "bit": "bool", "word": "uint16", "dword": "uint32",
               "short": "int16", "int": "int16", "long": "int32"}
    t = aliases.get(t, t)
    return t if t in MODBUS_DATA_WORDS else "uint16"


def parse_swap_flag(value, default=None):
    """'T'/'F'/True/False/'' -> True/False/default (NETx usa T/F)."""
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    v = str(value).strip().lower()
    if v in ("t", "true", "1", "sim", "s", "yes"):
        return True
    if v in ("f", "false", "0", "nao", "não", "n", "no"):
        return False
    return default


class ModbusError(Exception):
    def __init__(self, message, code=None):
        super().__init__(message)
        self.code = code


class ModbusTCPClient:
    def __init__(self, ip, port=MODBUS_DEFAULT_PORT, timeout_s=MODBUS_DEFAULT_TIMEOUT_S):
        self.ip = ip
        self.port = int(port or MODBUS_DEFAULT_PORT)
        self.timeout_s = timeout_s
        self.sock = None
        self._tid = int(time.time() * 1000) & 0x7FFF
        self.connect_ms = None

    def connect(self):
        t0 = time.time()
        self.sock = socket.create_connection((self.ip, self.port), timeout=self.timeout_s)
        self.sock.settimeout(self.timeout_s)
        self.connect_ms = round((time.time() - t0) * 1000, 1)

    def close(self):
        try:
            if self.sock:
                self.sock.close()
        except OSError:
            pass
        self.sock = None

    def _recv_exact(self, n):
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ModbusError("conexao fechada pelo equipamento")
            buf += chunk
        return buf

    def read(self, unit_id, function, address, count):
        """Retorna lista de inteiros (palavras de 16 bits, ou bits 0/1)."""
        if self.sock is None:
            self.connect()
        self._tid = (self._tid + 1) & 0xFFFF
        tid = self._tid
        pdu = struct.pack(">BHH", function, address & 0xFFFF, count)
        mbap = struct.pack(">HHHB", tid, 0, len(pdu) + 1, int(unit_id) & 0xFF)
        self.sock.sendall(mbap + pdu)
        for _ in range(4):  # descarta respostas atrasadas de pedidos anteriores
            header = self._recv_exact(7)
            r_tid, _proto, length, _unit = struct.unpack(">HHHB", header)
            body = self._recv_exact(length - 1)
            if r_tid == tid:
                break
        else:
            raise ModbusError("resposta fora de sequencia")
        fc = body[0]
        if fc & 0x80:
            code = body[1] if len(body) > 1 else None
            raise ModbusError("excecao Modbus %s (%s)" % (code, MODBUS_EXCEPTIONS.get(code, "?")), code)
        byte_count = body[1]
        data = body[2:2 + byte_count]
        if function in (1, 2):
            return [(data[i // 8] >> (i % 8)) & 1 for i in range(count)]
        if len(data) < count * 2:
            raise ModbusError("resposta curta (%d bytes)" % len(data))
        return list(struct.unpack(">" + "H" * count, data[:count * 2]))


def decode_modbus_words(words, data_type, word_swap=True, dword_swap=True):
    data_type = normalize_data_type(data_type)
    if data_type == "bool":
        return 1 if words and words[0] else 0
    if data_type == "uint16":
        return words[0]
    if data_type == "int16":
        w = words[0]
        return w - 0x10000 if w & 0x8000 else w
    ws = list(words[:MODBUS_DATA_WORDS[data_type]])
    pairs = [ws[i:i + 2] for i in range(0, len(ws), 2)]
    if word_swap is False:
        pairs = [list(reversed(p)) for p in pairs]
    if len(pairs) == 2 and dword_swap is False:
        pairs.reverse()
    ordered = [w for p in pairs for w in p]
    raw = b"".join(struct.pack(">H", w & 0xFFFF) for w in ordered)
    fmt = {"uint32": ">I", "int32": ">i", "float": ">f",
           "uint64": ">Q", "int64": ">q", "double": ">d"}[data_type]
    value = struct.unpack(fmt, raw)[0]
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            return None
        value = round(value, 4)
    return value


def _modbus_metric_spec(metric, device_cfg):
    rtype = normalize_register_type(metric.get("register_type"))
    dtype = normalize_data_type(metric.get("data_type"))
    words = 1 if rtype in ("coil", "discrete") else MODBUS_DATA_WORDS[dtype]
    ws = parse_swap_flag(metric.get("word_swap"), None)
    if ws is None:
        ws = parse_swap_flag(device_cfg.get("word_swap"), True)
    ds = parse_swap_flag(metric.get("dword_swap"), None)
    if ds is None:
        ds = parse_swap_flag(device_cfg.get("dword_swap"), True)
    return {
        "label": metric.get("label"),
        "rtype": rtype,
        "fc": MODBUS_REGISTER_TYPES[rtype],
        "address": int(metric.get("address") or 0),
        "words": words,
        "dtype": dtype,
        "word_swap": ws,
        "dword_swap": ds,
    }


def _group_modbus_blocks(specs):
    """Agrupa leituras vizinhas do mesmo tipo em blocos (menos pedidos)."""
    blocks = []
    by_fc = {}
    for sp in specs:
        by_fc.setdefault(sp["fc"], []).append(sp)
    for fc, items in by_fc.items():
        items = sorted(items, key=lambda x: x["address"])
        current = None
        for sp in items:
            end = sp["address"] + sp["words"]
            if current is not None:
                gap = sp["address"] - current["end"]
                new_len = max(current["end"], end) - current["start"]
                if gap <= MODBUS_BLOCK_MAX_GAP and new_len <= MODBUS_BLOCK_MAX_WORDS:
                    current["items"].append(sp)
                    current["end"] = max(current["end"], end)
                    continue
            current = {"fc": fc, "start": sp["address"], "end": end, "items": [sp]}
            blocks.append(current)
    return blocks


def modbus_read_device(device, metrics):
    """Le todas as metricas Modbus de UM dispositivo numa unica conexao.
    Retorna (conectou: bool, latencia_ms, {label: valor_bruto|None}, erro|None)."""
    cfg = device.get("modbus") or {}
    ip = device.get("ip")
    port = int(cfg.get("port") or MODBUS_DEFAULT_PORT)
    unit_id = int(cfg.get("unit_id") if cfg.get("unit_id") not in (None, "") else 1)
    try:
        timeout_s = float(cfg.get("timeout_ms") or MODBUS_DEFAULT_TIMEOUT_S * 1000) / 1000.0
    except (TypeError, ValueError):
        timeout_s = MODBUS_DEFAULT_TIMEOUT_S
    timeout_s = max(MODBUS_MIN_TIMEOUT_S, min(MODBUS_MAX_TIMEOUT_S, timeout_s))

    specs = []
    for m in metrics:
        try:
            specs.append(_modbus_metric_spec(m, cfg))
        except (TypeError, ValueError):
            continue
    values = {sp["label"]: None for sp in specs}

    key = (ip, port)
    if time.time() < _MODBUS_ENDPOINT_DOWN_UNTIL.get(key, 0):
        return False, None, values, "sem conexao TCP %s:%s (falha recente)" % (ip, port)
    lock = _modbus_endpoint_lock(ip, port)
    with lock:
        if time.time() < _MODBUS_ENDPOINT_DOWN_UNTIL.get(key, 0):
            return False, None, values, "sem conexao TCP %s:%s (falha recente)" % (ip, port)
        # conexao PERSISTENTE (igual ao NETx): abrir/fechar a cada ciclo
        # esgota as vagas de conexao de gateways e controladores de gerador
        client = _MODBUS_CLIENTS.get(key)
        if client is None or client.sock is None:
            client = ModbusTCPClient(ip, port, timeout_s)
            try:
                client.connect()
                _MODBUS_ENDPOINT_DOWN_UNTIL.pop(key, None)
            except OSError as e:
                _MODBUS_ENDPOINT_DOWN_UNTIL[key] = time.time() + MODBUS_DOWN_HOLD_S
                return False, None, values, "sem conexao TCP %s:%s (%s)" % (ip, port, e)
            _MODBUS_CLIENTS[key] = client
        client.timeout_s = timeout_s
        try:
            client.sock.settimeout(timeout_s)
        except OSError:
            pass
        t_rtt = time.time()

        def _read(fc, address, count):
            """le com UMA nova tentativa se a conexao antiga tiver caido
            (o gateway fecha conexoes paradas depois de um tempo)."""
            try:
                if client.sock is None:
                    client.connect()
                return client.read(unit_id, fc, address, count)
            except socket.timeout:
                raise   # equipamento lento/mudo: nao repete (seguraria o gateway para os outros)
            except (ModbusError, OSError) as e:
                if isinstance(e, ModbusError) and e.code is not None:
                    raise
                client.close()
                client.connect()
                return client.read(unit_id, fc, address, count)

        last_error = None
        consecutive_timeouts = 0
        try:
            for block in _group_modbus_blocks(specs):
                if consecutive_timeouts >= 2:
                    break  # escravo mudo: nao insiste nas outras leituras
                count = block["end"] - block["start"]
                try:
                    raw = _read(block["fc"], block["start"], count)
                    consecutive_timeouts = 0
                    for sp in block["items"]:
                        off = sp["address"] - block["start"]
                        chunk = raw[off:off + sp["words"]]
                        if block["fc"] in (1, 2):
                            values[sp["label"]] = chunk[0] if chunk else None
                        else:
                            values[sp["label"]] = decode_modbus_words(chunk, sp["dtype"], sp["word_swap"], sp["dword_swap"])
                    continue
                except socket.timeout:
                    consecutive_timeouts += 1
                    last_error = "timeout"
                    client.close()   # descarta resposta atrasada
                    continue
                except ModbusError as e:
                    last_error = str(e)
                    if e.code in (10, 11):
                        consecutive_timeouts += 1
                        continue
                except OSError as e:
                    last_error = str(e)
                    client.close()
                    continue
                # bloco recusado (ex: endereco invalido no meio do bloco): le um por um
                if len(block["items"]) > 1:
                    for sp in block["items"]:
                        try:
                            raw = _read(sp["fc"], sp["address"], sp["words"])
                            if sp["fc"] in (1, 2):
                                values[sp["label"]] = raw[0]
                            else:
                                values[sp["label"]] = decode_modbus_words(raw, sp["dtype"], sp["word_swap"], sp["dword_swap"])
                        except socket.timeout:
                            last_error = "timeout"
                            client.close()
                        except (ModbusError, OSError) as e:
                            last_error = str(e)
                            if isinstance(e, OSError):
                                client.close()
        finally:
            latency = round((time.time() - t_rtt) * 1000, 1)
            if client.sock is None:
                _MODBUS_CLIENTS.pop(key, None)
    return True, latency, values, last_error


def modbus_read_single(ip, port, unit_id, register_type, address, data_type,
                       word_swap=True, dword_swap=True, timeout_s=MODBUS_DEFAULT_TIMEOUT_S):
    """Leitura avulsa - usada pelo botao 'testar leitura' da configuracao."""
    fake_dev = {"ip": ip, "modbus": {"port": port, "unit_id": unit_id,
                                     "word_swap": word_swap, "dword_swap": dword_swap,
                                     "timeout_ms": timeout_s * 1000}}
    metric = {"label": "_teste", "register_type": register_type, "address": address, "data_type": data_type}
    ok, latency, values, err = modbus_read_device(fake_dev, [metric])
    return {"connected": ok, "latency_ms": latency, "value": values.get("_teste"), "error": err}


def modbus_apply_tolerance(dev_id, connected, raw_values):
    """Segura o ultimo valor valido por ate MODBUS_HOLD_S e so declara o
    equipamento offline depois de MODBUS_FAIL_TOLERANCE ciclos seguidos
    sem nenhuma resposta. Retorna (reachable, qtd_valores_segurados)."""
    now = time.time()
    cache = _MODBUS_LAST_GOOD.setdefault(dev_id, {})
    got_any = False
    for k, v in raw_values.items():
        if v is not None:
            cache[k] = (v, now)
            got_any = True
    fails = 0 if got_any else _MODBUS_FAILS.get(dev_id, 0) + 1
    _MODBUS_FAILS[dev_id] = fails
    held = 0
    if fails < MODBUS_FAIL_TOLERANCE:
        for k, v in list(raw_values.items()):
            if v is None and k in cache and now - cache[k][1] <= MODBUS_HOLD_S:
                raw_values[k] = cache[k][0]
                held += 1
    has_values = any(v is not None for v in raw_values.values())
    if got_any or not raw_values:
        return connected, held
    return (has_values and fails < MODBUS_FAIL_TOLERANCE), held


def is_modbus_device(device):
    return (device.get("protocol") or "").lower() == "modbus"


DEVICES_FILE_LOCK = threading.Lock()


def load_devices():
    with open(DEVICES_FILE, encoding="utf-8") as f:
        return json.load(f)


def save_devices(data):
    tmp_path = DEVICES_FILE + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, DEVICES_FILE)


def load_devices_fg():
    try:
        with open(DEVICES_FG_FILE, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"map_name": "Mapa FG", "categories_order": [], "devices": [], "categories": {}}


def load_all_devices_for_polling():
    """Combina os dispositivos de TODOS os mapas (Interior, FG, e
    quaisquer mapas novos criados) numa unica lista para o loop de
    polling - todos compartilham o mesmo motor de leitura, historico,
    incidentes e notificacoes."""
    interior = load_devices().get("devices", [])
    fg = load_devices_fg().get("devices", [])
    for dev in interior:
        dev["map"] = "interior"
    for dev in fg:
        dev["map"] = "fg"
    combined = interior + fg

    registry = load_maps_registry()
    for map_id, entry in registry.items():
        if entry.get("kind") != "custom":
            continue
        custom_devices = load_devices_generic(map_id).get("devices", [])
        for dev in custom_devices:
            dev["map"] = map_id
        combined.extend(custom_devices)

    return combined


def _set_device_field(dev_id, field, value, event_label_true, event_label_false):
    """Atualiza um campo booleano de um dispositivo em todas as copias
    presentes (network.json + 'sites', ou network_fg.json + 'categories'),
    de forma atomica. Retorna True se encontrou e salvou, False senao."""
    with DEVICES_FILE_LOCK:
        data = load_devices()
        found = False
        dev_name = None
        dev_ip = None

        for dev in data.get("devices", []):
            if dev.get("id") == dev_id:
                dev[field] = value
                found = True
                dev_name = dev.get("name")
                dev_ip = dev.get("ip")

        for site_devs in data.get("sites", {}).values():
            for dev in site_devs:
                if dev.get("id") == dev_id:
                    dev[field] = value

        if found:
            save_devices(data)
            log_event(event_label_true if value else event_label_false, dev_name or str(dev_id), dev_ip)
            return True

        # nao encontrado no Mapa Interior: tenta no Mapa FG
        fg_data = load_devices_fg()
        for dev in fg_data.get("devices", []):
            if dev.get("id") == dev_id:
                dev[field] = value
                found = True
                dev_name = dev.get("name")
                dev_ip = dev.get("ip")

        for cat_devs in fg_data.get("categories", {}).values():
            for dev in cat_devs:
                if dev.get("id") == dev_id:
                    dev[field] = value

        if found:
            tmp_path = DEVICES_FG_FILE + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(fg_data, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, DEVICES_FG_FILE)
            log_event(event_label_true if value else event_label_false, dev_name or str(dev_id), dev_ip)
            return True

        # nao encontrado em Interior nem FG: tenta em qualquer mapa novo
        registry = load_maps_registry()
        for map_id, entry in registry.items():
            if entry.get("kind") != "custom":
                continue
            path = devices_file_for_map(map_id)
            custom_data = load_devices_generic(map_id)
            found = False
            for dev in custom_data.get("devices", []):
                if dev.get("id") == dev_id:
                    dev[field] = value
                    found = True
                    dev_name = dev.get("name")
                    dev_ip = dev.get("ip")
            for cat_devs in custom_data.get("categories", {}).values():
                for dev in cat_devs:
                    if dev.get("id") == dev_id:
                        dev[field] = value
            if found:
                tmp_path = path + ".tmp"
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(custom_data, f, ensure_ascii=False, indent=2)
                os.replace(tmp_path, path)
                log_event(event_label_true if value else event_label_false, dev_name or str(dev_id), dev_ip)
                return True

        return False


def set_device_enabled(dev_id, enabled):
    """Ativa/desativa o MONITORAMENTO do dispositivo (para de fazer
    ping/SNMP nele quando desativado)."""
    return _set_device_field(dev_id, "enabled", enabled, "ATIVADO", "DESATIVADO")


def set_device_alert_enabled(dev_id, alert_enabled):
    """Ativa/desativa apenas o ENVIO DE ALARME (Telegram) desse dispositivo.
    O monitoramento (mapa, historico, logs) continua funcionando normalmente
    mesmo com o alarme desativado."""
    return _set_device_field(dev_id, "alert_enabled", alert_enabled, "ALARME ATIVADO", "ALARME DESATIVADO")


def set_device_sound_enabled(dev_id, sound_enabled):
    """Ativa/desativa o AVISO SONORO/VOZ (beep + fala) desse dispositivo
    especificamente, sem afetar o alarme do Telegram nem o monitoramento."""
    return _set_device_field(dev_id, "sound_enabled", sound_enabled, "SOM DO ALARME ATIVADO", "SOM DO ALARME DESATIVADO")


def set_device_ping_enabled(dev_id, ping_enabled):
    """Ativa/desativa apenas o SERVICO DE PING (conectividade) de um
    dispositivo. Os demais servicos/metricas SNMP continuam funcionando
    normalmente mesmo com o ping desativado."""
    return _set_device_field(dev_id, "ping_enabled", ping_enabled, "PING ATIVADO", "PING DESATIVADO")


def set_device_report_enabled(dev_id, report_enabled):
    """Inclui/exclui um dispositivo do Relatorio Mensal de Ocorrencias.
    Nao afeta o monitoramento, alarme, ping ou qualquer outra coisa -
    so decide se as ocorrencias desse dispositivo aparecem no relatorio."""
    return _set_device_field(dev_id, "report_enabled", report_enabled, "RELATORIO ATIVADO", "RELATORIO DESATIVADO")


def set_metric_enabled(dev_id, metric_label, enabled):
    """Ativa/desativa um SERVICO (metrica) especifico de um dispositivo,
    sem afetar os demais servicos nem o monitoramento geral por ping."""
    def _apply(devices_list):
        found = False
        dev_name = None
        for dev in devices_list:
            if dev.get("id") == dev_id:
                for m in dev.get("metrics", []):
                    if m.get("label") == metric_label:
                        m["enabled"] = enabled
                        found = True
                dev_name = dev.get("name")
        return found, dev_name

    with DEVICES_FILE_LOCK:
        data = load_devices()
        found, dev_name = _apply(data.get("devices", []))
        for site_devs in data.get("sites", {}).values():
            _apply(site_devs)

        if found:
            save_devices(data)
            log_event("SERVICO " + ("ATIVADO" if enabled else "DESATIVADO"),
                      f"{dev_name} -> {metric_label}", None)
            return True

        fg_data = load_devices_fg()
        found, dev_name = _apply(fg_data.get("devices", []))
        for cat_devs in fg_data.get("categories", {}).values():
            _apply(cat_devs)

        if found:
            tmp_path = DEVICES_FG_FILE + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(fg_data, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, DEVICES_FG_FILE)
            log_event("SERVICO " + ("ATIVADO" if enabled else "DESATIVADO"),
                      f"{dev_name} -> {metric_label}", None)
            return True

        registry = load_maps_registry()
        for map_id, entry in registry.items():
            if entry.get("kind") != "custom":
                continue
            path = devices_file_for_map(map_id)
            custom_data = load_devices_generic(map_id)
            found, dev_name = _apply(custom_data.get("devices", []))
            for cat_devs in custom_data.get("categories", {}).values():
                _apply(cat_devs)
            if found:
                tmp_path = path + ".tmp"
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(custom_data, f, ensure_ascii=False, indent=2)
                os.replace(tmp_path, path)
                log_event("SERVICO " + ("ATIVADO" if enabled else "DESATIVADO"),
                          f"{dev_name} -> {metric_label}", None)
                return True

        return False


VALID_KINDS = {"radio", "power", "tower", "tx", "sensor", "ups", "link", "other"}


def _new_device_id():
    # espaco de IDs bem acima dos usados pelo Dude, para nunca colidir
    # com os sys-id reais importados do backup.
    return 900000000 + int(time.time() * 10) % 99999999


def find_device_anywhere(dev_id):
    """Procura um dispositivo pelo id em TODOS os mapas (Interior, FG,
    customizados e biblioteca). Retorna (map_id, dispositivo) ou (None, None)."""
    for map_id, entry in load_maps_registry().items():
        data = load_devices_generic(map_id) if map_id not in ("interior",) else load_devices()
        for dev in data.get("devices", []):
            if dev.get("id") == dev_id:
                return map_id, dev
    return None, None


# --------------------------------------------------------------------------
# Rotulos de valor conhecidos por convencao - aplicados automaticamente
# quando o nome da metrica bate com um padrao conhecido, MESMO que o
# usuario nao configure nada explicitamente. Isso evita que "Porta" (ou
# similares) apareca como "0"/"1" cru sempre que alguem adicionar esse
# servico por Configuracoes ou pelo Editor de Mapa.
# --------------------------------------------------------------------------
_KNOWN_VALUE_LABELS_BY_LABEL_PATTERN = [
    (re.compile(r"porta", re.IGNORECASE), {"0": "Porta Aberta", "1": "Porta Fechada"}),
]


def _apply_known_value_labels(label, metric_entry, explicit_value_labels=None):
    """Preenche metric_entry['value_labels'] automaticamente quando o nome
    da metrica bate com um padrao conhecido (ex: qualquer coisa com
    'Porta' no nome vira Aberta/Fechada) - a nao ser que o proprio pedido
    ja tenha mandado um value_labels explicito, que sempre tem prioridade."""
    if explicit_value_labels:
        metric_entry["value_labels"] = explicit_value_labels
        return
    for pattern, labels in _KNOWN_VALUE_LABELS_BY_LABEL_PATTERN:
        if pattern.search(label or ""):
            metric_entry["value_labels"] = labels
            return



def _build_metric_entry(m, protocol="snmp"):
    """Monta uma metrica validada (SNMP ou Modbus) a partir do que veio do
    formulario/importacao. Retorna (metrica, None), (None, erro) ou
    (None, None) quando a linha deve ser ignorada (incompleta)."""
    label = (m.get("label") or "").strip()
    if not label:
        return None, None
    source = (m.get("source") or protocol or "snmp").lower()
    divisor = m.get("divisor")
    try:
        divisor = float(divisor) if divisor not in (None, "") else None
    except (TypeError, ValueError):
        divisor = None
    if divisor == 0:
        divisor = None

    if source == "modbus":
        try:
            address = int(float(m.get("address")))
        except (TypeError, ValueError):
            return None, None
        if address < 0 or address > 65535:
            return None, f"endereco Modbus invalido: {address}"
        entry = {
            "label": label,
            "source": "modbus",
            "register_type": normalize_register_type(m.get("register_type")),
            "address": address,
            "data_type": normalize_data_type(m.get("data_type")),
            "unit": (m.get("unit") or "").strip(),
            "divisor": divisor,
        }
        ws = parse_swap_flag(m.get("word_swap"), None)
        if ws is not None:
            entry["word_swap"] = ws
        ds = parse_swap_flag(m.get("dword_swap"), None)
        if ds is not None:
            entry["dword_swap"] = ds
    else:
        oid = (m.get("oid") or "").strip().strip(".")
        if not oid:
            return None, None
        if not re.match(r"^\d+(\.\d+)+$", oid):
            return None, f"OID invalido: {oid}"
        entry = {
            "label": label,
            "oid": oid,
            "unit": (m.get("unit") or "").strip(),
            "divisor": divisor,
            "community": (m.get("community") or "public").strip(),
            "version": str(m.get("version") or "1"),
        }

    # limite de alarme no mesmo estilo do The Dude (snmpCompareMethod):
    # 1=igual 2=diferente 3=menor 4=menor-ou-igual 5=maior 6=maior-ou-igual
    cm = m.get("compare_method")
    cv = m.get("compare_value")
    if cm not in (None, "") and cv not in (None, ""):
        try:
            entry["compare_method"] = int(cm)
            entry["compare_value"] = float(cv)
        except (TypeError, ValueError):
            pass
    try:
        off = float(str(m.get("offset")).replace(",", ".")) if m.get("offset") not in (None, "") else 0.0
    except (TypeError, ValueError):
        off = 0.0
    if off:
        entry["offset"] = round(off, 4)
    if "enabled" in m and m.get("enabled") is not None:
        entry["enabled"] = bool(m.get("enabled"))
    if m.get("hide_on_map"):
        entry["hide_on_map"] = True
    if m.get("rate_mode"):
        entry["rate_mode"] = m.get("rate_mode")
    if m.get("pair_with"):
        entry["pair_with"] = m.get("pair_with")
    if m.get("desc"):
        entry["desc"] = str(m.get("desc"))[:200]
    _apply_known_value_labels(label, entry, m.get("value_labels"))
    return entry, None


def _build_protocol_fields(payload):
    """Campos de protocolo do dispositivo (SNMP x Modbus TCP)."""
    protocol = (payload.get("protocol") or "snmp").strip().lower()
    if protocol != "modbus":
        return "snmp", None
    cfg_in = payload.get("modbus") or {}
    def _int(v, default):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return default
    cfg = {
        "port": _int(cfg_in.get("port"), MODBUS_DEFAULT_PORT),
        "unit_id": _int(cfg_in.get("unit_id"), 1),
        "word_swap": parse_swap_flag(cfg_in.get("word_swap"), True),
        "dword_swap": parse_swap_flag(cfg_in.get("dword_swap"), True),
        "timeout_ms": _int(cfg_in.get("timeout_ms"), int(MODBUS_DEFAULT_TIMEOUT_S * 1000)),
    }
    if cfg_in.get("description"):
        cfg["description"] = str(cfg_in.get("description"))[:120]
    return "modbus", cfg


def _build_metrics_list(metrics_in, protocol):
    metrics = []
    seen = set()
    for m in metrics_in or []:
        entry, error = _build_metric_entry(m, protocol)
        if error:
            return None, error
        if entry is None:
            continue
        # o rotulo e a chave da metrica (status/historico) - nao pode repetir
        base = entry["label"]
        n = 2
        while entry["label"] in seen:
            entry["label"] = f"{base} ({n})"
            n += 1
        seen.add(entry["label"])
        metrics.append(entry)
    return metrics, None


def _validate_device_fields(payload, require_name=True):
    """Valida nome/ip/kind/protocolo/metricas no mesmo formato usado ao
    adicionar um dispositivo. Retorna (campos_validados, None) ou (None, erro)."""
    name = (payload.get("name") or "").strip()
    ip = (payload.get("ip") or "").strip() or None
    kind = (payload.get("kind") or "other").strip().lower()

    if require_name and not name:
        return None, "nome e obrigatorio"
    if kind not in VALID_KINDS:
        kind = "other"
    if ip and not re.match(r"^\d{1,3}(\.\d{1,3}){3}$", ip):
        return None, "IP invalido"

    protocol, modbus_cfg = _build_protocol_fields(payload)
    metrics, error = _build_metrics_list(payload.get("metrics"), protocol)
    if error:
        return None, error

    return {"name": name, "ip": ip, "kind": kind, "metrics": metrics,
            "protocol": protocol, "modbus": modbus_cfg}, None


def edit_device_anywhere(dev_id, payload):
    """Edita nome/IP/tipo/metricas (OIDs, SNMP, limites de alarme) de um
    dispositivo, em qualquer mapa em que ele estiver - usado pelo botao
    'Editar' no Editor de Mapa. Mantem intactos id, site/categoria e os
    interruptores (Monitorar/Alarme/Ping/Som/Relatorio)."""
    validated, error = _validate_device_fields(payload)
    if error:
        return None, error

    map_id, _ = find_device_anywhere(dev_id)
    if map_id is None:
        return None, "dispositivo nao encontrado em nenhum mapa"

    devices_path = devices_file_for_map(map_id)
    with DEVICES_FILE_LOCK:
        data = load_devices() if map_id == "interior" else load_devices_generic(map_id)
        target = None
        for dev in data.get("devices", []):
            if dev.get("id") == dev_id:
                target = dev
                break
        if target is None:
            return None, "dispositivo nao encontrado (concorrencia?)"

        def _apply(dev):
            dev["name"] = validated["name"]
            dev["ip"] = validated["ip"]
            dev["kind"] = validated["kind"]
            dev["metrics"] = validated["metrics"]
            if validated["protocol"] == "modbus":
                dev["protocol"] = "modbus"
                dev["modbus"] = validated["modbus"]
            else:
                dev.pop("protocol", None)
                dev.pop("modbus", None)

        _apply(target)

        # atualiza tambem a copia dentro do agrupamento por site/categoria
        group_key = _group_key_for_map(data)
        for group_devs in data.get(group_key, {}).values():
            for dev in group_devs:
                if dev.get("id") == dev_id and dev is not target:
                    _apply(dev)

        tmp_path = devices_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, devices_path)

    return target, None


def _group_key_for_map(data):
    """Retorna o nome da chave de agrupamento usada por esse mapa:
    'sites' (Mapa Interior) ou 'categories' (FG/customizados/biblioteca)."""
    return "sites" if "sites" in data else "categories"


def remove_device_from_map(map_id, dev_id):
    """Remove um dispositivo (por id) de qualquer mapa, tanto da lista
    plana 'devices' quanto do agrupamento por site/categoria. Retorna o
    dispositivo removido (dict) ou None se nao foi encontrado."""
    devices_path = devices_file_for_map(map_id)
    with DEVICES_FILE_LOCK:
        data = load_devices() if map_id == "interior" else load_devices_generic(map_id)
        removed = None
        remaining = []
        for dev in data.get("devices", []):
            if dev.get("id") == dev_id and removed is None:
                removed = dev
            else:
                remaining.append(dev)
        if removed is None:
            return None
        data["devices"] = remaining

        group_key = _group_key_for_map(data)
        for group_devs in data.get(group_key, {}).values():
            group_devs[:] = [d for d in group_devs if d.get("id") != dev_id]

        tmp_path = devices_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, devices_path)
        return removed


def add_full_device_to_map(map_id, device, group_name):
    """Insere um dispositivo JA PRONTO (com todos os campos, metricas,
    OIDs etc.) num mapa, no grupo/site indicado - usado ao colar da
    biblioteca de volta em um mapa ativo."""
    devices_path = devices_file_for_map(map_id)
    with DEVICES_FILE_LOCK:
        data = load_devices() if map_id == "interior" else load_devices_generic(map_id)
        device = dict(device)  # copia, pra nao compartilhar referencia com a biblioteca
        device["site"] = group_name
        if "category" in device or _group_key_for_map(data) == "categories":
            device["category"] = group_name

        data.setdefault("devices", []).append(device)
        group_key = _group_key_for_map(data)
        data.setdefault(group_key, {}).setdefault(group_name, []).append(device)
        order_key = "sites_order" if group_key == "sites" else "categories_order"
        if group_name not in data.setdefault(order_key, []):
            data[order_key].append(group_name)

        tmp_path = devices_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, devices_path)
    return device


def move_device_to_library(dev_id):
    """'Recorta' um dispositivo do mapa onde ele estiver e guarda ele
    DESATIVADO na Biblioteca, preservando OIDs/SNMP/limites de alarme -
    para reaproveitar depois quando o equipamento for trocado/reinstalado."""
    source_map, _ = find_device_anywhere(dev_id)
    if source_map is None:
        return None, "dispositivo nao encontrado em nenhum mapa"
    if source_map == "biblioteca":
        return None, "esse dispositivo ja esta na biblioteca"

    removed = remove_device_from_map(source_map, dev_id)
    if removed is None:
        return None, "falha ao remover o dispositivo do mapa de origem"

    removed["enabled"] = False
    removed["_original_map"] = source_map
    removed["_original_site"] = removed.get("site") or removed.get("category")
    saved = add_full_device_to_map("biblioteca", removed, "Arquivados")
    return saved, None


def paste_device_from_library(dev_id, target_map, target_site):
    """'Cola' um dispositivo guardado na Biblioteca de volta num mapa
    ativo, reativando-o com a mesma configuracao de OIDs/SNMP/alarmes."""
    removed = remove_device_from_map("biblioteca", dev_id)
    if removed is None:
        return None, "dispositivo nao encontrado na biblioteca"

    removed["enabled"] = True
    removed.pop("_original_map", None)
    removed.pop("_original_site", None)
    saved = add_full_device_to_map(target_map, removed, target_site or "GERAL")
    return saved, None



def add_device_to_map(map_id, payload):
    """Versao generica do add_device() - funciona para o Mapa FG e para
    qualquer mapa novo criado pelo usuario (todos usam o formato de
    categorias). Retorna (dispositivo_criado, None) ou (None, erro)."""
    name = (payload.get("name") or "").strip()
    category = (payload.get("category") or payload.get("site") or "GERAL").strip() or "GERAL"
    ip = (payload.get("ip") or "").strip() or None
    kind = (payload.get("kind") or "other").strip().lower()
    metrics_in = payload.get("metrics") or []

    if not name:
        return None, "nome e obrigatorio"
    if kind not in VALID_KINDS:
        kind = "other"
    if ip and not re.match(r"^\d{1,3}(\.\d{1,3}){3}$", ip):
        return None, "IP invalido"

    protocol, modbus_cfg = _build_protocol_fields(payload)
    metrics, error = _build_metrics_list(metrics_in, protocol)
    if error:
        return None, error

    new_dev = {
        "id": _new_device_id(),
        "name": name,
        "ip": ip,
        "kind": kind,
        "category": category,
        "site": category,
        "metrics": metrics,
        "enabled": True,
    }
    if protocol == "modbus":
        new_dev["protocol"] = "modbus"
        new_dev["modbus"] = modbus_cfg

    devices_path = devices_file_for_map(map_id)
    with DEVICES_FILE_LOCK:
        data = load_devices_generic(map_id)
        data.setdefault("devices", []).append(new_dev)
        data.setdefault("categories", {}).setdefault(category, []).append(new_dev)
        if category not in data.setdefault("categories_order", []):
            data["categories_order"].append(category)

        tmp_path = devices_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, devices_path)

    return new_dev, None


def add_device(payload):
    """Valida e adiciona um novo dispositivo ao network.json (lista
    'devices', dentro de 'sites' e em 'sites_order' se for um site novo).
    Retorna (dispositivo_criado, None) ou (None, mensagem_de_erro)."""
    name = (payload.get("name") or "").strip()
    site = (payload.get("site") or "").strip()
    ip = (payload.get("ip") or "").strip() or None
    kind = (payload.get("kind") or "other").strip().lower()
    metrics_in = payload.get("metrics") or []

    if not name:
        return None, "nome e obrigatorio"
    if not site:
        return None, "site e obrigatorio"
    if kind not in VALID_KINDS:
        kind = "other"
    if ip:
        if not re.match(r"^\d{1,3}(\.\d{1,3}){3}$", ip):
            return None, "IP invalido"

    protocol, modbus_cfg = _build_protocol_fields(payload)
    metrics, error = _build_metrics_list(metrics_in, protocol)
    if error:
        return None, error

    new_dev = {
        "id": _new_device_id(),
        "site": site,
        "name": name,
        "ip": ip,
        "kind": kind,
        "metrics": metrics,
        "enabled": True,
    }
    if protocol == "modbus":
        new_dev["protocol"] = "modbus"
        new_dev["modbus"] = modbus_cfg

    with DEVICES_FILE_LOCK:
        data = load_devices()
        data.setdefault("devices", []).append(new_dev)
        data.setdefault("sites", {}).setdefault(site, []).append(new_dev)
        if site not in data.setdefault("sites_order", []):
            data["sites_order"].append(site)
        save_devices(data)

    return new_dev, None


def format_value(raw_value, metric):
    if raw_value is None:
        return None
    try:
        num = float(raw_value)
    except (TypeError, ValueError):
        return str(raw_value)

    # metricas de status (ex: porta aberta/fechada) podem definir um
    # dicionario "value_labels" mapeando o valor bruto para um texto,
    # em vez de mostrar o numero cru na notificacao/mapa
    value_labels = metric.get("value_labels")
    if value_labels:
        key = str(int(num)) if num == int(num) else str(num)
        if key in value_labels:
            return value_labels[key]

    divisor = metric.get("divisor")
    if divisor:
        num = num / float(divisor)
    if metric.get("offset"):
        num = num + float(metric["offset"])   # ajuste de calibracao (ex: sonda marcando 1,5 C a mais -> -1,5)
    unit = metric.get("unit") or ""
    # potencias grandes em W/VA/var aparecem em k (29148 W -> 29,15 kW)
    if unit in ("W", "VA", "var") and abs(num) >= 10000:
        num, unit = num / 1000.0, "k" + unit
    decimals = UNIT_DECIMALS.get(unit)
    if decimals is None:
        decimals = 0 if num == int(num) else 2
    num_str = _fmt_br(num, decimals)
    return f"{num_str}{unit}".strip() if unit in ("%", "°C") else f"{num_str} {unit}".strip()


# casas decimais por unidade (o valor guardado/gráfico continua com a precisao total)
UNIT_DECIMALS = {
    "V": 1, "kV": 2, "A": 1, "Hz": 1, "°C": 1, "%": 0,
    "W": 0, "VA": 0, "var": 0, "kW": 2, "kVA": 2, "kvar": 2, "kWh": 0, "Wh": 0,
    "h": 0, "min": 0, "L": 0, "rpm": 0, "bar": 1, "dB": 1, "dBm": 1,
}


def _fmt_br(num, decimals):
    """1234.5 -> '1.234,5' (padrao brasileiro)."""
    txt = f"{num:,.{decimals}f}"
    return txt.replace(",", "\x00").replace(".", ",").replace("\x00", ".")


def evaluate_threshold(raw_value, compare_method, compare_value):
    """Reproduz a logica de qualificacao de servico do proprio The Dude:
    cada probe SNMP tem um metodo de comparacao (snmpCompareMethod) e um
    valor de referencia (snmpValueNumber), extraidos diretamente do
    dude.db. Se a comparacao FALHAR, o Dude marcaria o servico como
    problema/alarme (equivalente ao servico ficar vermelho no mapa).

    Retorna True (dentro do esperado / ok) ou False (fora do limite / alarme).
    Se o metodo nao for reconhecido, assume-se "sem verificacao" -> True.
    """
    if compare_method is None or raw_value is None:
        return True
    try:
        value = float(raw_value)
        threshold = float(compare_value)
    except (TypeError, ValueError):
        return True

    if compare_method == 1:      # igual
        return value == threshold
    if compare_method == 2:      # diferente
        return value != threshold
    if compare_method == 3:      # menor que
        return value < threshold
    if compare_method == 4:      # menor ou igual
        return value <= threshold
    if compare_method == 5:      # maior que
        return value > threshold
    if compare_method == 6:      # maior ou igual
        return value >= threshold
    return True  # metodo nao mapeado (7/8/0) -> nao qualifica, so informativo


# --------------------------------------------------------------------------
# Notificacao Telegram
# --------------------------------------------------------------------------
TELEGRAM_LAST = {"ok_at": None, "error": None, "error_at": None}


def _telegram_api(token, method, params=None, timeout=10):
    """Chamada simples a API do Telegram. Retorna o JSON de resposta
    (levanta excecao com a descricao do Telegram quando der erro)."""
    url = f"https://api.telegram.org/bot{token}/{method}"
    data = urllib.parse.urlencode(params or {}).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            desc = json.loads(e.read().decode("utf-8")).get("description")
        except Exception:
            desc = None
        raise RuntimeError(desc or f"HTTP {e.code}")


def send_telegram_message(text, force=False, token=None, chat_id=None):
    """Envia para o grupo configurado. Tenta 3 vezes (rede instavel) e
    guarda o ultimo erro para aparecer na tela de configuracao."""
    if not force and not TELEGRAM_ENABLED:
        return False
    token = token or TELEGRAM_BOT_TOKEN
    chat_id = chat_id or TELEGRAM_CHAT_ID
    if not token or not chat_id:
        return False
    last_err = None
    for attempt in range(3):
        try:
            _telegram_api(token, "sendMessage", {"chat_id": chat_id, "text": text,
                                                 "disable_web_page_preview": "true"})
            TELEGRAM_LAST["ok_at"] = time.time()
            return True
        except Exception as e:
            last_err = str(e)
            # erro de configuracao (token/chat errado): nao adianta insistir
            if any(k in last_err.lower() for k in ("unauthorized", "not found", "chat not found", "kicked", "forbidden")):
                break
            time.sleep(2 + attempt * 3)
    TELEGRAM_LAST["error"] = last_err
    TELEGRAM_LAST["error_at"] = time.time()
    print("[telegram] erro ao enviar notificacao:", last_err, file=sys.stderr)
    return False


def telegram_public_config():
    tok = TELEGRAM_BOT_TOKEN
    masked = (tok[:6] + "..." + tok[-4:]) if len(tok) > 12 else ("" if not tok else "***")
    return {
        "enabled": TELEGRAM_ENABLED,
        "has_token": bool(tok),
        "token_masked": masked,
        "chat_id": TELEGRAM_CHAT_ID,
        "last_ok_at": TELEGRAM_LAST["ok_at"],
        "last_error": TELEGRAM_LAST["error"],
        "last_error_at": TELEGRAM_LAST["error_at"],
    }


def save_telegram_config(enabled, token, chat_id):
    global TELEGRAM_ENABLED, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    token = (token or "").strip() or TELEGRAM_BOT_TOKEN   # vazio = mantem o atual
    chat_id = (chat_id or "").strip()
    data = {"enabled": bool(enabled), "bot_token": token, "chat_id": chat_id}
    tmp = TELEGRAM_CONFIG_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, TELEGRAM_CONFIG_FILE)
    TELEGRAM_BOT_TOKEN = token
    TELEGRAM_CHAT_ID = chat_id
    TELEGRAM_ENABLED = bool(enabled) and bool(token) and bool(chat_id)
    return telegram_public_config()


def telegram_discover_chats(token=None):
    """Lista os grupos em que o bot recebeu mensagens (getUpdates) - serve
    para descobrir o chat_id do grupo sem precisar de outro programa."""
    token = (token or "").strip() or TELEGRAM_BOT_TOKEN
    if not token:
        raise RuntimeError("informe o token do bot")
    me = _telegram_api(token, "getMe").get("result", {})
    upd = _telegram_api(token, "getUpdates", {"limit": 100, "timeout": 0}).get("result", [])
    chats = {}
    for u in upd:
        for key in ("message", "channel_post", "my_chat_member", "edited_message"):
            obj = u.get(key)
            if not obj:
                continue
            c = obj.get("chat") or {}
            if c.get("id") is None:
                continue
            chats[str(c["id"])] = {"id": str(c["id"]), "type": c.get("type"),
                                   "title": c.get("title") or c.get("username") or c.get("first_name") or ""}
    return {"bot": me.get("username"), "chats": list(chats.values())}


def _notify_async(text):
    threading.Thread(target=send_telegram_message, args=(text,), daemon=True).start()


# --------------------------------------------------------------------------
# Confirmacao de mudanca de estado (equivalente ao "down count" do Dude):
# so notifica depois de NOTIFY_CONFIRM_COUNT leituras seguidas confirmando
# a mudanca, evitando alarme falso por uma oscilacao passageira.
# --------------------------------------------------------------------------
NOTIFY_LOCK = threading.Lock()
_confirmed_state = {}   # dev_id -> {"reachable": bool|None, "metrics": {label: state}}
_pending_state = {}     # dev_id -> {"reachable": {"candidate":..,"count":0}, "metrics": {label: {...}}}


def _check_transition(dev_id, key, current_value, is_metric=False, metric_label=None, confirm_count=None):
    """Retorna o valor CONFIRMADO se a mudanca acabou de ser confirmada
    (ou seja, atingiu 'confirm_count' leituras seguidas iguais e
    diferentes do valor confirmado anterior). Retorna None caso contrario
    (ainda nao confirmado, ou nao houve mudanca)."""
    if confirm_count is None:
        confirm_count = load_alarm_confirm_config()["metric_confirm_count"]

    conf = _confirmed_state.setdefault(dev_id, {"reachable": None, "metrics": {}})
    pend = _pending_state.setdefault(dev_id, {"reachable": {"candidate": None, "count": 0}, "metrics": {}})

    if is_metric:
        if metric_label not in conf["metrics"]:
            conf["metrics"][metric_label] = current_value
            return None
        confirmed_value = conf["metrics"][metric_label]
        pend_entry = pend["metrics"].setdefault(metric_label, {"candidate": None, "count": 0})
    else:
        if conf["reachable"] is None:
            conf["reachable"] = current_value
            return None
        confirmed_value = conf["reachable"]
        pend_entry = pend["reachable"]

    if current_value == confirmed_value:
        pend_entry["candidate"] = None
        pend_entry["count"] = 0
        return None

    if pend_entry["candidate"] == current_value:
        pend_entry["count"] += 1
    else:
        pend_entry["candidate"] = current_value
        pend_entry["count"] = 1

    if pend_entry["count"] >= confirm_count:
        if is_metric:
            conf["metrics"][metric_label] = current_value
        else:
            conf["reachable"] = current_value
        pend_entry["candidate"] = None
        pend_entry["count"] = 0
        return current_value

    return None


def notify_status_changes(device, prev, result):
    """Reproduz o mesmo formato de alerta usado pela notificacao
    'Telegran Grande Vitoria' do proprio Dude (ALERTA FG / EQUIPAMENTO /
    STATUS / IP), acrescentando o valor atual do OID quando disponivel.
    So dispara depois de NOTIFY_CONFIRM_COUNT leituras seguidas confirmando
    a mudanca (evita alarme falso por oscilacao passageira)."""
    if result.get("disabled"):
        return  # dispositivo desativado manualmente: nao gera alerta

    dev_id = device.get("id")
    name = device.get("name", "?")
    ip = device.get("ip") or "sem IP"
    map_name = device.get("map", "interior")
    telegram_allowed = is_within_alarm_window(map_name) and device.get("alert_enabled", True)
    templates = get_message_template_for_map(map_name)

    with NOTIFY_LOCK:
        # 1) PERDA DE COMUNICACAO (ping / conexao Modbus): so avisa, grava no
        #    log e abre incidente depois de N minutos SEGUIDOS sem resposta
        #    (padrao 15 min). Quedas curtas nao geram mensagem nem log.
        hold_s = load_polling_config()["comm_loss_min"] * 60
        now = time.time()
        cs = _COMM_STATE.setdefault(dev_id, {"down_since": None, "down_alerted": False,
                                            "err_since": None, "err_alerted": False, "err_labels": []})
        reachable = result.get("reachable")
        if reachable is False:
            if cs["down_since"] is None:
                cs["down_since"] = now
            if not cs["down_alerted"] and now - cs["down_since"] >= hold_s:
                cs["down_alerted"] = True
                mins = int((now - cs["down_since"]) // 60)
                status_txt = f"DOWN (sem comunicação há {mins} min)"
                text = render_message_template(templates.get("ping", DEFAULT_PING_TEMPLATE),
                                               equipamento=name, status=status_txt, ip=ip, abrigo=abrigo_alert_name(device))
                publish_alert(text, telegram=telegram_allowed)
                log_event("SEM COMUNICACAO", name, ip, detail=f"sem resposta ha {mins} min")
                open_or_update_incident(device, "ping", "Ping", f"Sem comunicacao ha {mins} min (equipamento offline)", None)
        elif reachable:
            if cs["down_alerted"]:
                mins = int((now - (cs["down_since"] or now)) // 60)
                text = render_message_template(templates.get("ping", DEFAULT_PING_TEMPLATE),
                                               equipamento=name, status=f"UP (voltou após {mins} min)", ip=ip,
                                               abrigo=abrigo_alert_name(device))
                publish_alert(text, telegram=telegram_allowed)
                log_event("COMUNICACAO NORMALIZADA", name, ip, detail=f"ficou {mins} min sem comunicacao")
                close_incident(dev_id, "ping", "Ping")
            cs["down_since"] = None
            cs["down_alerted"] = False

        # 2) mudanca de estado em cada metrica SNMP (ok <-> alarm <-> error)
        device_metrics_cfg = {m["label"]: m for m in device.get("metrics", [])}
        error_labels = []
        for label, entry in (result.get("metrics") or {}).items():
            # metricas marcadas "hide_on_map" sao so pra diagnostico interno
            # do painel (ex: leituras detalhadas de PA/excitador que nao tem
            # limiar de alarme oficial) - nunca devem gerar notificacao no
            # Telegram nem contar pra abertura de incidente, mesmo que a
            # leitura SNMP falhe (estado "error") ou o valor pareca fora da
            # faixa - so as metricas realmente visiveis no mapa alarmam.
            cfg_m = device_metrics_cfg.get(label, {})
            curr_state = entry.get("state")
            if cfg_m.get("hide_on_map"):
                # escondida da caixinha: so avisa se tiver alarme configurado
                # de proposito (ex: "Motor ligado" do gerador), e nunca por
                # simples falta de leitura
                if cfg_m.get("compare_method") in (None, "") or curr_state == "error":
                    continue

            # "sem leitura" nao vira alerta por medida: e tratado junto, por
            # equipamento, com a regra dos N minutos (logo abaixo)
            if curr_state == "error":
                error_labels.append(label)
                continue
            confirmed_state = _check_transition(dev_id, "metric", curr_state, is_metric=True, metric_label=label)
            if confirmed_state is None or confirmed_state == "error":
                continue

            if confirmed_state == "alarm":
                status_txt = "ALARME"
            else:
                status_txt = "OK"

            value_txt = entry.get("display") or "sem leitura"
            text = render_message_template(
                templates.get("metric", DEFAULT_METRIC_TEMPLATE),
                equipamento=name, status=status_txt, ip=ip, metrica=label, valor=value_txt,
                abrigo=abrigo_alert_name(device),
            )
            publish_alert(text, telegram=telegram_allowed)
            log_event(status_txt, name, ip, detail=f"{label}: {value_txt}")

            if confirmed_state == "ok":
                close_incident(dev_id, "metric", label)
            else:
                metric_cfg = next((m for m in device.get("metrics", []) if m["label"] == label), {})
                reason = build_metric_reason(
                    label, confirmed_state, value_txt,
                    metric_cfg.get("compare_method"), metric_cfg.get("compare_value"),
                    metric_cfg.get("unit"),
                )
                open_or_update_incident(device, "metric", label, reason, value_txt)

        # 3) SEM LEITURA (equipamento responde, mas SNMP/Modbus nao): UMA
        #    mensagem por equipamento, so depois de N minutos seguidos
        proto = "MODBUS" if is_modbus_device(device) else "SNMP"
        if error_labels and reachable:
            if cs["err_since"] is None:
                cs["err_since"] = now
            cs["err_labels"] = error_labels
            if not cs["err_alerted"] and now - cs["err_since"] >= hold_s:
                cs["err_alerted"] = True
                mins = int((now - cs["err_since"]) // 60)
                lista = ", ".join(error_labels[:8]) + (f" (+{len(error_labels) - 8})" if len(error_labels) > 8 else "")
                text = (f"🟠 ALERTA {abrigo_alert_name(device)}\n\nSEM LEITURA {proto} há {mins} min\n"
                        f"EQUIPAMENTO: {name}\nIP={ip}\nMEDIDAS: {lista}")
                publish_alert(text, telegram=telegram_allowed)
                log_event(f"SEM LEITURA {proto}", name, ip, detail=f"{len(error_labels)} medida(s) ha {mins} min: {lista}")
                open_or_update_incident(device, "metric", "Sem leitura", f"Sem leitura {proto} ha {mins} min ({lista})", None)
        elif not error_labels:
            if cs["err_alerted"]:
                mins = int((now - (cs["err_since"] or now)) // 60)
                text = (f"✅ ALERTA {abrigo_alert_name(device)}\n\nLEITURA {proto} NORMALIZADA (após {mins} min)\n"
                        f"EQUIPAMENTO: {name}\nIP={ip}")
                publish_alert(text, telegram=telegram_allowed)
                log_event(f"LEITURA {proto} NORMALIZADA", name, ip, detail=f"ficou {mins} min sem leitura")
                close_incident(dev_id, "metric", "Sem leitura")
            cs["err_since"] = None
            cs["err_alerted"] = False


_COMM_STATE = {}   # dev_id -> controle da regra "N minutos sem comunicacao"


# --------------------------------------------------------------------------
# Loop de polling em background
# --------------------------------------------------------------------------
def _fetch_raw_metric_value(ip, metric):
    """So busca o valor bruto (SNMP ou espelho do Dude) de UMA metrica -
    usado em paralelo pra varias metricas do mesmo dispositivo ao mesmo
    tempo, em vez de esperar uma consulta terminar pra comecar a proxima.
    Isso importa principalmente em enlaces de radio com latencia mais alta,
    onde varias metricas sequenciais podiam atrasar bastante a atualizacao
    do dispositivo inteiro."""
    dsid = metric.get("dude_datasource_id")
    if dsid:
        raw = read_dude_datasource_value(dsid)
        if raw is None and metric.get("oid"):
            raw = snmp_get(
                ip,
                metric.get("community", "public"),
                metric["oid"],
                version=metric.get("version", "1"),
            )
        return raw
    return snmp_get(
        ip,
        metric.get("community", "public"),
        metric["oid"],
        version=metric.get("version", "1"),
    )


_METRIC_FETCH_POOL = ThreadPoolExecutor(max_workers=20, thread_name_prefix="metricfetch")


def poll_device(device):
    dev_id = device.get("id")
    ip = device.get("ip")
    metrics_cfg = device.get("metrics", [])
    result = {
        "reachable": False,
        "latency_ms": None,
        "metrics": {},        # label -> {value, display, state}
        "disabled": False,
        "updated_at": time.time(),
    }

    if device.get("enabled", True) is False:
        result["disabled"] = True
        with STATUS_LOCK:
            STATUS[dev_id] = result
        return

    if not ip:
        with STATUS_LOCK:
            STATUS[dev_id] = result
        return

    active_metrics = [m for m in metrics_cfg if m.get("enabled", True) is not False]
    raw_values = {}
    modbus_mode = is_modbus_device(device)

    if modbus_mode:
        # Modbus TCP: a propria conexao TCP (porta 502) faz o papel do ping.
        # Se conectou mas o escravo nao respondeu NENHUMA leitura, conta
        # como offline (ex: medidor desligado atras de um gateway que esta ok).
        connected, latency, raw_values, mb_error = modbus_read_device(device, active_metrics)
        any_ok = any(v is not None for v in raw_values.values())
        reachable, held = modbus_apply_tolerance(device.get("id"), connected, raw_values)
        result["reachable"] = bool(reachable or (connected and not active_metrics))
        result["latency_ms"] = latency if any_ok else None
        result["mac"] = None
        result["protocol"] = "modbus"
        if held:
            result["modbus_held"] = held   # valores mostrados sao da ultima leitura boa
        if mb_error and not any_ok:
            result["error"] = mb_error
    elif device.get("ping_enabled", True) is False:
        # ping desativado como servico: nao verifica conectividade, so
        # considera "alcancavel" pra nao gerar falso alarme de offline -
        # o dispositivo continua sendo avaliado pelos demais servicos/metricas
        result["reachable"] = True
        result["latency_ms"] = None
        result["mac"] = None
    else:
        reachable, latency = ping_host(ip)
        result["reachable"] = reachable
        result["latency_ms"] = latency
        # o ping recem-feito garante uma entrada fresca na tabela ARP,
        # entao aproveitamos pra descobrir o MAC do mesmo IP
        result["mac"] = get_mac_from_ip(ip) if reachable else None

    # busca o valor BRUTO de todas as metricas ativas EM PARALELO - so
    # depois disso processa (calculo de taxa, alarme, formatacao) uma por
    # uma, com cada leitura ja em mao. Uma metrica lenta/travada nao
    # segura mais as outras na fila.
    if active_metrics and not modbus_mode:
        futures = {
            _METRIC_FETCH_POOL.submit(_fetch_raw_metric_value, ip, m): m["label"]
            for m in active_metrics
        }
        for future in futures:
            label = futures[future]
            try:
                raw_values[label] = future.result(timeout=SNMP_TIMEOUT_S * SNMP_RETRY_COUNT + 2)
            except Exception:
                raw_values[label] = None

    for metric in active_metrics:

        try:
            raw = raw_values.get(metric["label"])

            if raw is None:
                state = "error"          # sem resposta SNMP (servico com problema)
                scaled = None
                display = None
            elif metric.get("rate_mode") == "counter32_mbps":
                # metrica de trafego (contador acumulado) - calcula a taxa em
                # Mbps com base na diferenca entre esta leitura e a anterior.
                # alguns equipamentos mandam esse contador marcado como
                # "INTEGER com sinal" em vez de "Counter32 sem sinal" - nesse
                # caso, qualquer valor real acima de ~2.1 bilhoes chega aqui
                # como numero NEGATIVO por erro de interpretacao do proprio
                # equipamento. Como bytes trafegados nunca sao negativos,
                # corrige reinterpretando como um valor de 32 bits sem sinal.
                raw_counter = float(raw)
                if raw_counter < 0:
                    raw_counter += COUNTER32_MAX
                rate = compute_counter_rate_mbps(dev_id, metric["label"], raw_counter, time.time())
                if rate is None:
                    state = "ok"
                    scaled = None
                    display = "coletando..."   # primeira leitura, ainda sem taxa pra comparar
                else:
                    in_range = evaluate_threshold(rate, metric.get("compare_method"), metric.get("compare_value"))
                    state = "ok" if in_range else "alarm"
                    scaled = rate
                    display = f"{rate:.2f} Mb/s"
            else:
                scaled = None
                try:
                    scaled = float(raw)
                    divisor = metric.get("divisor")
                    if divisor:
                        scaled = scaled / float(divisor)
                    if metric.get("offset"):
                        scaled = scaled + float(metric["offset"])
                except (TypeError, ValueError):
                    scaled = None
                # o valor de referencia do alarme e comparado com o valor JA
                # convertido (o mesmo numero que aparece na tela: 108 V, nao 1080)
                in_range = evaluate_threshold(scaled if scaled is not None else raw,
                                              metric.get("compare_method"), metric.get("compare_value"))
                state = "ok" if in_range else "alarm"
                display = format_value(raw, metric)

            entry = {
                "ok": raw is not None,
                "state": state,
                "value": raw,
                "scaled": scaled,
                "display": display,
                "unit": metric.get("unit", ""),
            }
        except Exception as e:
            # uma metrica com problema (erro de configuracao, valor
            # inesperado, etc.) nao pode travar a atualizacao das OUTRAS
            # metricas nem deixar o dispositivo inteiro congelado no
            # ultimo valor bom - registra o erro nessa metrica especifica
            # e segue processando as demais normalmente
            log_event("ERRO AO LER METRICA " + repr(metric.get("label")), device.get("name", "?"), str(e))
            entry = {
                "ok": False,
                "state": "error",
                "value": None,
                "scaled": None,
                "display": None,
                "unit": metric.get("unit", ""),
            }
        result["metrics"][metric["label"]] = entry

    with STATUS_LOCK:
        prev = STATUS.get(dev_id)
        STATUS[dev_id] = result

    notify_status_changes(device, prev, result)
    record_history(dev_id, result)



TICK_SECONDS = 2   # granularidade de verificacao; nao eh o intervalo de leitura em si

# --------------------------------------------------------------------------
# Velocidade de leitura por grupo (Config > Velocidade de atualizacao):
#   fg_interval_s       -> todos os equipamentos do Abrigo Fonte Grande
#   abrigos_interval_s  -> equipamentos dos demais abrigos (sem intervalo proprio)
# --------------------------------------------------------------------------
POLLING_CONFIG_FILE = os.path.join(BASE_DIR, "polling_config.json")
DEFAULT_POLLING_CONFIG = {"fg_interval_s": 5, "abrigos_interval_s": 5, "comm_loss_min": 15}
_POLLING_CACHE = {"ts": 0, "cfg": None}


def load_polling_config():
    now = time.time()
    if _POLLING_CACHE["cfg"] is not None and now - _POLLING_CACHE["ts"] < 5:
        return _POLLING_CACHE["cfg"]
    cfg = dict(DEFAULT_POLLING_CONFIG)
    try:
        with open(POLLING_CONFIG_FILE, encoding="utf-8") as f:
            cfg.update({k: v for k, v in json.load(f).items() if k in DEFAULT_POLLING_CONFIG})
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        pass
    for k in cfg:
        lo, hi = (1, 240) if k == "comm_loss_min" else (5, 300)
        try:
            cfg[k] = max(lo, min(hi, int(cfg[k])))
        except (TypeError, ValueError):
            cfg[k] = DEFAULT_POLLING_CONFIG[k]
    _POLLING_CACHE.update(ts=now, cfg=cfg)
    return cfg


def save_polling_config(data):
    cfg = dict(load_polling_config())
    for k in DEFAULT_POLLING_CONFIG:
        if k in data:
            lo, hi = (1, 240) if k == "comm_loss_min" else (5, 300)
            cfg[k] = max(lo, min(hi, int(data[k])))
    tmp = POLLING_CONFIG_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
    os.replace(tmp, POLLING_CONFIG_FILE)
    _POLLING_CACHE.update(ts=0, cfg=None)
    _next_due.clear()   # aplica o novo intervalo ja na proxima volta
    return load_polling_config()


def poll_interval_for(dev):
    cfg = load_polling_config()
    if dev.get("map") == "fg":
        return cfg["fg_interval_s"]
    return cfg["abrigos_interval_s"]
_next_due = {}      # dev_id -> timestamp da proxima leitura
_IN_PROGRESS = set()            # dispositivos com leitura ainda em andamento
_POLL_SEM = threading.Semaphore(120)   # cada equipamento tem no maximo 1 leitura em andamento
_IN_PROGRESS_LOCK = threading.Lock()


def polling_loop():
    global LAST_CYCLE_TS
    while True:
        try:
            devices = load_all_devices_for_polling()
            now = time.time()

            due_devices = []
            for dev in devices:
                dev_id = dev.get("id")
                interval = poll_interval_for(dev)
                due_at = _next_due.get(dev_id, 0)
                if now >= due_at:
                    with _IN_PROGRESS_LOCK:
                        busy = dev_id in _IN_PROGRESS
                    if busy:
                        continue  # leitura anterior ainda rodando (equipamento lento): nao empilha outra
                    due_devices.append(dev)
                    _next_due[dev_id] = now + interval

            if due_devices:
                # dispara cada leitura em paralelo e NAO espera terminar: um
                # equipamento lento/fora do ar (timeout SNMP) nao atrasa mais
                # a proxima leitura dos outros - e o que permite ler a Fonte
                # Grande a cada 5 s mesmo com algum equipamento offline
                def worker(dev):
                    try:
                        with _POLL_SEM:
                            poll_device(dev)
                    except Exception as e:
                        print("[poll_device] erro:", dev.get("name"), e, file=sys.stderr)
                    finally:
                        with _IN_PROGRESS_LOCK:
                            _IN_PROGRESS.discard(dev.get("id"))

                for dev in due_devices:
                    with _IN_PROGRESS_LOCK:
                        _IN_PROGRESS.add(dev.get("id"))
                    threading.Thread(target=worker, args=(dev,), daemon=True).start()

                LAST_CYCLE_TS = time.time()
        except Exception as e:
            print("[polling_loop] erro:", e, file=sys.stderr)
        time.sleep(TICK_SECONDS)


# --------------------------------------------------------------------------
# HTTP server
# --------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # silencia o log padrao do http.server

    def _send_json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path, content_type):
        try:
            with open(path, "rb") as f:
                body = f.read()
        except FileNotFoundError:
            self.send_error(404, "Arquivo nao encontrado")
            return
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        # evita que o navegador guarde uma versao antiga em cache -
        # sem isso, um F5 simples pode continuar mostrando a pagina
        # de antes mesmo depois de trocar o arquivo no servidor.
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.end_headers()
        self.wfile.write(body)

    def _send_favicon(self):
        try:
            with open(FAVICON_FILE, "rb") as f:
                body = f.read()
        except FileNotFoundError:
            self.send_error(404, "favicon nao encontrado")
            return
        self.send_response(200)
        self.send_header("Content-Type", "image/x-icon")
        self.send_header("Content-Length", str(len(body)))
        # o favicon quase nunca muda - diferente das paginas HTML, aqui
        # cache normal do navegador e bem-vindo (evita pedir de novo a
        # cada clique em link/pagina)
        self.send_header("Cache-Control", "public, max-age=604800")
        self.end_headers()
        self.wfile.write(body)

    def _redirect(self, location):
        self.send_response(302)
        self.send_header("Location", location)
        self.end_headers()

    def _is_authenticated(self):
        token = get_session_token(self)
        return is_valid_session(token)

    def do_GET(self):
        parsed = urlparse(self.path)

        if not is_ip_allowed_network(self.client_address[0]):
            self.send_error(403, "Acesso nao permitido a partir desse endereco de rede")
            return

        # rotas publicas (nao exigem login)
        if parsed.path == "/favicon.ico":
            self._send_favicon()
            return
        if parsed.path in ("/login", "/login.html"):
            self._send_file(LOGIN_HTML_FILE, "text/html; charset=utf-8")
            return
        if parsed.path in ("/manifest.webmanifest", "/sw.js"):
            fname = "manifest.webmanifest" if parsed.path.endswith("manifest") else "sw.js"
            path = os.path.join(STATIC_DIR, "app", fname)
            ctype = "application/manifest+json" if fname.endswith("manifest") else "application/javascript; charset=utf-8"
            try:
                with open(path, "rb") as f:
                    body = f.read()
            except FileNotFoundError:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-cache")
            if fname == "sw.js":
                self.send_header("Service-Worker-Allowed", "/")
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path.startswith("/static/"):
            # fontes, logo e tema - publicos (a tela de login tambem usa)
            rel = urllib.parse.unquote(parsed.path[len("/static/"):])
            full = os.path.normpath(os.path.join(STATIC_DIR, rel))
            if not full.startswith(os.path.normpath(STATIC_DIR) + os.sep) or not os.path.isfile(full):
                self.send_error(404, "Nao encontrado")
                return
            ctype = mimetypes.guess_type(full)[0] or "application/octet-stream"
            if full.endswith(".woff2"):
                ctype = "font/woff2"
            elif full.endswith(".css"):
                ctype = "text/css; charset=utf-8"
            with open(full, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "public, max-age=3600")
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/logout":
            token = get_session_token(self)
            if token:
                destroy_session(token)
            self.send_response(302)
            self.send_header("Location", "/login")
            self.send_header("Set-Cookie", f"{SESSION_COOKIE_NAME}=; Path=/; Max-Age=0")
            self.end_headers()
            return

        # todas as rotas abaixo exigem sessao valida
        if not self._is_authenticated():
            if parsed.path.startswith("/api/"):
                self._send_json({"error": "nao autenticado"}, status=401)
            else:
                self._redirect("/login")
            return

        if parsed.path in ("/", "/index.html", "/selecionar", "/selecionar.html"):
            self._send_file(SELECTOR_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path in ("/mapa-interior", "/mapa-interior.html", "/interior"):
            # o "Mapa Abrigos" unico foi substituido por um mapa por abrigo
            self.send_response(302)
            self.send_header("Location", "/selecionar")
            self.end_headers()
            return
        elif parsed.path in ("/mapa-interior-antigo",):
            self._send_file(HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path in ("/mapa-fg", "/mapa-fg.html", "/fg"):
            self._send_file(MAPA_FG_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path.startswith("/mapa/"):
            self._send_file(MAPA_GENERICO_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path.startswith("/mapa-abrigo/"):
            slug = parsed.path.rstrip("/").rsplit("/", 1)[-1]
            if not abrigo_by_slug(slug):
                self.send_error(404, "Abrigo nao encontrado")
                return
            self._send_file(MAPA_FG_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path == "/api/abrigos":
            data = load_devices()
            out = []
            for ab in get_abrigos():
                key = _norm_site(ab["site"])
                ids = [d.get("id") for d in data.get("devices", []) if _norm_site(d.get("site")) == key]
                out.append(dict(ab, device_ids=ids, url="/mapa-abrigo/" + ab["slug"]))
            fg_info = dict(load_abrigos_config()["fg"])
            fg_info["device_ids"] = [d.get("id") for d in load_devices_fg().get("devices", [])]
            self._send_json({"abrigos": out, "fg": fg_info, "edp_extras": load_abrigos_config().get("edp_extras", []),
                             "edp_tel": EDP_TELEFONE})
        elif parsed.path.startswith("/api/devices-abrigo/"):
            slug = parsed.path.rstrip("/").rsplit("/", 1)[-1]
            net = build_abrigo_network(slug)
            if net is None:
                self._send_json({"error": "abrigo nao encontrado"}, status=404)
                return
            self._send_json(net)
        elif parsed.path in ("/app", "/app/", "/alertas", "/m"):
            self._send_file(os.path.join(BASE_DIR, "app.html"), "text/html; charset=utf-8")
        elif parsed.path == "/api/alertas":
            qs = parse_qs(parsed.query)
            try:
                limit = max(1, min(2000, int((qs.get("limit") or ["500"])[0])))
                desde = int((qs.get("desde_id") or ["0"])[0])
            except ValueError:
                limit, desde = 500, 0
            self._send_json(alertas_snapshot(limit, desde))
        elif parsed.path in ("/comandos", "/comandos.html"):
            self._send_file(os.path.join(BASE_DIR, "comandos.html"), "text/html; charset=utf-8")
        elif parsed.path == "/api/comandos/estado":
            tok = self.headers.get("X-Cmd-Token")
            self._send_json({"senha_definida": bool(_cmd_cfg().get("senha_hash")), "liberado": cmd_token_ok(tok)})
        elif parsed.path in ("/api/comandos/lista", "/api/comandos/historico"):
            if not cmd_token_ok(self.headers.get("X-Cmd-Token")):
                self._send_json({"error": "senha de comandos necessaria"}, status=401)
                return
            if parsed.path.endswith("/lista"):
                self._send_json({"comandos": build_commands()})
            else:
                try:
                    with open(COMANDOS_LOG_FILE, encoding="utf-8") as f:
                        self._send_json({"historico": json.load(f)[:50]})
                except (FileNotFoundError, json.JSONDecodeError):
                    self._send_json({"historico": []})
        elif parsed.path in ("/incendio", "/incendio.html", "/alarme-incendio"):
            self._send_file(INCENDIO_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path == "/api/weather":
            qs = parse_qs(parsed.query)
            slug = (qs.get("slug") or ["fg"])[0]
            cfg = load_abrigos_config()
            loc = cfg["fg"] if slug == "fg" else next((a for a in cfg["abrigos"] if a["slug"] == slug), None)
            if not loc or loc.get("lat") is None:
                self._send_json({"error": "abrigo sem coordenadas"}, status=404)
                return
            self._send_json(get_weather(loc["lat"], loc["lon"]))
        elif parsed.path == "/api/knx/status":
            if KNX_MONITOR is None:
                self._send_json({"connected": False, "error": "modulo KNX nao iniciado", "points": []})
            else:
                self._send_json(KNX_MONITOR.snapshot())
        elif parsed.path in ("/climatizacao", "/climatizacao.html", "/climatizacao-fg"):
            self._send_file(CLIMATIZACAO_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path.startswith("/tiles/"):
            m = re.match(r"^/tiles/(osm|sat)/(\d+)/(\d+)/(\d+)\.(png|jpg)$", parsed.path)
            tile = get_tile(m.group(1), int(m.group(2)), int(m.group(3)), int(m.group(4))) if m else None
            if not tile:
                self.send_error(404, "tile indisponivel")
                return
            data, ctype = tile
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "private, max-age=604800")
            self.end_headers()
            self.wfile.write(data)
        elif parsed.path in ("/energia", "/energia.html"):
            self._send_file(ENERGIA_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path == "/api/modbus/test":
            qs = parse_qs(parsed.query)
            def _q(k, d=None):
                return (qs.get(k) or [d])[0]
            ip = (_q("ip", "") or "").strip()
            if not re.match(r"^\d{1,3}(\.\d{1,3}){3}$", ip):
                self._send_json({"error": "IP invalido"}, status=400)
                return
            try:
                result = modbus_read_single(
                    ip, int(_q("port", "502")), int(_q("unit", "1")), _q("type", "holding"),
                    int(float(_q("address", "0"))), _q("dtype", "uint16"),
                    word_swap=parse_swap_flag(_q("word_swap"), True),
                    dword_swap=parse_swap_flag(_q("dword_swap"), True),
                )
                self._send_json(result)
            except Exception as e:
                self._send_json({"error": str(e)}, status=500)
        elif parsed.path in ("/config", "/config.html", "/configuracao"):
            self._send_file(CONFIG_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path == "/api/devices-fg":
            _fgnet = load_devices_fg()
            _fgi = load_abrigos_config()["fg"]
            _fgnet = dict(_fgnet, map_name=_fgi.get("nome"), caption=_fgi.get("sub") or re.sub(r"^Abrigo\s+", "", _fgi.get("nome") or "Fonte Grande"),
                          local=dict(slug="fg", nome=_fgi.get("nome"), lat=_fgi.get("lat"), lon=_fgi.get("lon"),
                                     edp=_fgi.get("edp"), coord_aprox=_fgi.get("coord_aprox", False),
                                     edp_tel=EDP_TELEFONE))
            self._send_json(_fgnet)
        elif parsed.path == "/api/maps":
            self._send_json(load_maps_registry())
        elif parsed.path.startswith("/api/map-devices/"):
            map_id = parsed.path.rsplit("/", 1)[-1]
            self._send_json(load_devices_generic(map_id))
        elif parsed.path == "/api/alarm-schedule":
            self._send_json(load_alarm_schedule())
        elif parsed.path == "/api/sunday-summary":
            self._send_json(load_sunday_summary_config())
        elif parsed.path == "/api/agenda-summary":
            self._send_json(load_agenda_summary_config())
        elif parsed.path == "/api/agenda-summary-amanha":
            self._send_json(load_agenda_summary_amanha_config())
        elif parsed.path == "/api/network-config":
            self._send_json(load_network_config())
        elif parsed.path == "/api/alarm-confirm-config":
            self._send_json(load_alarm_confirm_config())
        elif parsed.path == "/api/polling-config":
            self._send_json(load_polling_config())
        elif parsed.path == "/api/telegram-config":
            self._send_json(telegram_public_config())
        elif parsed.path == "/api/monthly-report":
            qs = parse_qs(parsed.query)
            try:
                year = int((qs.get("year") or [str(time.localtime().tm_year)])[0])
                month = int((qs.get("month") or [str(time.localtime().tm_mon)])[0])
            except ValueError:
                self._send_json({"error": "year/month invalidos"}, status=400)
                return
            full_report = build_monthly_report(year, month)
            only_map = (qs.get("map") or [None])[0]
            if only_map:
                full_report = {only_map: full_report[only_map]} if only_map in full_report else {}
            self._send_json({"year": year, "month": month, "maps": full_report})
        elif parsed.path == "/api/monthly-report/outros-sistemas":
            qs = parse_qs(parsed.query)
            try:
                year = int((qs.get("year") or [str(time.localtime().tm_year)])[0])
                month = int((qs.get("month") or [str(time.localtime().tm_mon)])[0])
            except ValueError:
                self._send_json({"error": "year/month invalidos"}, status=400)
                return
            self._send_json({"year": year, "month": month, "cities": load_other_system_status(year, month)})
        elif parsed.path == "/api/annual-report":
            qs = parse_qs(parsed.query)
            try:
                year = int((qs.get("year") or [str(time.localtime().tm_year)])[0])
            except ValueError:
                self._send_json({"error": "year invalido"}, status=400)
                return
            self._send_json(build_annual_report(year))
        elif parsed.path == "/api/annual-report/downtime-transmissores":
            qs = parse_qs(parsed.query)
            try:
                year = int((qs.get("year") or [str(time.localtime().tm_year)])[0])
            except ValueError:
                self._send_json({"error": "year invalido"}, status=400)
                return
            self._send_json(build_transmitter_downtime(year))
        elif parsed.path == "/api/annual-report/outros-sistemas":
            qs = parse_qs(parsed.query)
            try:
                year = int((qs.get("year") or [str(time.localtime().tm_year)])[0])
            except ValueError:
                self._send_json({"error": "year invalido"}, status=400)
                return
            self._send_json({"year": year, "cities": load_other_system_status_annual(year)})
        elif parsed.path == "/api/nobreak":
            self._send_json(load_nobreak_data())
        elif parsed.path == "/api/planning":
            self._send_json(load_planning_data())
        elif parsed.path == "/api/arquivos":
            self._send_json(load_arquivos_data())
        elif parsed.path == "/api/refrigeracao":
            self._send_json(load_refrig_data())
        elif parsed.path == "/api/relatorios-sites":
            self._send_json(load_sites_tasks_data())
        elif parsed.path == "/api/inventario":
            self._send_json(load_inventario_data())
        elif parsed.path == "/api/lixeira":
            items = load_lixeira()
            items.sort(key=lambda i: i.get("excluido_em", 0), reverse=True)
            self._send_json({"items": items, "tipo_labels": TIPO_LABELS})
        elif parsed.path == "/api/tx-be1001":
            self._send_json(load_tx_be1001_data())
        elif parsed.path in ("/manual_tx_be1001.pdf", "/api/tx-be1001/manual"):
            self._send_file(TX_BE1001_MANUAL_PDF, "application/pdf")
        elif parsed.path == "/manual_tx_vitoria_fg61.pdf":
            self._send_file(TX_VITORIA_FG61_MANUAL_PDF, "application/pdf")
        elif parsed.path == "/manual_tx_odia905fg.pdf":
            self._send_file(TX_ODIA905FG_MANUAL_PDF, "application/pdf")
        elif parsed.path == "/api/relatorios-sites/pendencias":
            data = load_sites_tasks_data()
            result = {}
            for s in data["sites"]:
                pendentes = [
                    {
                        "pendencia_descricao": t.get("pendencia_descricao", ""),
                        "responsavel": t.get("responsavel", ""),
                        "data": t.get("data", ""),
                    }
                    for t in s["tarefas"]
                    if t.get("pendencia") and t.get("pendencia_status") != "finalizada"
                ]
                if pendentes:
                    result[s["nome"]] = pendentes
            self._send_json({"municipios": result})
        elif parsed.path.startswith("/site_photos/"):
            filename = parsed.path.rsplit("/", 1)[-1]
            # trava simples contra path traversal - so aceita nome de arquivo puro
            if "/" in filename or ".." in filename:
                self.send_error(400, "nome de arquivo invalido")
                return
            filepath = os.path.join(SITE_PHOTOS_DIR, filename)
            if not os.path.isfile(filepath):
                self.send_error(404, "foto nao encontrada")
                return
            ext = os.path.splitext(filename)[1].lower()
            content_type = {
                ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
                ".webp": "image/webp", ".gif": "image/gif",
            }.get(ext, "application/octet-stream")
            self._send_file(filepath, content_type)
        elif parsed.path.startswith("/inventario_pdfs/"):
            filename = parsed.path.rsplit("/", 1)[-1]
            if "/" in filename or ".." in filename:
                self.send_error(400, "nome de arquivo invalido")
                return
            filepath = os.path.join(INVENTARIO_PDFS_DIR, filename)
            if not os.path.isfile(filepath):
                self.send_error(404, "anexo nao encontrado")
                return
            # acha o nome ORIGINAL do anexo (o nome no disco e so um uuid)
            # pra sugerir no download e escolher o tipo certo - antes so
            # servia como application/pdf fixo, agora aceita qualquer tipo
            nome_original = filename
            inv_data = load_inventario_data()
            for site in inv_data.get("sites", []):
                achado = next((a for a in site.get("anexos", []) if a.get("filename") == filename), None)
                if achado:
                    nome_original = achado.get("nome_original") or filename
                    break
            content_type, _ = mimetypes.guess_type(nome_original)
            content_type = content_type or "application/octet-stream"
            with open(filepath, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Disposition", f'inline; filename="{nome_original}"')
            self.end_headers()
            self.wfile.write(body)
        elif parsed.path.startswith("/arquivos_upload/"):
            filename = parsed.path.rsplit("/", 1)[-1]
            if "/" in filename or ".." in filename:
                self.send_error(400, "nome de arquivo invalido")
                return
            filepath = os.path.join(ARQUIVOS_DIR, filename)
            if not os.path.isfile(filepath):
                self.send_error(404, "arquivo nao encontrado")
                return
            # acha o nome original (pra sugerir no download) e o tipo
            # mime certo a partir dele - o nome no disco e so um uuid
            data = load_arquivos_data()
            registro = next((a for a in data["arquivos"] if a["nome_arquivo"] == filename), None)
            nome_original = registro["nome_original"] if registro else filename
            content_type, _ = mimetypes.guess_type(nome_original)
            content_type = content_type or "application/octet-stream"
            with open(filepath, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Disposition", f'attachment; filename="{nome_original}"')
            self.end_headers()
            self.wfile.write(body)
        elif parsed.path == "/api/camera-snapshot":
            qs = parse_qs(parsed.query)
            ip = (qs.get("ip") or [""])[0]
            if not re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", ip):
                self.send_error(400, "IP invalido")
                return
            data = fetch_camera_snapshot(ip)
            if data is None:
                self.send_error(502, "nao foi possivel obter a imagem da camera (login ou modelo diferente)")
                return
            self.send_response(200)
            self.send_header("Content-Type", "image/jpeg")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.end_headers()
            self.wfile.write(data)
        elif parsed.path == "/api/report-devices":
            self._send_json(build_report_device_list())
        elif parsed.path == "/api/report-site-names":
            names = set(EXTRA_INTERIOR_SITES)
            for map_data in build_report_device_list().values():
                for d in map_data["devices"]:
                    if d.get("site"):
                        names.add(d["site"])
            self._send_json({"sites": sorted(names, key=lambda s: s.lower())})
        elif parsed.path == "/api/monthly-report/powerbi":
            qs = parse_qs(parsed.query)
            year_raw = (qs.get("year") or [None])[0]
            month_raw = (qs.get("month") or [None])[0]
            try:
                year = int(year_raw) if year_raw else None
                month = int(month_raw) if month_raw else None
            except ValueError:
                self._send_json({"error": "year/month invalidos"}, status=400)
                return
            self._send_json(build_powerbi_rows(year, month))
        elif parsed.path == "/api/monthly-report/pdf":
            qs = parse_qs(parsed.query)
            try:
                year = int((qs.get("year") or [str(time.localtime().tm_year)])[0])
                month = int((qs.get("month") or [str(time.localtime().tm_mon)])[0])
            except ValueError:
                self._send_json({"error": "year/month invalidos"}, status=400)
                return
            try:
                pdf_bytes = export_monthly_report_pdf(year, month)
            except Exception as e:
                self._send_json({"error": str(e)}, status=500)
                return
            month_names = ["jan","fev","mar","abr","mai","jun","jul","ago","set","out","nov","dez"]
            filename = f"relatorio_ocorrencias_{month_names[month-1]}{str(year)[2:]}.pdf"
            self.send_response(200)
            self.send_header("Content-Type", "application/pdf")
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
            self.send_header("Content-Length", str(len(pdf_bytes)))
            self.end_headers()
            self.wfile.write(pdf_bytes)
        elif parsed.path == "/api/annual-report/pdf":
            qs = parse_qs(parsed.query)
            try:
                year = int((qs.get("year") or [str(time.localtime().tm_year)])[0])
            except ValueError:
                self._send_json({"error": "year invalido"}, status=400)
                return
            try:
                pdf_bytes = export_annual_report_pdf(year)
            except Exception as e:
                self._send_json({"error": str(e)}, status=500)
                return
            filename = f"relatorio_anual_ocorrencias_{year}.pdf"
            self.send_response(200)
            self.send_header("Content-Type", "application/pdf")
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
            self.send_header("Content-Length", str(len(pdf_bytes)))
            self.end_headers()
            self.wfile.write(pdf_bytes)
        elif parsed.path.startswith("/api/layout/"):
            map_name = parsed.path.rsplit("/", 1)[-1]
            layout = load_map_layout(map_name)
            self._send_json(layout or {})
        elif parsed.path.startswith("/api/message-template/"):
            map_name = parsed.path.rsplit("/", 1)[-1]
            templates = get_message_template_for_map(map_name)
            self._send_json(templates)
        elif parsed.path == "/api/status":
            with STATUS_LOCK:
                snapshot = dict(STATUS)
            self._send_json({
                "updated_at": LAST_CYCLE_TS,
                "refresh_interval": REFRESH_INTERVAL,
                "fg_refresh_interval": load_polling_config()["fg_interval_s"],
                "abrigos_refresh_interval": load_polling_config()["abrigos_interval_s"],
                "devices": snapshot,
            })
        elif parsed.path == "/api/devices":
            self._send_json(load_devices())
        elif parsed.path == "/api/history":
            qs = parse_qs(parsed.query)
            id_values = qs.get("id")
            if not id_values:
                self._send_json({"error": "parametro 'id' obrigatorio"}, status=400)
                return
            try:
                dev_id = int(id_values[0])
            except ValueError:
                self._send_json({"error": "id invalido"}, status=400)
                return
            with HISTORY_LOCK:
                h = HISTORY.get(dev_id, {"latency": [], "metrics": {}})
                snapshot = {
                    "latency": list(h["latency"]),
                    "metrics": {k: list(v) for k, v in h["metrics"].items()},
                }
            self._send_json({"id": dev_id, **snapshot})
        elif parsed.path in ("/logs", "/logs.html"):
            self._send_file(LOGS_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path == "/api/events":
            with EVENTS_LOCK:
                events = list(EVENTS)
            events.reverse()  # mais recentes primeiro
            self._send_json({"events": events, "total": len(events)})
        elif parsed.path == "/api/events-by-site":
            qs = parse_qs(parsed.query)
            site = (qs.get("site") or [""])[0]
            map_id = (qs.get("map") or [None])[0]
            if not site:
                self._send_json({"error": "site ausente"}, status=400)
                return
            events = build_events_for_site(site, map_id)
            self._send_json({"events": events, "site": site})
        elif parsed.path in ("/eventos.log", "/download/eventos.log"):
            self._send_file(EVENTS_LOG_FILE, "text/plain; charset=utf-8")
        elif parsed.path in ("/dashboard", "/falhas", "/falhas.html"):
            self._send_file(DASHBOARD_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path in ("/relatorio", "/relatorio-mensal", "/relatorio.html"):
            self._send_file(RELATORIO_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path in ("/relatorio-anual", "/relatorio_anual.html"):
            self._send_file(RELATORIO_ANUAL_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path in ("/nobreak", "/nobreak.html"):
            self._send_file(NOBREAK_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path in ("/planning", "/planning.html"):
            self._send_file(PLANNING_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path in ("/arquivos", "/arquivos.html"):
            self._send_file(ARQUIVOS_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path in ("/refrigeracao", "/refrigeracao.html"):
            self._send_file(REFRIGERACAO_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path in ("/relatorios-sites", "/relatorios_sites.html"):
            self._send_file(RELATORIOS_SITES_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path in ("/inventario", "/inventario.html"):
            self._send_file(INVENTARIO_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path in ("/lixeira", "/lixeira.html"):
            self._send_file(LIXEIRA_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path in ("/relatorio-tx-be1001", "/relatorio_tx_be1001.html"):
            self._send_file(TX_BE1001_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path in ("/relatorio-tx-vitoria-fg61", "/relatorio_tx_vitoria_fg61.html"):
            self._send_file(TX_VITORIA_FG61_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path in ("/relatorio-tx-odia905fg", "/relatorio_tx_odia905fg.html"):
            self._send_file(TX_ODIA905FG_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path in ("/relatorio-transmissores-fg", "/relatorio_transmissores_fg.html"):
            self._send_file(TRANSMISSORES_FG_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path in ("/graficos-historico", "/graficos_historico.html"):
            self._send_file(GRAFICOS_HTML_FILE, "text/html; charset=utf-8")
        elif parsed.path == "/api/incidents":
            qs = parse_qs(parsed.query)
            try:
                days = int(qs.get("days", ["30"])[0])
            except ValueError:
                days = 30
            cutoff = time.time() - days * 24 * 3600
            now = time.time()
            with INCIDENTS_LOCK:
                snapshot = [dict(i) for i in INCIDENTS]
            filtered = [i for i in snapshot if (i.get("end_ts") or now) >= cutoff]
            for i in filtered:
                start = i["start_ts"]
                end = i.get("end_ts") or now
                i["duration_s"] = round(end - start, 1)
                i["ongoing"] = i.get("end_ts") is None
            self._send_json({"incidents": filtered, "days": days})
        elif parsed.path == "/api/snmp/scan":
            qs = parse_qs(parsed.query)
            ip = (qs.get("ip") or [""])[0].strip()
            community = (qs.get("community") or ["public"])[0]
            version = (qs.get("version") or ["1"])[0]
            base_oid = (qs.get("oid") or ["1.3.6.1.2.1.1"])[0].strip()
            try:
                port = int((qs.get("port") or ["161"])[0])
            except ValueError:
                port = 161
            try:
                max_results = min(5000, int((qs.get("max") or ["5000"])[0]))
            except ValueError:
                max_results = 40

            if not ip:
                self._send_json({"error": "parametro 'ip' obrigatorio"}, status=400)
                return
            if version == "3":
                self._send_json({"error": "SNMPv3 ainda nao e suportado para leitura/busca - use v1 ou v2c"}, status=400)
                return
            try:
                results = snmp_walk(ip, community, base_oid, version=version, port=port, max_results=max_results)
                self._send_json({"ip": ip, "base_oid": base_oid, "results": results, "count": len(results)})
            except Exception as e:
                self._send_json({"error": str(e)}, status=500)
        else:
            self.send_error(404, "Nao encontrado")

    def do_POST(self):
        parsed = urlparse(self.path)

        if not is_ip_allowed_network(self.client_address[0]):
            self.send_error(403, "Acesso nao permitido a partir desse endereco de rede")
            return

        if parsed.path == "/login":
            client_ip = self.client_address[0]

            if is_login_blocked(client_ip):
                self.send_response(302)
                self.send_header("Location", "/login?erro=bloqueado")
                self.end_headers()
                return

            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length) if length else b""
            content_type = self.headers.get("Content-Type", "")

            if "application/json" in content_type:
                try:
                    payload = json.loads(body.decode("utf-8") or "{}")
                except json.JSONDecodeError:
                    payload = {}
                username = payload.get("username", "")
                password = payload.get("password", "")
            else:
                form = parse_qs(body.decode("utf-8"))
                username = (form.get("username") or [""])[0]
                password = (form.get("password") or [""])[0]

            if username == AUTH_USERNAME and password == AUTH_PASSWORD:
                register_login_success(client_ip)
                token = create_session()
                self.send_response(302)
                self.send_header("Location", "/")
                self.send_header(
                    "Set-Cookie",
                    f"{SESSION_COOKIE_NAME}={token}; Path=/; Max-Age={SESSION_TTL_S}; HttpOnly; SameSite=Lax",
                )
                self.end_headers()
            else:
                register_login_failure(client_ip)
                self.send_response(302)
                self.send_header("Location", "/login?erro=1")
                self.end_headers()
            return

        # todas as demais rotas POST exigem sessao valida
        if not self._is_authenticated():
            self._send_json({"error": "nao autenticado"}, status=401)
            return

        if parsed.path == "/api/toggle":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                dev_id = payload.get("id")
                enabled = bool(payload.get("enabled"))
                if dev_id is None:
                    self._send_json({"ok": False, "error": "id ausente"}, status=400)
                    return
                found = set_device_enabled(int(dev_id), enabled)
                if not found:
                    self._send_json({"ok": False, "error": "dispositivo nao encontrado"}, status=404)
                    return
                # forca uma nova leitura imediata na proxima rodada de polling
                _next_due[int(dev_id)] = 0
                self._send_json({"ok": True, "id": dev_id, "enabled": enabled})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/toggle-alert":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                dev_id = payload.get("id")
                alert_enabled = bool(payload.get("alert_enabled"))
                if dev_id is None:
                    self._send_json({"ok": False, "error": "id ausente"}, status=400)
                    return
                found = set_device_alert_enabled(int(dev_id), alert_enabled)
                if not found:
                    self._send_json({"ok": False, "error": "dispositivo nao encontrado"}, status=404)
                    return
                self._send_json({"ok": True, "id": dev_id, "alert_enabled": alert_enabled})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/toggle-sound":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                dev_id = payload.get("id")
                sound_enabled = bool(payload.get("sound_enabled"))
                if dev_id is None:
                    self._send_json({"ok": False, "error": "id ausente"}, status=400)
                    return
                found = set_device_sound_enabled(int(dev_id), sound_enabled)
                if not found:
                    self._send_json({"ok": False, "error": "dispositivo nao encontrado"}, status=404)
                    return
                self._send_json({"ok": True, "id": dev_id, "sound_enabled": sound_enabled})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/toggle-service":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                dev_id = payload.get("id")
                label = payload.get("label")
                enabled = bool(payload.get("enabled"))
                if dev_id is None or not label:
                    self._send_json({"ok": False, "error": "id ou label ausente"}, status=400)
                    return
                found = set_metric_enabled(int(dev_id), label, enabled)
                if not found:
                    self._send_json({"ok": False, "error": "servico nao encontrado"}, status=404)
                    return
                self._send_json({"ok": True, "id": dev_id, "label": label, "enabled": enabled})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/toggle-ping":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                dev_id = payload.get("id")
                ping_enabled = bool(payload.get("ping_enabled"))
                if dev_id is None:
                    self._send_json({"ok": False, "error": "id ausente"}, status=400)
                    return
                found = set_device_ping_enabled(int(dev_id), ping_enabled)
                if not found:
                    self._send_json({"ok": False, "error": "dispositivo nao encontrado"}, status=404)
                    return
                self._send_json({"ok": True, "id": dev_id, "ping_enabled": ping_enabled})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/monthly-report/observation":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                incident_id = payload.get("id")
                text = (payload.get("text") or "").strip()
                if not incident_id:
                    self._send_json({"ok": False, "error": "id ausente"}, status=400)
                    return
                # tenta primeiro como entrada manual; se nao for, salva como
                # observacao de ocorrencia automatica (sistema antigo)
                if not update_manual_entry_observation(incident_id, text):
                    save_observation(incident_id, text)
                self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/monthly-report/exclude-incident":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                incident_id = payload.get("id")
                excluded = bool(payload.get("excluded"))
                if not incident_id:
                    self._send_json({"ok": False, "error": "id ausente"}, status=400)
                    return
                set_incident_excluded(incident_id, excluded)
                log_event("OCORRENCIA " + ("EXCLUIDA DO RELATORIO" if excluded else "INCLUIDA NO RELATORIO"),
                          incident_id, None)
                self._send_json({"ok": True, "id": incident_id, "excluded": excluded})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/monthly-report/outros-sistemas":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                year = int(payload.get("year"))
                month = int(payload.get("month"))
                city = payload.get("city")
                situacao = payload.get("situacao", "ok")
                observacao = payload.get("observacao", "")
                if situacao not in ("ok", "Fora"):
                    self._send_json({"ok": False, "error": "situacao invalida"}, status=400)
                    return
                save_other_system_status(year, month, city, situacao, observacao)
                self._send_json({"ok": True})
            except ValueError as e:
                self._send_json({"ok": False, "error": str(e)}, status=400)
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/arquivos/upload":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                original_name = (payload.get("nome") or "").strip()
                base64_data = payload.get("dados") or ""
                if not original_name or not base64_data:
                    self._send_json({"ok": False, "error": "nome e dados do arquivo são obrigatórios"}, status=400)
                    return
                nome_arquivo, tamanho = save_uploaded_arquivo(base64_data, original_name)
                data = load_arquivos_data()
                novo_id = str(uuid.uuid4())
                data["arquivos"].append({
                    "id": novo_id,
                    "nome_original": original_name,
                    "nome_arquivo": nome_arquivo,
                    "tamanho": tamanho,
                    "descricao": (payload.get("descricao") or "").strip(),
                    "enviado_por": (payload.get("enviado_por") or "").strip(),
                    "enviado_em": time.time(),
                })
                save_arquivos_data(data)
                self._send_json({"ok": True, "new_id": novo_id})
            except ValueError as e:
                self._send_json({"ok": False, "error": str(e)}, status=400)
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/arquivos/delete":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                arq_id = payload.get("id")
                data = load_arquivos_data()
                alvo = next((a for a in data["arquivos"] if a["id"] == arq_id), None)
                if alvo:
                    mover_para_lixeira("arquivo", alvo.get("nome_original") or "(sem nome)", alvo)
                data["arquivos"] = [a for a in data["arquivos"] if a["id"] != arq_id]
                save_arquivos_data(data)
                self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/planning/activity":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                action = payload.get("action")
                data = load_planning_data()

                if action == "add":
                    new_id = str(uuid.uuid4())
                    data["activities"].append({
                        "id": new_id,
                        "titulo": (payload.get("titulo") or "").strip() or "Nova atividade",
                        "local": (payload.get("local") or "").strip(),
                        "data": (payload.get("data") or "").strip() or time.strftime("%Y-%m-%d"),
                        "prioridade": payload.get("prioridade") if payload.get("prioridade") in ("baixa", "media", "alta") else "media",
                        "responsavel": (payload.get("responsavel") or "").strip(),
                        "descricao": (payload.get("descricao") or "").strip(),
                        "status": "concluida" if payload.get("status") == "concluida" else "pendente",
                        "criado_em": time.time(),
                    })
                    save_planning_data(data)
                    self._send_json({"ok": True, "new_id": new_id})
                    return
                elif action == "edit":
                    for a in data["activities"]:
                        if a["id"] == payload.get("id"):
                            if "titulo" in payload:
                                a["titulo"] = (payload.get("titulo") or "").strip() or a["titulo"]
                            if "local" in payload:
                                a["local"] = (payload.get("local") or "").strip()
                            if "data" in payload:
                                a["data"] = (payload.get("data") or "").strip() or a["data"]
                            if "prioridade" in payload and payload.get("prioridade") in ("baixa", "media", "alta"):
                                a["prioridade"] = payload.get("prioridade")
                            if "responsavel" in payload:
                                a["responsavel"] = (payload.get("responsavel") or "").strip()
                            if "descricao" in payload:
                                a["descricao"] = (payload.get("descricao") or "").strip()
                            if "status" in payload:
                                a["status"] = "concluida" if payload.get("status") == "concluida" else "pendente"
                elif action == "delete":
                    a_del = next((a for a in data["activities"] if a["id"] == payload.get("id")), None)
                    if a_del:
                        mover_para_lixeira("planning_activity", a_del.get("titulo") or "(sem título)", a_del)
                    data["activities"] = [a for a in data["activities"] if a["id"] != payload.get("id")]
                else:
                    self._send_json({"ok": False, "error": "acao invalida"}, status=400)
                    return
                save_planning_data(data)
                self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/nobreak/location":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                action = payload.get("action")
                data = load_nobreak_data()
                if action == "add":
                    data["locations"].append({
                        "id": str(uuid.uuid4()),
                        "local": payload.get("local", "").strip() or "Novo local",
                        "modelo_data": payload.get("modelo_data", "").strip(),
                        "data_troca_bateria": payload.get("data_troca_bateria", "").strip(),
                        "validade_meses": payload.get("validade_meses") or 24,
                        "eventos": [],
                    })
                elif action == "edit":
                    for loc in data["locations"]:
                        if loc["id"] == payload.get("id"):
                            loc["local"] = payload.get("local", loc["local"])
                            loc["modelo_data"] = payload.get("modelo_data", loc["modelo_data"])
                            if "data_troca_bateria" in payload:
                                loc["data_troca_bateria"] = (payload.get("data_troca_bateria") or "").strip()
                            if "validade_meses" in payload:
                                try:
                                    loc["validade_meses"] = int(payload.get("validade_meses") or 24)
                                except (TypeError, ValueError):
                                    pass
                elif action == "delete":
                    loc_del = next((l for l in data["locations"] if l["id"] == payload.get("id")), None)
                    if loc_del:
                        mover_para_lixeira("nobreak_location", loc_del.get("local") or "(sem nome)", loc_del)
                    data["locations"] = [l for l in data["locations"] if l["id"] != payload.get("id")]
                else:
                    self._send_json({"ok": False, "error": "acao invalida"}, status=400)
                    return
                save_nobreak_data(data)
                self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/nobreak/event":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                action = payload.get("action")
                data = load_nobreak_data()
                loc = next((l for l in data["locations"] if l["id"] == payload.get("location_id")), None)
                if not loc:
                    self._send_json({"ok": False, "error": "localidade nao encontrada"}, status=404)
                    return
                new_event_id = None
                if action == "add":
                    new_event_id = str(uuid.uuid4())
                    loc["eventos"].append({
                        "id": new_event_id,
                        "data": payload.get("data", "").strip() or "—",
                        "texto": payload.get("texto", "").strip(),
                    })
                elif action == "edit":
                    for ev in loc["eventos"]:
                        if ev["id"] == payload.get("id"):
                            ev["data"] = payload.get("data", ev["data"])
                            ev["texto"] = payload.get("texto", ev["texto"])
                elif action == "delete":
                    ev_del = next((e for e in loc["eventos"] if e["id"] == payload.get("id")), None)
                    if ev_del:
                        mover_para_lixeira(
                            "nobreak_event",
                            f"{loc.get('local','?')}: {(ev_del.get('texto') or '')[:60]}",
                            ev_del, {"location_id": loc["id"], "location_nome": loc.get("local")}
                        )
                    loc["eventos"] = [e for e in loc["eventos"] if e["id"] != payload.get("id")]
                else:
                    self._send_json({"ok": False, "error": "acao invalida"}, status=400)
                    return
                save_nobreak_data(data)
                self._send_json({"ok": True, "new_id": new_event_id})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/nobreak/battery-info":
            # data de troca de bateria + validade, guardada por ID do
            # dispositivo (NB 40KVA FG / NB 80KVA FG) - diferente da
            # "location" (que e um registro manual de manutencao por
            # municipio); aqui e direto ligado ao equipamento monitorado
            # por SNMP, sem precisar duplicar como uma localidade.
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                dev_id = str(payload.get("device_id", "")).strip()
                if not dev_id:
                    self._send_json({"ok": False, "error": "device_id obrigatorio"}, status=400)
                    return
                data = load_nobreak_data()
                info = data["battery_info"].setdefault(dev_id, {})
                if "data_troca_bateria" in payload:
                    info["data_troca_bateria"] = (payload.get("data_troca_bateria") or "").strip()
                if "validade_meses" in payload:
                    try:
                        info["validade_meses"] = int(payload.get("validade_meses") or 24)
                    except (TypeError, ValueError):
                        pass
                save_nobreak_data(data)
                self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/nobreak/battery":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                action = payload.get("action")
                data = load_nobreak_data()
                if action == "add":
                    data["battery_reference"].append({
                        "id": str(uuid.uuid4()),
                        "modelo": payload.get("modelo", "").strip() or "Novo modelo",
                        "bateria": payload.get("bateria", "").strip(),
                        "quantidade": payload.get("quantidade", "").strip(),
                        "observacao": payload.get("observacao", "").strip(),
                    })
                elif action == "edit":
                    for b in data["battery_reference"]:
                        if b["id"] == payload.get("id"):
                            b["modelo"] = payload.get("modelo", b["modelo"])
                            b["bateria"] = payload.get("bateria", b["bateria"])
                            b["quantidade"] = payload.get("quantidade", b["quantidade"])
                            b["observacao"] = payload.get("observacao", b["observacao"])
                elif action == "delete":
                    bat_del = next((b for b in data["battery_reference"] if b["id"] == payload.get("id")), None)
                    if bat_del:
                        mover_para_lixeira("nobreak_battery", bat_del.get("modelo") or "(sem modelo)", bat_del)
                    data["battery_reference"] = [b for b in data["battery_reference"] if b["id"] != payload.get("id")]
                else:
                    self._send_json({"ok": False, "error": "acao invalida"}, status=400)
                    return
                save_nobreak_data(data)
                self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/refrigeracao/location":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                action = payload.get("action")
                data = load_refrig_data()
                if action == "add":
                    data["locations"].append({
                        "id": str(uuid.uuid4()),
                        "local": payload.get("local", "").strip() or "Novo local",
                        "qtd_btus": payload.get("qtd_btus", "").strip(),
                        "eventos": [],
                    })
                elif action == "edit":
                    for loc in data["locations"]:
                        if loc["id"] == payload.get("id"):
                            loc["local"] = payload.get("local", loc["local"])
                            loc["qtd_btus"] = payload.get("qtd_btus", loc["qtd_btus"])
                elif action == "delete":
                    loc_del = next((l for l in data["locations"] if l["id"] == payload.get("id")), None)
                    if loc_del:
                        mover_para_lixeira("refrigeracao_location", loc_del.get("local") or "(sem nome)", loc_del)
                    data["locations"] = [l for l in data["locations"] if l["id"] != payload.get("id")]
                else:
                    self._send_json({"ok": False, "error": "acao invalida"}, status=400)
                    return
                save_refrig_data(data)
                self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/refrigeracao/event":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                action = payload.get("action")
                data = load_refrig_data()
                loc = next((l for l in data["locations"] if l["id"] == payload.get("location_id")), None)
                if not loc:
                    self._send_json({"ok": False, "error": "localidade nao encontrada"}, status=404)
                    return
                new_event_id = None
                if action == "add":
                    new_event_id = str(uuid.uuid4())
                    loc["eventos"].append({
                        "id": new_event_id,
                        "data": payload.get("data", "").strip() or "—",
                        "texto": payload.get("texto", "").strip(),
                    })
                elif action == "edit":
                    for ev in loc["eventos"]:
                        if ev["id"] == payload.get("id"):
                            ev["data"] = payload.get("data", ev["data"])
                            ev["texto"] = payload.get("texto", ev["texto"])
                elif action == "delete":
                    ev_del = next((e for e in loc["eventos"] if e["id"] == payload.get("id")), None)
                    if ev_del:
                        mover_para_lixeira(
                            "refrigeracao_event",
                            f"{loc.get('local','?')}: {(ev_del.get('texto') or '')[:60]}",
                            ev_del, {"location_id": loc["id"], "location_nome": loc.get("local")}
                        )
                    loc["eventos"] = [e for e in loc["eventos"] if e["id"] != payload.get("id")]
                else:
                    self._send_json({"ok": False, "error": "acao invalida"}, status=400)
                    return
                save_refrig_data(data)
                self._send_json({"ok": True, "new_id": new_event_id})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/relatorios-sites/site":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                action = payload.get("action")
                data = load_sites_tasks_data()
                if action == "add":
                    nome = (payload.get("nome") or "").strip() or "Novo município"
                    if any(s["nome"].lower() == nome.lower() for s in data["sites"]):
                        self._send_json({"ok": False, "error": "esse município já existe"}, status=400)
                        return
                    data["sites"].append({"id": str(uuid.uuid4()), "nome": nome, "tarefas": []})
                elif action == "edit":
                    for s in data["sites"]:
                        if s["id"] == payload.get("id"):
                            s["nome"] = (payload.get("nome") or s["nome"]).strip()
                elif action == "delete":
                    site_del = next((s for s in data["sites"] if s["id"] == payload.get("id")), None)
                    if site_del:
                        mover_para_lixeira("relatorios_sites_site", site_del.get("nome") or "(sem nome)", site_del)
                    data["sites"] = [s for s in data["sites"] if s["id"] != payload.get("id")]
                else:
                    self._send_json({"ok": False, "error": "acao invalida"}, status=400)
                    return
                save_sites_tasks_data(data)
                self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/relatorios-sites/task":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                action = payload.get("action")
                data = load_sites_tasks_data()
                site = next((s for s in data["sites"] if s["id"] == payload.get("site_id")), None)
                if not site:
                    self._send_json({"ok": False, "error": "município não encontrado"}, status=404)
                    return

                if action == "add":
                    new_id = str(uuid.uuid4())
                    fotos = payload.get("fotos") or ([payload["foto"]] if payload.get("foto") else [])
                    status_inicial = payload.get("pendencia_status") or "aberta"
                    # data de finalizacao: usa a que a pessoa escolheu no
                    # formulario (util pra registrar servicos concluidos em
                    # dias anteriores) - se nao vier nenhuma, cai no padrao
                    # de usar a data de hoje
                    data_final_payload = (payload.get("pendencia_data_finalizacao") or "").strip()
                    if status_inicial == "finalizada":
                        data_finalizacao = data_final_payload or time.strftime("%Y-%m-%d")
                    else:
                        data_finalizacao = None
                    site["tarefas"].append({
                        "id": new_id,
                        "data": payload.get("data", "").strip() or time.strftime("%Y-%m-%d"),
                        "descricao": (payload.get("descricao") or "").strip(),
                        "tipo_registro": payload.get("tipo_registro") if payload.get("tipo_registro") in ("tarefa", "pendencia") else "tarefa",
                        "pendencia": bool(payload.get("pendencia")),
                        "pendencia_descricao": (payload.get("pendencia_descricao") or "").strip(),
                        "pendencia_status": status_inicial,
                        "pendencia_data_finalizacao": data_finalizacao,
                        "responsavel": (payload.get("responsavel") or "").strip(),
                        "fotos": fotos[:10],  # nomes dos arquivos, ja salvos via /photo - maximo 10
                        "criado_em": time.time(),
                    })
                    save_sites_tasks_data(data)
                    self._send_json({"ok": True, "new_id": new_id})
                    return
                elif action == "edit":
                    for t in site["tarefas"]:
                        if t["id"] == payload.get("id"):
                            status_anterior = t.get("pendencia_status")
                            for field in ("data", "descricao", "pendencia_descricao", "responsavel", "pendencia_status"):
                                if field in payload:
                                    t[field] = (payload.get(field) or "").strip() if isinstance(payload.get(field), str) else payload.get(field)
                            if "tipo_registro" in payload and payload.get("tipo_registro") in ("tarefa", "pendencia"):
                                t["tipo_registro"] = payload.get("tipo_registro")
                            if "pendencia" in payload:
                                t["pendencia"] = bool(payload.get("pendencia"))
                            if "fotos" in payload:
                                t["fotos"] = (payload.get("fotos") or [])[:10]
                            # data de finalizacao: se a pessoa escolheu uma
                            # data manualmente no formulario, usa ela (util
                            # pra registrar conclusao de dias anteriores, ou
                            # corrigir uma data errada sem precisar reabrir
                            # e finalizar de novo). Se nao veio nenhuma data
                            # explicita, mantem o comportamento antigo:
                            # preenche com hoje SO no momento da transicao
                            # pra "finalizada" (nao mexe se ja estava
                            # finalizada antes, pra nao sobrescrever toda
                            # vez que a tarefa e editada por outro motivo)
                            novo_status = t.get("pendencia_status")
                            data_final_payload = (payload.get("pendencia_data_finalizacao") or "").strip() if "pendencia_data_finalizacao" in payload else ""
                            if novo_status == "finalizada":
                                if data_final_payload:
                                    t["pendencia_data_finalizacao"] = data_final_payload
                                elif status_anterior != "finalizada":
                                    t["pendencia_data_finalizacao"] = time.strftime("%Y-%m-%d")
                            else:
                                t["pendencia_data_finalizacao"] = None
                elif action == "delete":
                    task_del = next((t for t in site["tarefas"] if t["id"] == payload.get("id")), None)
                    if task_del:
                        desc = task_del.get("pendencia_descricao") or task_del.get("descricao") or "(sem descrição)"
                        mover_para_lixeira(
                            "relatorios_sites_task",
                            f"{site.get('nome','?')}: {desc[:60]}",
                            task_del, {"site_id": site["id"], "site_nome": site.get("nome")}
                        )
                    site["tarefas"] = [t for t in site["tarefas"] if t["id"] != payload.get("id")]
                else:
                    self._send_json({"ok": False, "error": "acao invalida"}, status=400)
                    return
                save_sites_tasks_data(data)
                self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/inventario/site":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                action = payload.get("action")
                data = load_inventario_data()
                if action == "add":
                    nome = (payload.get("nome") or "").strip() or "Novo município"
                    if any(s["nome"].lower() == nome.lower() for s in data["sites"]):
                        self._send_json({"ok": False, "error": "esse município já existe"}, status=400)
                        return
                    data["sites"].append({
                        "id": str(uuid.uuid4()), "nome": nome, "equipamentos": [],
                        "endereco": "", "latitude": None, "longitude": None, "anexos": [],
                    })
                elif action == "edit":
                    for s in data["sites"]:
                        if s["id"] == payload.get("id"):
                            s["nome"] = (payload.get("nome") or s["nome"]).strip()
                elif action == "edit_local":
                    # salva endereco/geolocalizacao - separado do "edit" (nome) pra
                    # nao disparar junto com o autosave do campo de nome
                    for s in data["sites"]:
                        if s["id"] == payload.get("id"):
                            s["endereco"] = (payload.get("endereco") or "").strip()
                            lat = payload.get("latitude")
                            lng = payload.get("longitude")
                            s["latitude"] = float(lat) if lat not in (None, "") else None
                            s["longitude"] = float(lng) if lng not in (None, "") else None
                elif action == "delete":
                    site_del = next((s for s in data["sites"] if s["id"] == payload.get("id")), None)
                    if site_del:
                        mover_para_lixeira("inventario_site", site_del.get("nome") or "(sem nome)", site_del)
                    data["sites"] = [s for s in data["sites"] if s["id"] != payload.get("id")]
                else:
                    self._send_json({"ok": False, "error": "acao invalida"}, status=400)
                    return
                save_inventario_data(data)
                self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/inventario/equipamento":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                action = payload.get("action")
                data = load_inventario_data()
                site = next((s for s in data["sites"] if s["id"] == payload.get("site_id")), None)
                if not site:
                    self._send_json({"ok": False, "error": "município não encontrado"}, status=404)
                    return

                campos_texto = (
                    "nome", "categoria", "modelo", "fabricante", "numero_serie",
                    "patrimonio", "status", "data_aquisicao", "observacao",
                )
                if action == "add":
                    new_id = str(uuid.uuid4())
                    equip = {"id": new_id, "fotos": [], "criado_em": time.time()}
                    for field in campos_texto:
                        equip[field] = (payload.get(field) or "").strip()
                    if payload.get("fotos"):
                        equip["fotos"] = (payload.get("fotos") or [])[:10]
                    site["equipamentos"].append(equip)
                    save_inventario_data(data)
                    self._send_json({"ok": True, "new_id": new_id})
                    return
                elif action == "edit":
                    for e in site["equipamentos"]:
                        if e["id"] == payload.get("id"):
                            for field in campos_texto:
                                if field in payload:
                                    e[field] = (payload.get(field) or "").strip()
                            if "fotos" in payload:
                                e["fotos"] = (payload.get("fotos") or [])[:10]
                elif action == "delete":
                    equip_del = next((e for e in site["equipamentos"] if e["id"] == payload.get("id")), None)
                    if equip_del:
                        mover_para_lixeira(
                            "inventario_equipamento",
                            f"{site.get('nome','?')}: {equip_del.get('nome') or '(sem nome)'}",
                            equip_del, {"site_id": site["id"], "site_nome": site.get("nome")}
                        )
                    site["equipamentos"] = [e for e in site["equipamentos"] if e["id"] != payload.get("id")]
                else:
                    self._send_json({"ok": False, "error": "acao invalida"}, status=400)
                    return
                save_inventario_data(data)
                self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/inventario/anexo":
            try:
                length = int(self.headers.get("Content-Length", 0))
                if length > int(MAX_ANEXO_BYTES * 1.4):  # base64 e ~33% maior que o binario original
                    self._send_json({"ok": False, "error": "arquivo muito grande"}, status=413)
                    return
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                action = payload.get("action")
                data = load_inventario_data()
                site = next((s for s in data["sites"] if s["id"] == payload.get("site_id")), None)
                if not site:
                    self._send_json({"ok": False, "error": "município não encontrado"}, status=404)
                    return
                if "anexos" not in site:
                    site["anexos"] = []

                if action == "add":
                    nome_original = payload.get("nome_original") or "documento.pdf"
                    filename, tamanho = save_site_pdf(payload.get("data", ""), nome_original)
                    new_id = str(uuid.uuid4())
                    site["anexos"].append({
                        "id": new_id,
                        "filename": filename,
                        "nome_original": nome_original,
                        "tamanho": tamanho,
                        "enviado_em": time.time(),
                    })
                    save_inventario_data(data)
                    self._send_json({"ok": True, "new_id": new_id})
                    return
                elif action == "delete":
                    anexo_del = next((a for a in site["anexos"] if a["id"] == payload.get("id")), None)
                    if anexo_del:
                        mover_para_lixeira(
                            "inventario_anexo",
                            f"{site.get('nome','?')}: {anexo_del.get('nome_original') or '(sem nome)'}",
                            anexo_del, {"site_id": site["id"], "site_nome": site.get("nome")}
                        )
                    site["anexos"] = [a for a in site["anexos"] if a["id"] != payload.get("id")]
                else:
                    self._send_json({"ok": False, "error": "acao invalida"}, status=400)
                    return
                save_inventario_data(data)
                self._send_json({"ok": True})
            except ValueError as e:
                self._send_json({"ok": False, "error": str(e)}, status=400)
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/lixeira/restaurar":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                ok, erro = restaurar_da_lixeira(payload.get("id"))
                if ok:
                    self._send_json({"ok": True})
                else:
                    self._send_json({"ok": False, "error": erro}, status=400)
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/lixeira/excluir-definitivo":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                items = load_lixeira()
                novo = [i for i in items if i["id"] != payload.get("id")]
                if len(novo) == len(items):
                    self._send_json({"ok": False, "error": "item não encontrado"}, status=404)
                    return
                save_lixeira(novo)
                self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/lixeira/esvaziar":
            try:
                save_lixeira([])
                self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/tx-be1001/leitura":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                action = payload.get("action")
                data = load_tx_be1001_data()

                if action == "add":
                    metrics = {k: float(v) for k, v in (payload.get("metrics") or {}).items() if v not in (None, "")}
                    obs_auto, alerta = diagnosticar_leitura_tx_be1001(metrics, data["faixas_normais"], data["leituras"])
                    obs_manual = (payload.get("observacao_manual") or "").strip()
                    new_id = str(uuid.uuid4())
                    data["leituras"].append({
                        "id": new_id,
                        "data": payload.get("data") or time.strftime("%Y-%m-%d"),
                        "responsavel": (payload.get("responsavel") or "").strip(),
                        "metrics": metrics,
                        "observacao_manual": obs_manual,
                        "observacao_automatica": obs_auto,
                        "alerta": alerta,
                        "importado_da_planilha": False,
                    })
                    data["leituras"].sort(key=lambda l: l["data"])
                    save_tx_be1001_data(data)
                    self._send_json({"ok": True, "new_id": new_id, "observacao_automatica": obs_auto, "alerta": alerta})
                    return
                elif action == "delete":
                    leitura_del = next((l for l in data["leituras"] if l["id"] == payload.get("id")), None)
                    if leitura_del:
                        mover_para_lixeira(
                            "tx_be1001_leitura",
                            f"TX BE 100.1: leitura de {leitura_del.get('data','?')} ({leitura_del.get('responsavel','?')})",
                            leitura_del
                        )
                    data["leituras"] = [l for l in data["leituras"] if l["id"] != payload.get("id")]
                else:
                    self._send_json({"ok": False, "error": "acao invalida"}, status=400)
                    return
                save_tx_be1001_data(data)
                self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/tx-be1001/valvula":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                action = payload.get("action")
                data = load_tx_be1001_data()
                if action == "add":
                    new_id = str(uuid.uuid4())
                    data["trocas_valvula"].append({
                        "id": new_id,
                        "data": payload.get("data") or time.strftime("%Y-%m-%d"),
                        "observacao": (payload.get("observacao") or "").strip(),
                    })
                    data["trocas_valvula"].sort(key=lambda t: t["data"])
                elif action == "delete":
                    troca_del = next((t for t in data["trocas_valvula"] if t["id"] == payload.get("id")), None)
                    if troca_del:
                        mover_para_lixeira("tx_be1001_troca_valvula", f"TX BE 100.1: troca de válvula em {troca_del.get('data','?')}", troca_del)
                    data["trocas_valvula"] = [t for t in data["trocas_valvula"] if t["id"] != payload.get("id")]
                else:
                    self._send_json({"ok": False, "error": "acao invalida"}, status=400)
                    return
                save_tx_be1001_data(data)
                self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/tx-be1001/faixa":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                data = load_tx_be1001_data()
                key = payload.get("key")
                if key not in data["faixas_normais"]:
                    self._send_json({"ok": False, "error": "métrica não encontrada"}, status=404)
                    return
                data["faixas_normais"][key]["min"] = float(payload.get("min"))
                data["faixas_normais"][key]["max"] = float(payload.get("max"))
                save_tx_be1001_data(data)
                self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/relatorios-sites/photo":
            try:
                length = int(self.headers.get("Content-Length", 0))
                if length > int(MAX_PHOTO_BYTES * 1.4):  # base64 e ~33% maior que o binario original
                    self._send_json({"ok": False, "error": "foto muito grande"}, status=413)
                    return
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                filename = save_task_photo(payload.get("data", ""), payload.get("filename", ""))
                self._send_json({"ok": True, "filename": filename})
            except ValueError as e:
                self._send_json({"ok": False, "error": str(e)}, status=400)
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/monthly-report/manual-entry":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                map_id = (payload.get("map") or "interior").strip()
                site = (payload.get("site") or "").strip()
                date_str = (payload.get("date") or "").strip()
                description = (payload.get("description") or "").strip()
                start_time = (payload.get("start_time") or "").strip()
                end_time = (payload.get("end_time") or "").strip() or None
                observation = (payload.get("observation") or "").strip()

                if not site or not date_str or not description:
                    self._send_json({"ok": False, "error": "preencha local, data e descrição"}, status=400)
                    return
                try:
                    time.strptime(date_str, "%Y-%m-%d")
                except ValueError:
                    self._send_json({"ok": False, "error": "data invalida (use AAAA-MM-DD)"}, status=400)
                    return

                entry = add_manual_entry(map_id, site, date_str, description, start_time, end_time, observation)
                log_event("OCORRENCIA MANUAL ADICIONADA", f"[{map_id}] {site}", None, detail=description)
                self._send_json({"ok": True, "entry": entry})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/monthly-report/manual-entry/delete":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                entry_id = payload.get("id")
                if not entry_id:
                    self._send_json({"ok": False, "error": "id ausente"}, status=400)
                    return
                found = delete_manual_entry(entry_id)
                if not found:
                    self._send_json({"ok": False, "error": "entrada nao encontrada"}, status=404)
                    return
                self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/toggle-report-device":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                dev_id = payload.get("id")
                enabled = bool(payload.get("enabled"))
                if dev_id is None:
                    self._send_json({"ok": False, "error": "id ausente"}, status=400)
                    return
                found = set_device_report_enabled(int(dev_id), enabled)
                if not found:
                    self._send_json({"ok": False, "error": "dispositivo nao encontrado"}, status=404)
                    return
                self._send_json({"ok": True, "id": dev_id, "report_enabled": enabled})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/rename-device":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                dev_id = payload.get("id")
                new_name = (payload.get("name") or "").strip()
                if dev_id is None:
                    self._send_json({"ok": False, "error": "id ausente"}, status=400)
                    return
                if not new_name:
                    self._send_json({"ok": False, "error": "nome nao pode ficar vazio"}, status=400)
                    return
                found = _set_device_field(int(dev_id), "name", new_name, "NOME ALTERADO", "NOME ALTERADO")
                if not found:
                    self._send_json({"ok": False, "error": "dispositivo nao encontrado"}, status=404)
                    return
                self._send_json({"ok": True, "id": dev_id, "name": new_name})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/devices/add":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                new_dev, error = add_device(payload)
                if error:
                    self._send_json({"ok": False, "error": error}, status=400)
                    return
                log_event("DISPOSITIVO ADICIONADO", new_dev["name"], new_dev.get("ip"))
                self._send_json({"ok": True, "device": new_dev})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/alarm-schedule":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")

                current = load_alarm_schedule()
                for map_key in ("interior", "fg"):
                    if map_key in payload:
                        start = payload[map_key].get("start", current[map_key]["start"])
                        end = payload[map_key].get("end", current[map_key]["end"])
                        if not re.match(r"^\d{2}:\d{2}$", start) or not re.match(r"^\d{2}:\d{2}$", end):
                            self._send_json({"ok": False, "error": f"horario invalido para {map_key}"}, status=400)
                            return
                        current[map_key] = {"start": start, "end": end}

                save_alarm_schedule(current)
                log_event("HORARIO DE ALARME ATUALIZADO", "sistema", None,
                          detail=json.dumps(current, ensure_ascii=False))
                self._send_json({"ok": True, "schedule": current})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/sunday-summary":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                cfg = load_sunday_summary_config()
                cfg["enabled"] = bool(payload.get("enabled"))
                save_sunday_summary_config(cfg)
                log_event("RESUMO DE DOMINGO " + ("ATIVADO" if cfg["enabled"] else "DESATIVADO"), "sistema", None)
                self._send_json({"ok": True, "enabled": cfg["enabled"]})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/agenda-summary":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                cfg = load_agenda_summary_config()
                cfg["enabled"] = bool(payload.get("enabled"))
                if "hour" in payload:
                    cfg["hour"] = max(0, min(23, int(payload.get("hour") or 0)))
                if "minute" in payload:
                    cfg["minute"] = max(0, min(59, int(payload.get("minute") or 0)))
                if "skip_if_empty" in payload:
                    cfg["skip_if_empty"] = bool(payload.get("skip_if_empty"))
                save_agenda_summary_config(cfg)
                log_event("RESUMO DE AGENDA " + ("ATIVADO" if cfg["enabled"] else "DESATIVADO"), "sistema", None)
                self._send_json({"ok": True, **cfg})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/agenda-summary/test":
            try:
                text = build_agenda_summary_text()
                send_telegram_message(text)
                log_event("RESUMO DE AGENDA - TESTE ENVIADO", "sistema", None)
                self._send_json({"ok": True, "preview": text})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/agenda-summary-amanha":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                cfg = load_agenda_summary_amanha_config()
                cfg["enabled"] = bool(payload.get("enabled"))
                if "hour" in payload:
                    cfg["hour"] = max(0, min(23, int(payload.get("hour") or 0)))
                if "minute" in payload:
                    cfg["minute"] = max(0, min(59, int(payload.get("minute") or 0)))
                if "skip_if_empty" in payload:
                    cfg["skip_if_empty"] = bool(payload.get("skip_if_empty"))
                save_agenda_summary_amanha_config(cfg)
                log_event("AVISO DE AGENDA (AMANHA) " + ("ATIVADO" if cfg["enabled"] else "DESATIVADO"), "sistema", None)
                self._send_json({"ok": True, **cfg})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/agenda-summary-amanha/test":
            try:
                text = build_agenda_summary_text(day_offset=1)
                send_telegram_message(text)
                log_event("AVISO DE AGENDA (AMANHA) - TESTE ENVIADO", "sistema", None)
                self._send_json({"ok": True, "preview": text})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/annual-report/downtime-transmissores/exclude":
            # exclusao PROPRIA desse relatorio (nao mexe na lista de
            # excluidos do relatorio mensal/logs) - o botao de lixeira
            # ao lado de cada incidente na tabela chama isso.
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                inc_id = payload.get("id")
                action = payload.get("action", "exclude")
                if not inc_id:
                    self._send_json({"ok": False, "error": "id obrigatorio"}, status=400)
                    return
                excluded = load_downtime_excluded()
                if action == "restore":
                    excluded.discard(inc_id)
                else:
                    excluded.add(inc_id)
                save_downtime_excluded(excluded)
                log_event(
                    "INCIDENTE " + ("EXCLUIDO" if action != "restore" else "RESTAURADO") + " DO TEMPO FORA DO AR",
                    "sistema", None,
                )
                self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/network-config":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")

                import ipaddress
                raw_ranges = payload.get("extra_ranges", [])
                clean_ranges = []
                for r in raw_ranges:
                    r = (r or "").strip()
                    if not r:
                        continue
                    try:
                        ipaddress.ip_network(r, strict=False)
                    except ValueError:
                        self._send_json({"ok": False, "error": f"faixa invalida: {r} (use o formato CIDR, ex: 203.0.113.0/24)"}, status=400)
                        return
                    clean_ranges.append(r)

                cfg = {
                    "restrict_enabled": bool(payload.get("restrict_enabled")),
                    "extra_ranges": clean_ranges,
                }
                save_network_config(cfg)
                log_event("CONFIGURACAO DE REDE ATUALIZADA", "sistema", None,
                          detail=json.dumps(cfg, ensure_ascii=False))
                self._send_json({"ok": True, "config": cfg})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path in ("/api/comandos/login", "/api/comandos/senha", "/api/comandos/executar", "/api/comandos/sair"):
            try:
                length = int(self.headers.get("Content-Length", 0))
                payload = json.loads((self.rfile.read(length) if length else b"{}").decode("utf-8") or "{}")
                if parsed.path == "/api/comandos/login":
                    self._send_json({"ok": True, "token": cmd_login(payload.get("senha"))})
                elif parsed.path == "/api/comandos/senha":
                    cmd_set_password(payload.get("nova"), payload.get("atual"))
                    log_event("SENHA DE COMANDOS ALTERADA", "Comandos", self.client_address[0])
                    self._send_json({"ok": True, "token": cmd_login(payload.get("nova"))})
                elif parsed.path == "/api/comandos/sair":
                    CMD_TOKENS.pop(self.headers.get("X-Cmd-Token") or "", None)
                    self._send_json({"ok": True})
                else:
                    if not cmd_token_ok(self.headers.get("X-Cmd-Token")):
                        self._send_json({"ok": False, "error": "senha de comandos necessaria"}, status=401)
                        return
                    entry = execute_command(payload.get("id"), payload.get("valor"), self.client_address[0])
                    self._send_json({"ok": True, "entry": entry})
            except PermissionError as e:
                self._send_json({"ok": False, "error": str(e)}, status=403)
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=400)
        elif parsed.path in ("/api/knx/point", "/api/knx/request-status", "/api/knx/reset"):
            try:
                length = int(self.headers.get("Content-Length", 0))
                payload = json.loads((self.rfile.read(length) if length else b"{}").decode("utf-8") or "{}")
                if KNX_MONITOR is None:
                    raise RuntimeError("modulo KNX nao iniciado")
                if parsed.path == "/api/knx/point":
                    pt = KNX_MONITOR.update_point(str(payload.get("ga")), payload)
                    if pt is None:
                        raise RuntimeError("endereco de grupo nao encontrado")
                    self._send_json({"ok": True, "point": pt})
                elif parsed.path == "/api/knx/reset":
                    if payload.get("confirmar") != "RESET":
                        raise RuntimeError("confirmacao ausente")
                    ga = KNX_MONITOR.reset_central(str(payload.get("grupo") or ""), origem=self.client_address[0])
                    self._send_json({"ok": True, "ga": ga})
                else:
                    n = KNX_MONITOR.request_status()
                    self._send_json({"ok": True, "enviados": n})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=400)
        elif parsed.path == "/api/polling-config":
            try:
                length = int(self.headers.get("Content-Length", 0))
                payload = json.loads((self.rfile.read(length) if length else b"{}").decode("utf-8") or "{}")
                cfg = save_polling_config(payload)
                log_event("VELOCIDADE DE LEITURA ALTERADA", f"FG {cfg['fg_interval_s']}s / abrigos {cfg['abrigos_interval_s']}s", None)
                self._send_json({"ok": True, "config": cfg})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=400)
        elif parsed.path in ("/api/abrigos/save", "/api/abrigos/delete", "/api/abrigos/move-device", "/api/devices/delete",
                             "/api/abrigos/location"):
            try:
                length = int(self.headers.get("Content-Length", 0))
                payload = json.loads((self.rfile.read(length) if length else b"{}").decode("utf-8") or "{}")
            except (ValueError, json.JSONDecodeError):
                self._send_json({"ok": False, "error": "JSON invalido"}, status=400)
                return
            try:
                result = handle_abrigos_api(parsed.path, payload)
                self._send_json(result, status=200 if result.get("ok") else 400)
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path in ("/api/telegram-config", "/api/telegram-test", "/api/telegram-discover"):
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
            except (ValueError, json.JSONDecodeError):
                self._send_json({"ok": False, "error": "JSON invalido"}, status=400)
                return
            try:
                if parsed.path == "/api/telegram-config":
                    cfg = save_telegram_config(payload.get("enabled"), payload.get("bot_token"), payload.get("chat_id"))
                    self._send_json({"ok": True, "config": cfg})
                elif parsed.path == "/api/telegram-discover":
                    self._send_json({"ok": True, **telegram_discover_chats(payload.get("bot_token"))})
                else:
                    token = (payload.get("bot_token") or "").strip() or TELEGRAM_BOT_TOKEN
                    chat = (payload.get("chat_id") or "").strip() or TELEGRAM_CHAT_ID
                    if not token or not chat:
                        self._send_json({"ok": False, "error": "preencha o token e o chat ID"}, status=400)
                        return
                    _telegram_api(token, "sendMessage", {"chat_id": chat, "text":
                        "✅ Sistema Painel - Rede Gazeta\nTeste de alerta enviado em " + time.strftime("%d/%m/%Y %H:%M:%S")})
                    TELEGRAM_LAST["ok_at"] = time.time()
                    self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=502)
        elif parsed.path == "/api/alarm-confirm-config":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                try:
                    metric_count = int(payload.get("metric_confirm_count"))
                    ping_count = int(payload.get("ping_confirm_count"))
                    visual_count = int(payload.get("visual_confirm_count", load_alarm_confirm_config()["visual_confirm_count"]))
                except (TypeError, ValueError):
                    self._send_json({"ok": False, "error": "valores invalidos"}, status=400)
                    return
                if not (1 <= metric_count <= 60) or not (1 <= ping_count <= 60):
                    self._send_json({"ok": False, "error": "os valores devem ficar entre 1 e 60 leituras"}, status=400)
                    return
                if not (1 <= visual_count <= VISUAL_CONFIRM_COUNT_MAX):
                    self._send_json({"ok": False, "error": f"o ciclo do mapa deve ficar entre 1 e {VISUAL_CONFIRM_COUNT_MAX}"}, status=400)
                    return
                cfg = save_alarm_confirm_config(metric_count, ping_count, visual_count)
                log_event("CONFIGURACAO DE CONFIRMACAO DE ALARME ATUALIZADA", "sistema", None,
                          detail=json.dumps(cfg, ensure_ascii=False))
                self._send_json({"ok": True, "config": cfg})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path.startswith("/api/layout/"):
            map_name = parsed.path.rsplit("/", 1)[-1]
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                save_map_layout(map_name, payload)
                log_event("LAYOUT DO MAPA SALVO", "Mapa " + map_name, None)
                self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path.startswith("/api/message-template/"):
            map_name = parsed.path.rsplit("/", 1)[-1]
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                ping_tpl = payload.get("ping", "").strip() or DEFAULT_PING_TEMPLATE
                metric_tpl = payload.get("metric", "").strip() or DEFAULT_METRIC_TEMPLATE

                all_templates = load_message_templates()
                all_templates[map_name] = {"ping": ping_tpl, "metric": metric_tpl}
                save_message_templates(all_templates)
                log_event("MODELO DE MENSAGEM SALVO", "Mapa " + map_name, None)
                self._send_json({"ok": True, "templates": all_templates[map_name]})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/maps":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                display_name = (payload.get("name") or "").strip()
                if not display_name:
                    self._send_json({"ok": False, "error": "nome do mapa e obrigatorio"}, status=400)
                    return
                map_id, error = create_new_map(display_name, display_name)
                if error:
                    self._send_json({"ok": False, "error": error}, status=400)
                    return
                log_event("MAPA CRIADO", display_name, None)
                self._send_json({"ok": True, "id": map_id, "name": display_name, "url": f"/mapa/{map_id}"})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path.startswith("/api/map-devices/") and parsed.path.endswith("/add"):
            map_id = parsed.path.split("/")[-2]
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                new_dev, error = add_device_to_map(map_id, payload)
                if error:
                    self._send_json({"ok": False, "error": error}, status=400)
                    return
                log_event("DISPOSITIVO ADICIONADO", f"[{map_id}] {new_dev['name']}", new_dev.get("ip"))
                self._send_json({"ok": True, "device": new_dev})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/library/move":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                dev_id = payload.get("id")
                if dev_id is None:
                    self._send_json({"ok": False, "error": "id ausente"}, status=400)
                    return
                saved, error = move_device_to_library(int(dev_id))
                if error:
                    self._send_json({"ok": False, "error": error}, status=400)
                    return
                log_event("DISPOSITIVO MOVIDO PARA BIBLIOTECA", saved.get("name", "?"), saved.get("ip"))
                self._send_json({"ok": True, "device": saved})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/library/paste":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                dev_id = payload.get("id")
                target_map = (payload.get("target_map") or "").strip()
                target_site = (payload.get("target_site") or "").strip()
                if dev_id is None or not target_map:
                    self._send_json({"ok": False, "error": "id e mapa de destino sao obrigatorios"}, status=400)
                    return
                saved, error = paste_device_from_library(int(dev_id), target_map, target_site)
                if error:
                    self._send_json({"ok": False, "error": error}, status=400)
                    return
                log_event("DISPOSITIVO RESTAURADO DA BIBLIOTECA", f"[{target_map}] {saved.get('name','?')}", saved.get("ip"))
                self._send_json({"ok": True, "device": saved})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        elif parsed.path == "/api/edit-device":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                dev_id = payload.get("id")
                if dev_id is None:
                    self._send_json({"ok": False, "error": "id ausente"}, status=400)
                    return
                saved, error = edit_device_anywhere(int(dev_id), payload)
                if error:
                    self._send_json({"ok": False, "error": error}, status=400)
                    return
                log_event("DISPOSITIVO EDITADO", saved.get("name", "?"), saved.get("ip"))
                self._send_json({"ok": True, "device": saved})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
        else:
            self.send_error(404, "Nao encontrado")


def _set_local_fields(target, item):
    """lat/lon/edp vindos da tela (Config ou mapa do abrigo)."""
    for k in ("lat", "lon"):
        if k in item and item[k] not in (None, ""):
            v = float(str(item[k]).replace(",", "."))
            if k == "lat" and not -90 <= v <= 90 or k == "lon" and not -180 <= v <= 180:
                raise ValueError("coordenada invalida")
            target[k] = round(v, 6)
            target["coord_aprox"] = False
    if "edp" in item:
        target["edp"] = re.sub(r"[^0-9A-Za-z.-]", "", str(item.get("edp") or ""))[:20]


def handle_abrigos_api(path, payload):
    """Gerenciamento feito pela tela Configuracao > Abrigos e dispositivos."""
    if path == "/api/abrigos/save":
        cfg = load_abrigos_config()
        fg_in = payload.get("fg") or {}
        if fg_in.get("nome"):
            cfg["fg"]["nome"] = str(fg_in["nome"]).strip()[:80]
        if fg_in.get("alerta"):
            cfg["fg"]["alerta"] = str(fg_in["alerta"]).strip().upper()[:60]
        _set_local_fields(cfg["fg"], fg_in)
        by_slug = {a["slug"]: a for a in cfg["abrigos"]}
        for item in payload.get("abrigos") or []:
            nome = str(item.get("nome") or "").strip()[:80]
            if not nome:
                continue
            slug = item.get("slug")
            if slug and slug in by_slug:
                ab = by_slug[slug]
                ab["nome"] = nome
                if item.get("alerta"):
                    ab["alerta"] = str(item["alerta"]).strip().upper()[:60]
                if "sub" in item:
                    ab["sub"] = str(item.get("sub") or "").strip()[:60]
                _set_local_fields(ab, item)
            else:
                # abrigo novo
                site = _norm_site(re.sub(r"^\s*abrigo\s+", "", nome, flags=re.I))
                if any(_norm_site(a["site"]) == site for a in cfg["abrigos"]):
                    return {"ok": False, "error": "ja existe um abrigo com esse nome"}
                new_slug = _slugify(nome)
                n = 2
                while new_slug in by_slug or new_slug == "fg":
                    new_slug = _slugify(nome) + "-" + str(n)
                    n += 1
                ab = {"slug": new_slug, "site": site, "nome": nome,
                      "alerta": str(item.get("alerta") or site).strip().upper()[:60]}
                if item.get("sub"):
                    ab["sub"] = str(item["sub"]).strip()[:60]
                cfg["abrigos"].append(ab)
                by_slug[new_slug] = ab
        order = payload.get("order")
        if isinstance(order, list) and order:
            cfg["abrigos"].sort(key=lambda a: order.index(a["slug"]) if a["slug"] in order else 999)
        save_abrigos_config(cfg)
        log_event("ABRIGOS ATUALIZADOS", "Configuracao", None)
        return {"ok": True, "config": cfg}

    if path == "/api/abrigos/location":
        slug = payload.get("slug")
        cfg = load_abrigos_config()
        target = cfg["fg"] if slug == "fg" else next((a for a in cfg["abrigos"] if a["slug"] == slug), None)
        if target is None:
            return {"ok": False, "error": "abrigo nao encontrado"}
        _set_local_fields(target, payload)
        save_abrigos_config(cfg)
        threading.Thread(target=prewarm_abrigo_tiles, daemon=True).start()
        log_event("LOCALIZACAO DO ABRIGO", target.get("nome"), None,
                  detail=f"{target.get('lat')}, {target.get('lon')}")
        return {"ok": True, "local": {k: target.get(k) for k in ("lat", "lon", "edp", "coord_aprox")}}

    if path == "/api/abrigos/delete":
        slug = payload.get("slug")
        cfg = load_abrigos_config()
        ab = next((a for a in cfg["abrigos"] if a["slug"] == slug), None)
        if not ab:
            return {"ok": False, "error": "abrigo nao encontrado"}
        key = _norm_site(ab["site"])
        if any(_norm_site(d.get("site")) == key for d in load_devices().get("devices", [])):
            return {"ok": False, "error": "o abrigo ainda tem dispositivos - mova ou exclua os dispositivos antes"}
        cfg["abrigos"] = [a for a in cfg["abrigos"] if a["slug"] != slug]
        save_abrigos_config(cfg)
        log_event("ABRIGO EXCLUIDO", ab["nome"], None)
        return {"ok": True}

    if path == "/api/abrigos/move-device":
        dev_id = int(payload.get("id"))
        target = payload.get("slug")
        source_map, dev = find_device_anywhere(dev_id)
        if source_map is None:
            return {"ok": False, "error": "dispositivo nao encontrado"}
        if target == "fg":
            target_map, group = "fg", (payload.get("category") or dev.get("category") or abrigo_category(dev))
        else:
            ab = abrigo_by_slug(target)
            if not ab:
                return {"ok": False, "error": "abrigo de destino nao encontrado"}
            target_map, group = "interior", ab["site"]
        removed = remove_device_from_map(source_map, dev_id)
        if removed is None:
            return {"ok": False, "error": "falha ao remover do abrigo de origem"}
        if target_map == "interior":
            removed.pop("category", None)
        saved = add_full_device_to_map(target_map, removed, group)
        log_event("DISPOSITIVO MOVIDO", removed.get("name"), removed.get("ip"), detail="para " + str(target))
        return {"ok": True, "device": saved}

    if path == "/api/devices/delete":
        dev_id = int(payload.get("id"))
        source_map, dev = find_device_anywhere(dev_id)
        if source_map is None:
            return {"ok": False, "error": "dispositivo nao encontrado"}
        removed = remove_device_from_map(source_map, dev_id)
        if removed is None:
            return {"ok": False, "error": "falha ao excluir"}
        with STATUS_LOCK:
            STATUS.pop(dev_id, None)
            STATUS.pop(str(dev_id), None)
        log_event("DISPOSITIVO EXCLUIDO", removed.get("name"), removed.get("ip"))
        return {"ok": True}

    return {"ok": False, "error": "acao desconhecida"}


# --------------------------------------------------------------------------
# Previsao do tempo por abrigo - Open-Meteo (gratuito, sem cadastro).
# O servidor busca (ele tem internet pela placa da internet) e guarda por
# 15 min, entao os PCs que so abrem o painel nao precisam de internet.
# --------------------------------------------------------------------------
WEATHER_CACHE = {}
WEATHER_TTL_S = 15 * 60
WEATHER_LOCK = threading.Lock()


def get_weather(lat, lon):
    key = (round(float(lat), 3), round(float(lon), 3))
    now = time.time()
    with WEATHER_LOCK:
        hit = WEATHER_CACHE.get(key)
        if hit and now - hit["fetched_at"] < WEATHER_TTL_S:
            return hit
    params = urllib.parse.urlencode({
        "latitude": key[0], "longitude": key[1],
        "current": "temperature_2m,relative_humidity_2m,apparent_temperature,is_day,weather_code,wind_speed_10m",
        "daily": "weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max",
        "timezone": "America/Sao_Paulo", "forecast_days": 6,
    })
    try:
        req = urllib.request.Request("https://api.open-meteo.com/v1/forecast?" + params,
                                     headers={"User-Agent": "SistemaPainel-RedeGazeta"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        out = {"fetched_at": now, "current": data.get("current") or {}, "daily": data.get("daily") or {},
               "source": "Open-Meteo"}
        with WEATHER_LOCK:
            WEATHER_CACHE[key] = out
        return out
    except Exception as e:
        if hit:   # sem internet agora: devolve o ultimo que tinha, marcado como antigo
            return dict(hit, stale=True, error=str(e))
        return {"error": "previsao indisponivel (" + str(e)[:80] + ")"}


# --------------------------------------------------------------------------
# Mapas sem internet nos PCs: o Debian (que tem internet) busca os "tiles"
# do mapa e guarda em disco (tile_cache/). Os PCs pedem tudo para o painel
# (/tiles/...), entao funcionam so com a rede interna. Tile que ja foi visto
# uma vez continua aparecendo mesmo se a internet do Debian cair.
# --------------------------------------------------------------------------
TILE_CACHE_DIR = os.path.join(BASE_DIR, "tile_cache")
TILE_SOURCES = {
    "osm": ("https://tile.openstreetmap.org/{z}/{x}/{y}.png", "image/png", "png"),
    "sat": ("https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
            "image/jpeg", "jpg"),
}
TILE_UA = "SistemaPainel-RedeGazeta/1.0 (monitoramento interno da transmissao)"
TILE_SEM = threading.Semaphore(6)
TILE_MAX_AGE_S = 90 * 24 * 3600


def get_tile(layer, z, x, y):
    """Retorna (bytes, content_type) do cache ou da internet; None se nao tiver."""
    src = TILE_SOURCES.get(layer)
    if not src or not (0 <= z <= 19) or not (0 <= x < 2 ** z) or not (0 <= y < 2 ** z):
        return None
    url_tpl, ctype, ext = src
    path = os.path.join(TILE_CACHE_DIR, layer, str(z), str(x), f"{y}.{ext}")
    cached = None
    if os.path.exists(path):
        with open(path, "rb") as f:
            cached = f.read()
        if time.time() - os.path.getmtime(path) < TILE_MAX_AGE_S:
            return cached, ctype
    try:
        with TILE_SEM:
            req = urllib.request.Request(url_tpl.format(z=z, x=x, y=y), headers={"User-Agent": TILE_UA})
            with urllib.request.urlopen(req, timeout=8) as resp:
                data = resp.read()
        if data:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            tmp = path + ".tmp"
            with open(tmp, "wb") as f:
                f.write(data)
            os.replace(tmp, path)
            return data, ctype
    except Exception:
        pass
    return (cached, ctype) if cached else None


def _deg2tile(lat, lon, z):
    import math
    n = 2 ** z
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2.0 * n)
    return x, y


def prewarm_abrigo_tiles():
    """Na partida, guarda as imagens de satelite em volta de cada abrigo (so as
    que ainda nao estao no cache), para o quadro do abrigo aparecer mesmo que
    a internet do Debian esteja fora quando alguem abrir o mapa."""
    try:
        cfg = load_abrigos_config()
        locais = [cfg["fg"]] + cfg["abrigos"]
        for loc in locais:
            if loc.get("lat") is None:
                continue
            for z, r in ((14, 1), (16, 2)):
                cx, cy = _deg2tile(float(loc["lat"]), float(loc["lon"]), z)
                for dx in range(-r, r + 1):
                    for dy in range(-r, r + 1):
                        path = os.path.join(TILE_CACHE_DIR, "sat", str(z), str(cx + dx), f"{cy + dy}.jpg")
                        if not os.path.exists(path):
                            get_tile("sat", z, cx + dx, cy + dy)
                            time.sleep(0.2)
    except Exception as e:
        print("[tiles] pre-carga falhou:", e)


# ==========================================================================
# COMANDOS (tela /comandos, protegida por SENHA PROPRIA)
# --------------------------------------------------------------------------
# Junta numa tela so os comandos que os equipamentos aceitam:
#   - SNMP  : medidas "Comando ... - Envia 1" (FLEX: ligar/desligar TX)  -> SET
#   - Modbus: registradores "( Somente Escrita )" do CLP de climatizacao -> FC6/FC5
#   - KNX   : Reset, armar/desarmar intrusao, desativar zona, iluminacao
# Todo comando pede confirmacao, fica no log e avisa no Telegram.
# ==========================================================================
COMANDOS_FILE = os.path.join(BASE_DIR, "comandos_config.json")
COMANDOS_LOG_FILE = os.path.join(BASE_DIR, "comandos_log.json")
CMD_TOKENS = {}                 # token -> expira_em
CMD_TOKEN_TTL_S = 15 * 60       # sessao de comandos: 15 min sem uso
CMD_LOCK = threading.Lock()


def _cmd_cfg():
    try:
        with open(COMANDOS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _cmd_hash(senha, salt):
    return hashlib.sha256((salt + ":" + senha).encode("utf-8")).hexdigest()


def cmd_set_password(nova, atual=None):
    cfg = _cmd_cfg()
    if cfg.get("senha_hash"):
        if not atual or _cmd_hash(atual, cfg["salt"]) != cfg["senha_hash"]:
            raise PermissionError("senha atual incorreta")
    if not nova or len(nova) < 4:
        raise ValueError("a senha de comandos precisa ter pelo menos 4 caracteres")
    salt = secrets.token_hex(8)
    cfg.update(salt=salt, senha_hash=_cmd_hash(nova, salt))
    with open(COMANDOS_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
    CMD_TOKENS.clear()


def cmd_login(senha):
    cfg = _cmd_cfg()
    if not cfg.get("senha_hash"):
        raise PermissionError("senha de comandos ainda nao foi criada")
    if _cmd_hash(senha or "", cfg["salt"]) != cfg["senha_hash"]:
        time.sleep(1.0)   # dificulta tentativa e erro
        raise PermissionError("senha incorreta")
    token = secrets.token_hex(16)
    CMD_TOKENS[token] = time.time() + CMD_TOKEN_TTL_S
    return token


def cmd_token_ok(token):
    exp = CMD_TOKENS.get(token or "")
    if not exp or exp < time.time():
        CMD_TOKENS.pop(token or "", None)
        return False
    CMD_TOKENS[token] = time.time() + CMD_TOKEN_TTL_S   # renova enquanto usa
    return True


def snmp_set_integer(ip, community, oid, value, version="1", port=161, timeout_s=3):
    ver_num = 1 if str(version) in ("2", "2c") else 0
    oid = (oid or "").strip().strip(".")
    req_id = int(time.time() * 1000) & 0x7FFFFFFF
    varbind = _ber_tlv(0x30, _ber_oid(oid) + _ber_int(int(value)))
    pdu = _ber_tlv(0xA3, _ber_int(req_id) + _ber_int(0) + _ber_int(0) + _ber_tlv(0x30, varbind))  # SetRequest
    packet = _ber_tlv(0x30, _ber_int(ver_num) + _ber_tlv(0x04, community.encode("utf-8")) + pdu)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout_s)
    try:
        sock.sendto(packet, (ip, port))
        data, _ = sock.recvfrom(4096)
    finally:
        sock.close()
    r = _BerReader(data)
    _, seq = r.read_tlv()
    inner = _BerReader(seq)
    inner.read_tlv(); inner.read_tlv()
    _, pdu_body = inner.read_tlv()
    p = _BerReader(pdu_body)
    p.read_tlv()
    _, err = p.read_tlv()
    code = int.from_bytes(err, "big") if err else 0
    if code:
        raise RuntimeError({2: "OID nao existe", 3: "valor invalido", 4: "somente leitura",
                            5: "erro no equipamento", 6: "sem permissao (community de escrita?)",
                            17: "somente leitura"}.get(code, f"erro SNMP {code}"))
    return True


def modbus_write(device, metric, raw_value):
    cfg = device.get("modbus") or {}
    ip, port = device.get("ip"), int(cfg.get("port") or MODBUS_DEFAULT_PORT)
    unit = int(cfg.get("unit_id") or 1)
    rtype = normalize_register_type(metric.get("register_type"))
    addr = int(metric.get("address"))
    with _modbus_endpoint_lock(ip, port):
        c = ModbusTCPClient(ip, port, 5)
        try:
            c.connect()
            c._tid = (c._tid + 1) & 0xFFFF
            if rtype == "coil":
                pdu = struct.pack(">BHH", 5, addr, 0xFF00 if raw_value else 0x0000)
            else:
                v = int(raw_value)
                pdu = struct.pack(">BHH", 6, addr, v & 0xFFFF)
            c.sock.sendall(struct.pack(">HHHB", c._tid, 0, len(pdu) + 1, unit) + pdu)
            hdr = c._recv_exact(7)
            body = c._recv_exact(struct.unpack(">H", hdr[4:6])[0] - 1)
            if body[0] & 0x80:
                raise ModbusError("equipamento recusou (excecao %s)" % body[1], body[1])
        finally:
            c.close()
    return True


def _scale_for_write(label):
    # setpoint/histerese/temperaturas do CLP sao em centesimos de grau
    return 100 if re.search(r"setpoint|histerese", label, re.I) else 1


def build_commands():
    """Lista de comandos disponiveis, agrupada por equipamento."""
    cmds = []
    with STATUS_LOCK:
        snap = dict(STATUS)
    for dev in load_all_devices_for_polling():
        abrigo = abrigo_alert_name(dev)
        st = snap.get(dev.get("id")) or snap.get(str(dev.get("id"))) or {}
        for m in dev.get("metrics", []):
            label = m.get("label") or ""
            if dev.get("protocol") != "modbus" and m.get("oid") and re.search(r"^comando|envia 1", label, re.I):
                nome = re.sub(r"\s*-\s*envia 1\s*$", "", label, flags=re.I)
                cmds.append({"id": f"snmp|{dev['id']}|{label}", "abrigo": abrigo, "equipamento": dev.get("name"),
                             "nome": nome, "tipo": "pulso", "rotulo": "Enviar",
                             "detalhe": f"SNMP SET {m['oid']} = 1"})
            elif dev.get("protocol") == "modbus" and re.search(r"somente escrita|netx prote", label, re.I):
                nome = re.sub(r"\s*\(\s*somente escrita\s*\)\s*", "", label, flags=re.I).strip()
                leitura = re.sub(r"somente escrita", "somente leitura", label, flags=re.I).lower()
                atual = next((v.get("display") for k, v in (st.get("metrics") or {}).items()
                              if k.lower() == leitura), None)
                if normalize_register_type(m.get("register_type")) == "coil":
                    cmds.append({"id": f"modbus|{dev['id']}|{label}", "abrigo": abrigo, "equipamento": dev.get("name"),
                                 "nome": nome, "tipo": "liga_desliga", "on": "Ligar", "off": "Desligar",
                                 "detalhe": f"Modbus coil {m.get('address')}"})
                else:
                    esc = _scale_for_write(label)
                    cmds.append({"id": f"modbus|{dev['id']}|{label}", "abrigo": abrigo, "equipamento": dev.get("name"),
                                 "nome": nome, "tipo": "valor", "atual": atual, "unidade": "°C" if esc == 100 else "",
                                 "detalhe": f"Modbus holding {m.get('address')}" + (" (x100)" if esc != 1 else "")})
    if KNX_MONITOR is not None:
        for p in KNX_MONITOR.cfg.get("points", []):
            n = str(p.get("nome", ""))
            base = {"id": f"knx|{p['ga']}", "abrigo": "FONTE GRANDE", "equipamento": "KNX " + str(p.get("grupo")),
                    "detalhe": f"KNX {p['ga']}"}
            if p.get("tipo") == "comando" and n.lower().startswith("reset"):
                cmds.append(dict(base, nome="Resetar central (todas as zonas)", tipo="pulso", rotulo="Resetar"))
            elif p.get("tipo") == "comando" and n.lower().startswith("on/off"):
                cmds.append(dict(base, nome=re.sub(r"^ON/OFF\s*", "", n), tipo="liga_desliga", on="Armar", off="Desarmar"))
            elif p.get("tipo") == "comando" and n.lower().startswith("desativar"):
                cmds.append(dict(base, nome=re.sub(r"^Desativar\s*", "", n), tipo="liga_desliga", on="Desativar zona", off="Reativar zona"))
            elif p.get("tipo") == "iluminacao":
                cmds.append(dict(base, nome=n, tipo="liga_desliga", on="Ligar", off="Desligar"))
    return cmds


def _cmd_log(entry):
    try:
        with open(COMANDOS_LOG_FILE, encoding="utf-8") as f:
            hist = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        hist = []
    hist.insert(0, entry)
    with open(COMANDOS_LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(hist[:300], f, ensure_ascii=False, indent=1)


def execute_command(cmd_id, valor, origem):
    cmd = next((c for c in build_commands() if c["id"] == cmd_id), None)
    if not cmd:
        raise RuntimeError("comando nao encontrado")
    kind = cmd_id.split("|", 1)[0]
    if cmd["tipo"] == "pulso":
        valor = 1
    elif cmd["tipo"] == "liga_desliga":
        valor = 1 if valor in (1, "1", True, "on") else 0
    else:
        valor = float(str(valor).replace(",", "."))
    if kind == "snmp":
        _, dev_id, label = cmd_id.split("|", 2)
        _, dev = find_device_anywhere(int(dev_id))
        m = next(x for x in dev["metrics"] if x["label"] == label)
        snmp_set_integer(dev["ip"], m.get("community") or "public", m["oid"], 1, version=m.get("version") or "1")
    elif kind == "modbus":
        _, dev_id, label = cmd_id.split("|", 2)
        _, dev = find_device_anywhere(int(dev_id))
        m = next(x for x in dev["metrics"] if x["label"] == label)
        raw = valor if cmd["tipo"] != "valor" else round(valor * _scale_for_write(label))
        modbus_write(dev, m, raw)
    elif kind == "knx":
        if KNX_MONITOR is None or not (KNX_MONITOR.tunnel and KNX_MONITOR.tunnel.connected):
            raise ConnectionError("tunel KNX desconectado")
        KNX_MONITOR.tunnel.group_write_bit(cmd_id.split("|", 1)[1], int(valor))
    else:
        raise RuntimeError("tipo de comando desconhecido")
    acao = cmd.get("rotulo") if cmd["tipo"] == "pulso" else (
        (cmd.get("on") if valor else cmd.get("off")) if cmd["tipo"] == "liga_desliga" else f"= {valor:g} {cmd.get('unidade', '')}".strip())
    entry = {"ts": time.time(), "equipamento": cmd["equipamento"], "comando": cmd["nome"], "acao": acao,
             "origem": origem, "abrigo": cmd["abrigo"]}
    _cmd_log(entry)
    log_event("COMANDO ENVIADO", cmd["equipamento"], None, detail=f"{cmd['nome']}: {acao} (PC {origem})")
    try:
        publish_alert(f"⚙️ COMANDO - {cmd['abrigo']}\n\nEQUIPAMENTO: {cmd['equipamento']}\n"
                              f"COMANDO: {cmd['nome']}\nAÇÃO: {acao}\nPC: {origem}")
    except Exception:
        pass
    return entry


# ==========================================================================
# CENTRAL DE ALERTAS (app de celular /app)
# --------------------------------------------------------------------------
# Todo alerta que o painel gera (o mesmo texto que vai pro Telegram) e
# guardado aqui ja separado em campos (abrigo, equipamento, tipo, status),
# para o app mostrar com filtros. Guarda mesmo quando o Telegram esta fora
# do horario de disparo. Bot do Telegram nao consegue ler o historico do
# grupo - por isso a fonte e o proprio painel.
# ==========================================================================
ALERTAS_FILE = os.path.join(BASE_DIR, "alertas.json")
ALERTAS_MAX = 3000
ALERTAS_DIAS = 30
ALERTAS = []
ALERTAS_LOCK = threading.Lock()
_ALERTA_SEQ = [0]


def _load_alertas():
    global ALERTAS
    try:
        with open(ALERTAS_FILE, encoding="utf-8") as f:
            ALERTAS = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        ALERTAS = []
    _ALERTA_SEQ[0] = max([a.get("id", 0) for a in ALERTAS] + [0])


def _save_alertas():
    corte = time.time() - ALERTAS_DIAS * 86400
    with ALERTAS_LOCK:
        ALERTAS[:] = [a for a in ALERTAS if a.get("ts", 0) >= corte][-ALERTAS_MAX:]
        data = list(ALERTAS)
    tmp = ALERTAS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, ALERTAS_FILE)


def _categoria(texto):
    t = texto.upper()
    if "COMANDO" in t or "RESET DA CENTRAL" in t or "🔄" in texto or "⚙" in texto:
        return "comando"
    if "INCÊNDIO" in t or "TSDA" in t or re.search(r"\b(DF\d?|DC|A/D)[-\s]", t) or "🔥" in texto:
        return "incendio"
    if "INTRUS" in t or "ALIMENTAÇÃO 12" in t or "ALARME GERAL" in t or "PRESENÇA" in t:
        return "seguranca"
    if re.search(r"SEM LEITURA|LEITURA (SNMP|MODBUS) NORMALIZADA|SEM COMUNICA|\bDOWN\b|\bUP \(|\bPING\b", t):
        return "comunicacao"
    return "medida"


def parse_alert_text(texto):
    """Quebra o texto do alerta em itens com campos (um por ponto no caso
    das mensagens do KNX que juntam varios pontos)."""
    linhas = [l.strip() for l in texto.split("\n")]
    cab = next((l for l in linhas if l), "")
    icone = cab.split(" ", 1)[0] if cab else ""
    m = re.search(r"ALERTA\s+(.+)$", cab) or re.search(r"COMANDO\s*-\s*(.+)$", cab)
    abrigo = (m.group(1).strip() if m else "").upper()
    corpo = [l for l in linhas[linhas.index(cab) + 1:] if l] if cab in linhas else []
    campos = {}
    soltas = []
    for l in corpo:
        mm = re.match(r"^([A-ZÇÃÁÉÍÓÚÊÔ ]{2,20})\s*[:=]\s*(.*)$", l)
        if mm:
            campos[mm.group(1).strip().upper()] = mm.group(2).strip()
        else:
            soltas.append(l)
    base_nivel = "normal" if icone == "✅" else ("info" if icone in ("⚙️", "⚙", "🔄") else "alarme")
    # mensagem do KNX com varios pontos: "⚠️ MT/S - 02 · Zona A - DF1-AR → ALARME"
    pontos = [l for l in soltas if "→" in l]
    itens = []
    if pontos:
        for l in pontos:
            mm = re.match(r"^(\S+)\s+(.+?)\s+·\s+(.+?)\s+→\s+(\S+)", l)
            if not mm:
                continue
            ic, central, ponto, st = mm.groups()
            itens.append({"icone": "🔥" if _categoria(ponto) == "incendio" and st != "NORMAL" else ic,
                          "abrigo": abrigo or "FONTE GRANDE", "equipamento": central, "titulo": ponto,
                          "status": st, "nivel": "normal" if st == "NORMAL" else "alarme",
                          "categoria": _categoria(ponto) if _categoria(ponto) != "medida" else "seguranca"})
    else:
        titulo = soltas[0] if soltas else (campos.get("COMANDO") or "")
        status = campos.get("STATUS") or campos.get("AÇÃO") or ""
        nivel = base_nivel
        if re.match(r"^(OK|UP|NORMAL)", status.upper()) or "NORMALIZADA" in titulo.upper():
            nivel = "normal"
        itens.append({"icone": icone, "abrigo": abrigo, "titulo": titulo,
                      "equipamento": campos.get("EQUIPAMENTO") or campos.get("CENTRAL") or "",
                      "ponto": campos.get("PONTO", ""), "status": status,
                      "valor": campos.get("VALOR ATUAL") or campos.get("MEDIDAS") or "",
                      "ip": campos.get("IP", ""), "nivel": nivel, "categoria": _categoria(texto)})
    for it in itens:
        cat = it["categoria"]
        alvo = it.get("ponto") or it.get("titulo") or ""
        if cat == "comunicacao":
            alvo = "ping" if re.search(r"\bping\b", it.get("titulo", ""), re.I) else "leitura"
        it["chave"] = "|".join([it.get("abrigo", ""), it.get("equipamento", ""), cat, alvo]).upper()
    return itens


def publish_alert(texto, telegram=True):
    """Ponto unico de saida dos alertas: guarda para o app e manda pro Telegram."""
    try:
        itens = parse_alert_text(texto)
        now = time.time()
        with ALERTAS_LOCK:
            for it in itens:
                _ALERTA_SEQ[0] += 1
                it.update(id=_ALERTA_SEQ[0], ts=now, texto=texto, telegram=bool(telegram))
                ALERTAS.append(it)
        threading.Thread(target=_save_alertas, daemon=True).start()
    except Exception as e:
        print("[alertas] nao consegui guardar:", e)
    if telegram:
        _notify_async(texto)


def alertas_snapshot(limit=500, desde_id=0):
    with ALERTAS_LOCK:
        todos = list(ALERTAS)
    ultimo = {}
    for a in todos:
        ultimo[a.get("chave")] = a
    ativos = sorted([a for a in ultimo.values() if a.get("nivel") == "alarme"], key=lambda a: -a["ts"])
    lista = [a for a in todos if a.get("id", 0) > desde_id][-limit:][::-1]
    return {"alertas": lista, "ativos": ativos, "ultimo_id": _ALERTA_SEQ[0], "agora": time.time()}


# --------------------------------------------------------------------------
# Ajustes automaticos de unidade/escala (rodam UMA vez, na partida).
# So mexem em medidas que ainda estao com o valor que veio da importacao -
# se voce ja ajustou alguma no editor, ela nao e tocada.
# --------------------------------------------------------------------------
MIGRATIONS_FILE = os.path.join(BASE_DIR, "migrations.json")


def _limpar_logs_uma_vez():
    """Zera log de eventos, incidentes/falhas e exclusoes - comeca do zero
    so com os alarmes novos. Agenda (planning), dispositivos, configuracoes
    e historico dos graficos NAO sao tocados. Copia de seguranca em backups/."""
    import shutil
    alvos = {
        "eventos.log": "",
        "incidentes.json": "[]",
        "excluded_incidents.json": "[]",
        "downtime_excluded_incidents.json": "[]",
        "annual_archive.json": "{}",
    }
    pasta = os.path.join(BASE_DIR, "backups", "antes_limpeza_" + time.strftime("%Y%m%d_%H%M%S"))
    os.makedirs(pasta, exist_ok=True)
    for nome, vazio in alvos.items():
        path = os.path.join(BASE_DIR, nome)
        if os.path.exists(path):
            shutil.copy2(path, os.path.join(pasta, nome))
        with open(path, "w", encoding="utf-8") as f:
            f.write(vazio)
    print("[limpeza] logs e incidentes zerados (copia em " + pasta + ")")


def apply_data_migrations():
    try:
        with open(MIGRATIONS_FILE, encoding="utf-8") as f:
            done = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        done = {}
    if not done.get("limpeza-logs-2026-10"):
        _limpar_logs_uma_vez()
        done["limpeza-logs-2026-10"] = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(MIGRATIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(done, f, ensure_ascii=False, indent=2)
    todo_units = not done.get("unidades-2026-10")
    todo_clima = not done.get("clima-setpoint-2026-10")
    if not todo_units and not todo_clima:
        return
    changes = 0
    for path in (DEVICES_FILE, os.path.join(BASE_DIR, "network_fg.json")):
        with DEVICES_FILE_LOCK:
            try:
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                continue
            seen = {}
            all_devs = list(data.get("devices", []))
            for grp in (data.get("sites") or data.get("categories") or {}).values():
                all_devs.extend(grp)
            for dev in all_devs:
                netx = str(dev.get("netx_id") or "")
                for m in dev.get("metrics", []):
                    label = (m.get("label") or "").lower()
                    unit = m.get("unit") or ""
                    div = m.get("divisor")
                    # CLP de refrigeracao: setpoint (2400 = 24,00 C) e histerese (50 = 0,50 C)
                    if todo_clima and netx == "FG_PLC_REFRIGERACAO" and re.match(r"^(setpoint|delta histerese)", label) \
                            and unit == "" and not div:
                        m["unit"] = "°C"; m["divisor"] = 100; changes += 1
                        continue
                    if not todo_units:
                        continue
                    # CLP de refrigeracao: temperatura vem em centesimos (2466 = 24,66 C)
                    if netx == "FG_PLC_REFRIGERACAO" and unit == "°C" and div == 10:
                        m["divisor"] = 100; changes += 1
                    # SEPAM (media tensao): tensao em kV
                    elif netx == "FG_SEPAM_S42" and label.startswith("tens") and unit == "V" and not div:
                        m["unit"] = "kV"; m["divisor"] = 1000; changes += 1
                    # excitadores M2X: entrada/modulador sao estados, nao volts
                    elif "M2X" in netx and re.match(r"^entrada \d", label) and unit == "V":
                        m["unit"] = ""; changes += 1
                    # potencia de TX sem unidade -> W
                    elif re.search(r"pot[eê]ncia (direta|refletida)", label) and unit == "" and not m.get("value_labels"):
                        m["unit"] = "W"; changes += 1
                    # nivel de RF dos receptores EITV
                    elif "EITV" in netx and "recep" in label and unit == "":
                        m["unit"] = "dBm"; changes += 1
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, path)
    done.setdefault("unidades-2026-10", time.strftime("%Y-%m-%d %H:%M:%S"))
    done.setdefault("clima-setpoint-2026-10", time.strftime("%Y-%m-%d %H:%M:%S"))
    with open(MIGRATIONS_FILE, "w", encoding="utf-8") as f:
        json.dump(done, f, ensure_ascii=False, indent=2)
    if changes:
        print(f"[ajustes] {changes} medidas tiveram unidade/escala corrigida")


def get_lan_ip():
    """Descobre o IP da maquina na rede local (sem depender de internet)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.5)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return None


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 80

    if not os.path.exists(EVENTS_LOG_FILE):
        try:
            open(EVENTS_LOG_FILE, "w", encoding="utf-8").close()
        except Exception:
            pass

    try:
        apply_data_migrations()
    except Exception as e:
        print("[ajustes] falhou:", e)
    _load_alertas()
    load_incidents()
    load_sessions()
    load_history_from_disk()

    t = threading.Thread(target=polling_loop, daemon=True)
    t.start()

    # imagens do mapa de cada abrigo guardadas no proprio servidor
    threading.Thread(target=prewarm_abrigo_tiles, daemon=True).start()

    # alarme de incendio / intrusao / presenca da Fonte Grande (KNX)
    global KNX_MONITOR
    try:
        import knx_fg
        KNX_MONITOR = knx_fg.KnxFireMonitor(BASE_DIR, notify=lambda text: publish_alert(text), log=log_event)
        KNX_MONITOR.start()
    except Exception as e:
        print("[knx] nao iniciado:", e)

    t2 = threading.Thread(target=sunday_summary_loop, daemon=True)
    t2.start()

    t2b = threading.Thread(target=agenda_summary_loop, daemon=True)
    t2b.start()

    t2c = threading.Thread(target=agenda_summary_amanha_loop, daemon=True)
    t2c.start()

    t3 = threading.Thread(target=annual_archive_loop, daemon=True)
    t3.start()

    t4 = threading.Thread(target=history_autosave_loop, daemon=True)
    t4.start()

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    lan_ip = get_lan_ip()
    print(f"Servidor rodando em http://localhost:{port}")
    if lan_ip:
        print(f"Acesso de outros PCs da rede: http://{lan_ip}:{port}")
        print("(se nao conectar de outro PC, libere a porta no Firewall do Windows - veja o README)")
    print(f"Pagina de configuracao em http://localhost:{port}/config")
    print(f"Log de eventos em http://localhost:{port}/logs")
    print(f"Dashboard de falhas (30 dias) em http://localhost:{port}/dashboard")
    print("Pressione Ctrl+C para parar.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nEncerrando...")
        try:
            save_history_to_disk()
        except Exception:
            pass
        server.shutdown()


if __name__ == "__main__":
    main()
