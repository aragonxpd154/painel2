#!/usr/bin/env python3
"""
Importador das planilhas do NETx (xio_*) para o Sistema Painel - Rede Gazeta
============================================================================

Le as 4 planilhas exportadas do NETx que ficam na pasta  planilhas/ :

    xio_SNMP_DeviceDefinitions.xlsx      -> equipamentos SNMP (IP, community, versao)
    xio_SNMP_PollingDefinitions.xlsx     -> medidas SNMP (OIDs)
    xio_Modbus_DeviceDefinitions.xlsx    -> equipamentos Modbus TCP (IP, porta, Slave ID)
    xio_Modbus_DatapointDefinitions.xlsx -> medidas Modbus (registrador, tipo de dado)

e gera:

    network.json      -> Mapa Abrigos (Sede, Serra, Morro do Ceu, Morro do Moreno,
                         Viana, Domingos Martins, Pedra Azul)
    network_fg.json   -> Mapa FG (Fonte Grande), incluindo os 2 geradores
                         principais e os medidores de energia em Modbus
    layout_interior.json -> posicoes iniciais do Mapa Abrigos (arrastavel depois)

So usa a biblioteca padrao do Python (nao precisa de pip install).

REIMPORTAR SEM PERDER AJUSTES
-----------------------------
Cada dispositivo recebe um ID fixo calculado a partir do nome no NETx, entao
rodar de novo (depois de mexer nas planilhas) NAO perde: posicoes no mapa,
historico, limites de alarme, divisores, unidades, "esconder do mapa" e
interruptores (monitorar / alarme / som / relatorio) que voce ja ajustou.

Uso:
    python importar_planilhas.py            (le planilhas/ e grava os JSON)
    python importar_planilhas.py --reset-layout   (recria as posicoes do Mapa Abrigos)
"""

import json
import os
import re
import sys
import zipfile
import zlib
import xml.etree.ElementTree as ET

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PLANILHAS_DIR = os.path.join(BASE_DIR, "planilhas")
NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

# ---------------------------------------------------------------------------
# Leitor de .xlsx minimo (zip + xml)
# ---------------------------------------------------------------------------

def _col_index(ref):
    letters = re.match(r"[A-Z]+", ref).group(0)
    n = 0
    for ch in letters:
        n = n * 26 + (ord(ch) - 64)
    return n - 1


def read_xlsx_rows(path):
    with zipfile.ZipFile(path) as z:
        shared = []
        if "xl/sharedStrings.xml" in z.namelist():
            root = ET.fromstring(z.read("xl/sharedStrings.xml"))
            for si in root.findall("m:si", NS):
                shared.append("".join(t.text or "" for t in si.iter("{%s}t" % NS["m"])))
        sheet_names = sorted(n for n in z.namelist() if n.startswith("xl/worksheets/sheet"))
        root = ET.fromstring(z.read(sheet_names[0]))
        rows = []
        for row in root.find("m:sheetData", NS).findall("m:row", NS):
            cells = {}
            for c in row.findall("m:c", NS):
                idx = _col_index(c.get("r"))
                t = c.get("t")
                v = c.find("m:v", NS)
                if t == "s" and v is not None:
                    val = shared[int(v.text)]
                elif t == "inlineStr":
                    val = "".join(x.text or "" for x in c.iter("{%s}t" % NS["m"]))
                elif v is not None:
                    val = v.text
                else:
                    val = None
                cells[idx] = val
            if cells:
                rows.append([cells.get(i) for i in range(max(cells) + 1)])
    return rows


def read_table(filename):
    """Retorna lista de dicts (cabecalho da 1a linha), ignorando linhas de
    comentario do NETx (comecam com apostrofo)."""
    path = os.path.join(PLANILHAS_DIR, filename)
    rows = read_xlsx_rows(path)
    header = [(h or "").strip() for h in rows[0]]
    out = []
    for r in rows[1:]:
        first = (r[0] or "").strip() if r else ""
        if not first or first.startswith("'"):
            continue
        item = {}
        for i, h in enumerate(header):
            item[h] = (r[i].strip() if isinstance(r[i], str) else r[i]) if i < len(r) else None
        out.append(item)
    return out


def col(item, prefix):
    """Busca uma coluna pelo comeco do nome (os cabecalhos do NETx trazem
    o valor padrao entre parenteses, ex: 'Port (161)')."""
    for k, v in item.items():
        if k.lower().startswith(prefix.lower()):
            return v
    return None


def as_int(v, default=None):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def flag(v):
    if v is None or str(v).strip() == "":
        return None
    return str(v).strip().upper().startswith("T")


# ---------------------------------------------------------------------------
# Abrigos e nomes
# ---------------------------------------------------------------------------
SITE_BY_PREFIX = {
    "FG": "FONTE GRANDE",
    "SD": "SEDE",
    "SE": "SERRA",
    "MC": "MORRO DO CÉU",
    "MM": "MORRO DO MORENO",
    "VI": "VIANA",
    "CA": "DOMINGOS MARTINS",
    "PA": "PEDRA AZUL",
}
# ordem de importancia dos abrigos (Fonte Grande tem mapa proprio)
ABRIGOS_ORDER = ["SEDE", "SERRA", "MORRO DO CÉU", "MORRO DO MORENO", "VIANA", "DOMINGOS MARTINS", "PEDRA AZUL"]

# nomes de exibicao (curtos, em maiusculas - aparecem no mapa e nos alertas)
DISPLAY_NAMES = {
    # --- Fonte Grande (SNMP)
    "FG_ROHDE": "TX DIGITAL PRINCIPAL R&S",
    "FG_BIRD_ATLAS": "TX DIGITAL RESERVA ATLAS",
    "FG_M2XA": "EXCITADOR M2X A",
    "FG_M2XB": "EXCITADOR M2X B",
    "FG_ECDI_HARRIS_ATLAS": "ECDI HARRIS ATLAS",
    "FG_GAZETA_FM_PRINCIPAL": "TX GAZETA FM PRINCIPAL",
    "FG_GAZETA_FM_RESERVA": "TX GAZETA FM RESERVA",
    "FG_MIX_FM_PRINCIPAL": "TX MIX FM PRINCIPAL",
    "FG_LITORAL_FM_RESERVA": "TX LITORAL FM RESERVA",
    "FG_CBN": "TX CBN FM",
    "FG_PROCESSADOR_MIXFM": "PROCESSADOR MIX FM",
    "FG_FLEX_01_TX_TV": "FLEX 01 TX TV",
    "FG_FLEX_02_RADIOS": "FLEX 02 RÁDIOS",
    "FG_AVIAT_FGXSEDE_7GHZ": "AVIAT FG x SEDE 7GHz",
    "FG_AVIAT_FGXSERRA": "AVIAT FG x SERRA",
    "FG_AVIAT_FGXCAMPINHO": "AVIAT FG x CAMPINHO",
    "FG_NETVX_FNT2": "NETVX FNT2",
    "FG_UPS1_EATON": "UPS 1 EATON",
    "FG_UPS2_EATON": "UPS 2 EATON",
    # --- Fonte Grande (Modbus)
    "GMG1_SCANIA": "GMG 1 SCANIA",
    "GMG2_CUMMINS": "GMG 2 CUMMINS",
    "FG_SEPAM_S42": "SEPAM S42 MÉDIA TENSÃO",
    "FG_PLC_REFRIGERACAO": "CLP REFRIGERAÇÃO",
    "Medidor QDTVD 1": "MED. QDTVD 1",
    "Medidor QDTVD 2": "MED. QDTVD 2",
    "Medidor QDFM 1": "MED. QDFM 1",
    "Medidor QDFM 2": "MED. QDFM 2",
    "Medidor QDFM 3": "MED. QDFM 3",
    "Medidor Bypass": "MED. QDNB BYPASS",
    "Medidor NB 1": "MED. QDNB NB 1",
    "Medidor NB 2": "MED. QDNB NB 2",
    "Medidor Saída Comutada": "MED. SAÍDA COMUTADA",
    "Medidor Entrada Comum": "MED. ENTRADA COMUM",
    "Medidor Terceiro": "MED. TERCEIRO",
    # --- Sede
    "SD_UPS1_CT01": "UPS 1 CT01",
    "SD_UPS2_CT01": "UPS 2 CT01",
    "SD_CPD_NB_1": "NB 1 CPD",
    "SD_CPD_NB_2": "NB 2 CPD",
    "SD_FLEX_CTRD": "FLEX CTRD ÁUDIO",
    "SD_FLEX_GERADORES": "FLEX GERADORES",
    "SD_SELENIO2": "SELENIO 2",
    "SD_LANTRONIX_XPORT": "LANTRONIX XPORT",
    "SD_AVIAT_SEDEXFG_7GHZ": "AVIAT SEDE x FG 7GHz",
    "SD_AVIAT_SEDEXMORENO": "AVIAT SEDE x MORENO",
    "SD_GMG_CAG": "GMG CAG",
    "SD_GMG_PREDIO": "GMG PRÉDIO",
    "SD_PLC_CAG": "CLP CAG",
    "SD_PLC_BOMBA_RECALQUE": "CLP BOMBA RECALQUE",
    "SD_MULT_SE": "MED. SUBESTAÇÃO",
    "SD_REDACAO_MULT_NORMAL": "MED. QD NORMAL REDAÇÃO",
    "SD_REDACAO_MULT_NB": "MED. QD UPS REDAÇÃO",
    "SD_REDACAO_INV1": "INVERSOR FANCOIL 1",
    "SD_REDACAO_INV2": "INVERSOR FANCOIL 2",
    # --- Morro do Ceu (Guarapari)
    "MC_TRANSMISSOR": "TX M. CÉU",
    "MC_EITV": "EITV M. CÉU",
    "MC_GERADOR": "GMG M. CÉU",
    "MC_NOBREAK_DELTA": "NB DELTA M. CÉU",
    "MC_FLEX": "FLEX M. CÉU",
    # --- Morro do Moreno
    "MM_AVIAT_MORENOXSEDE": "AVIAT MORENO x SEDE",
    "MM_AVIAT_MORENOXVIANA": "AVIAT MORENO x VIANA",
    "MM_NOBREAK_DELTA": "NB DELTA M. MORENO",
    "MM_FLEX": "FLEX M. MORENO",
    # --- Serra
    "SE_TRANSMISSOR": "TX SERRA",
    "SE_EITV": "EITV SERRA",
    "SE_AVIAT_SERRAXFG": "AVIAT SERRA x FG",
    "SE_NOBREAK_DELTA": "NB DELTA SERRA",
    "SE_FLEX": "FLEX SERRA",
    # --- Viana
    "VI_TRANSMISSOR": "TX VIANA",
    "VI_EITV": "EITV VIANA",
    "VI_AVIAT_VIANAXMORENO": "AVIAT VIANA x MORENO",
    "VI_NOBREAK_DELTA": "NB DELTA VIANA",
    "VI_FLEX": "FLEX VIANA",
    # --- Domingos Martins (Campinho)
    "CA_TRANSMISSOR": "TX D. MARTINS",
    "CA_EITV": "EITV D. MARTINS",
    "CA_AVIAT_SEDEXDOMINGOSMARTINS": "AVIAT D. MARTINS x SEDE",
    "CA_AVIAT_KAUTSKYXARACE": "AVIAT KAUTSKY x ARACÊ",
    "CA_NOBREAK_DELTA": "NB DELTA D. MARTINS",
    "CA_FLEX": "FLEX D. MARTINS",
    # --- Pedra Azul
    "PA_TRANSMISSOR": "TX P. AZUL",
    "PA_NOBREAK_DELTA": "NB DELTA P. AZUL",
    "PA_FLEX": "FLEX P. AZUL",
}

# categorias do Mapa FG (mesmo layout em grade do mapa FG original)
FG_CATEGORY = {
    "FG_ROHDE": "TRANSMISSORES", "FG_BIRD_ATLAS": "TRANSMISSORES", "FG_M2XA": "TRANSMISSORES",
    "FG_M2XB": "TRANSMISSORES", "FG_ECDI_HARRIS_ATLAS": "TRANSMISSORES",
    "FG_GAZETA_FM_PRINCIPAL": "RÁDIOS FM", "FG_GAZETA_FM_RESERVA": "RÁDIOS FM",
    "FG_MIX_FM_PRINCIPAL": "RÁDIOS FM", "FG_LITORAL_FM_RESERVA": "RÁDIOS FM",
    "FG_CBN": "RÁDIOS FM", "FG_PROCESSADOR_MIXFM": "RÁDIOS FM",
    "GMG1_SCANIA": "GERADORES", "GMG2_CUMMINS": "GERADORES",
    "FG_SEPAM_S42": "MEDIÇÃO DE ENERGIA",
    "FG_UPS1_EATON": "NOBREAK", "FG_UPS2_EATON": "NOBREAK",
    "FG_AVIAT_FGXSEDE_7GHZ": "ENLACES", "FG_AVIAT_FGXSERRA": "ENLACES",
    "FG_AVIAT_FGXCAMPINHO": "ENLACES", "FG_NETVX_FNT2": "ENLACES",
    "FG_FLEX_01_TX_TV": "INFRAESTRUTURA", "FG_FLEX_02_RADIOS": "INFRAESTRUTURA",
    "FG_PLC_REFRIGERACAO": "INFRAESTRUTURA",
}
FG_CATEGORIES_ORDER = ["TRANSMISSORES", "RÁDIOS FM", "GERADORES", "MEDIÇÃO DE ENERGIA",
                       "NOBREAK", "ENLACES", "INFRAESTRUTURA"]

# os 2 geradores principais ficam no Mapa FG (pedido do Marcos)
FORCE_FG = {"GMG1_SCANIA", "GMG2_CUMMINS"}


def site_for(netx_name, ip=None):
    if netx_name in FORCE_FG or netx_name.startswith("Medidor "):
        return "FONTE GRANDE"   # medidores no gateway 172.10.15.2 (quadros da FG)
    prefix = netx_name.split("_", 1)[0].upper()
    if prefix in SITE_BY_PREFIX:
        return SITE_BY_PREFIX[prefix]
    if ip and ip.startswith("172.10.15."):
        return "FONTE GRANDE"
    return "SEDE"


def stable_id(netx_name):
    """ID fixo por dispositivo (mantem historico/posicao ao reimportar)."""
    return 10_000_000 + zlib.crc32(netx_name.encode("utf-8")) % 890_000_000


def kind_for(netx_name, desc=""):
    n = (netx_name + " " + (desc or "")).upper()
    if re.search(r"UPS|NOBREAK|_NB_|\bNB\b", n) and "MEDIDOR" not in n and "MULT" not in n:
        return "ups"
    if "AVIAT" in n or "NETVX" in n or "LANTRONIX" in n:
        return "link"
    if re.search(r"TRANSMISSOR|ROHDE|M2X|BIRD|ATLAS|_FM_|FG_CBN|ECDI", n):
        return "tx"
    if re.search(r"GMG|GERADOR|MEDIDOR|MULT|SEPAM|INV", n):
        return "power"
    if "FLEX" in n or "EITV" in n or "PLC" in n:
        return "sensor"
    return "other"


# ---------------------------------------------------------------------------
# Regras de apresentacao das medidas
# ---------------------------------------------------------------------------

def clean_label(desc):
    """Encurta a descricao do NETx para caber na caixinha do mapa."""
    d = (desc or "").strip()
    d = re.sub(r"^Instrumenta[cç][aã]o\s*-\s*", "", d, flags=re.I)
    d = re.sub(r"^Status\s*-\s*Opera[cç][aã]o\s*-\s*", "", d, flags=re.I)
    d = re.sub(r"\s+-\s*|\s*-\s+", " - ", d)
    d = re.sub(r"\s+", " ", d)
    d = d.replace("Tensao", "Tensão").replace("Potencia", "Potência")
    d = d.replace("Gerador -Tensão", "Gerador - Tensão")
    for pat in LABEL_PREFIXES:
        d2 = re.sub(pat, "", d, flags=re.I).strip(" -")
        if d2:
            d = d2
    if d.upper() == d:
        d = sentence_case(d)
    d = d.strip(" -")
    for plain, accented in ACCENTS.items():
        d = re.sub(r"\b%s\b" % plain, accented, d)
        d = re.sub(r"\b%s\b" % plain.capitalize(), accented.capitalize(), d)
    return d[:1].upper() + d[1:]


ACCENTS = {
    "potencia": "potência", "tensao": "tensão", "nivel": "nível", "recepcao": "recepção",
    "presenca": "presença", "audio": "áudio", "combustivel": "combustível", "transmissao": "transmissão",
    "reservatorio": "reservatório", "saida": "saída", "disponivel": "disponível", "utilizacao": "utilização",
    "frequencia": "frequência", "intrusao": "intrusão", "pressao": "pressão", "automatico": "automático",
    "termico": "térmico", "necessaria": "necessária", "proporcao": "proporção", "prioritario": "prioritário",
    "maximo": "máximo", "minimo": "mínimo", "histerese": "histerese", "analogico": "analógico",
}


LABEL_PREFIXES = [
    r"^tx\s*dig[^-]*-\s*",
    r"^tx digital (principal|reserva)\s*",
    r"^(gazeta|mix|litoral|cbn) fm( reserva)?\s+",
    r"^m2x excitador [ab]\s+",
    r"^nobreak( [12] cpd)?\s*-?\s*",
]
ACRONYMS = {"TX", "GMG", "XPS", "UR", "CA", "CC", "BER", "CNR", "RF", "SFN", "NB", "FM", "UPS", "VCC",
            "R", "S", "T", "FG", "CBN", "BAT", "AC", "AG", "CAG", "QD", "UAX500"}


def sentence_case(text):
    words = text.split(" ")
    out = []
    for i, w in enumerate(words):
        if w.upper() in ACRONYMS or re.match(r"^[A-Z]\d", w):
            out.append(w.upper())
        else:
            out.append(w.lower())
    res = " ".join(out)
    return res[:1].upper() + res[1:]


BOOL_LABELS = [
    (r"problema|falha|alarme", {"0": "OK", "1": "FALHA"}, (1, 0)),
    (r"t[eé]rmico ok", {"0": "FALHA", "1": "OK"}, (1, 1)),
    (r"health", {"0": "Falha", "1": "OK"}, None),
    (r"fechado para", {"0": "Aberto", "1": "Fechado"}, None),
    (r"dispon[ií]vel", {"0": "Indisponível", "1": "Disponível"}, None),
    (r"remoto", {"0": "Local", "1": "Remoto"}, None),
    (r"autom[aá]tico off", {"0": "Não", "1": "Sim"}, None),
    (r"\bauto\b|autom[aá]tico", {"0": "Manual", "1": "Automático"}, None),
    (r"ligad|\bon\b|\brun\b|status|motor|compressor", {"0": "Desligado", "1": "Ligado"}, None),
]

DSE_CONTROL_MODE = {"0": "Stop", "1": "Auto", "2": "Manual", "3": "Teste c/ carga",
                    "4": "Auto (retorno manual)", "5": "Configuração", "6": "Teste s/ carga", "7": "Off"}
UPS_BAT_STATUS = {"1": "Desconhecido", "2": "Normal", "3": "Baixa", "4": "Esgotada"}
EATON_OUT_SOURCE = {"1": "Outro", "2": "Nenhuma", "3": "Normal", "4": "Bypass", "5": "Bateria",
                    "6": "Booster", "7": "Redutor", "8": "Paralelo", "9": "Paralelo",
                    "10": "Alta eficiência", "11": "Manut. bypass", "12": "ESS"}


def modbus_presentation(dev_netx, dev_desc, label, dtype):
    """Retorna (unit, divisor, value_labels, alarm(compare_method, value)|None)."""
    L = label.lower()
    D = (dev_desc or "").upper()
    is_dse = dev_netx in ("GMG1_SCANIA", "GMG2_CUMMINS", "SD_GMG_CAG", "SD_GMG_PREDIO")
    is_pm1000 = "PM1000" in D
    is_sepam = "SEPAM" in dev_netx

    if dtype == "bool":
        for pat, labels, alarm in BOOL_LABELS:
            if re.search(pat, L):
                return "", None, labels, alarm
        return "", None, {"0": "Não", "1": "Sim"}, None
    if "modo de controle" in L or L == "control mode":
        return "", None, DSE_CONTROL_MODE, None
    if "digital outputs" in L or "status word" in L or "log de falha" in L or "estado dos leds" in L:
        return "", None, None, None
    if "horas" in L or "run time" in L:
        return "h", None, None, None
    if "combust" in L:
        return "L", None, None, None
    if "bateria" in L or "bat voltage" in L:
        return "V", 10 if is_dse else None, None, None
    if "frequ" in L:
        if is_sepam:
            return "Hz", 100, None, None
        if is_dse:
            return "Hz", 10, None, None
        return "Hz", None, None, None
    if "fator de pot" in L:
        return "", 100, None, None
    if "energia" in L or L == "kwh":
        if is_sepam:
            return "kWh", None, None, None
        if L == "kwh":
            return "kWh", None, None, None
        return "kWh", 1000, None, None
    if "reativa" in L or L == "var":
        return "kvar" if is_sepam else "var", None, None, None
    if "aparente" in L or L == "va":
        if is_dse:
            return "kVA", 1000, None, None
        return "kVA" if is_sepam else "VA", None, None, None
    if "potência" in L or "potencia" in L or L == "w":
        if is_dse:
            return "kW", 1000, None, None
        if is_sepam:
            return "kW", None, None, None
        if is_pm1000 or L == "w":
            return "W", None, None, None
        return "kW", None, None, None
    if "velocidade" in L or "rpm" in L:
        return "rpm", None, None, None
    if "speed" in L:
        return "%" if "%" in L else "", None, None, None
    if "nivel" in L or "nível" in L:
        return "%", None, None, None
    if "pressao" in L or "pressão" in L:
        return "bar", 10, None, None
    if "temperatura" in L or re.search(r"(saida|entrada) (ac|ag)", L) or "setpoint - ac" in L:
        return "°C", 10, None, None
    if "umidade" in L:
        return "%", 10, None, None
    if "corrente" in L:
        return "A", 10 if is_dse else None, None, None
    if "tensão" in L or "tensao" in L or "l1-n" in L or "l2-n" in L or "l3-n" in L:
        return "V", 10 if is_dse else None, None, None
    if "quantidade" in L or "contador" in L:
        return "", None, None, None
    return "", None, None, None


def snmp_presentation(netx, desc, oid):
    """(unit, divisor, value_labels, alarm)"""
    d = (desc or "").lower()
    o = oid.strip(".")
    if o.startswith("1.3.6.1.2.1.33.1.2.1"):          # upsBatteryStatus
        return "", None, UPS_BAT_STATUS, (1, 2)
    if o.startswith("1.3.6.1.2.1.33.1.2.3"):          # upsEstimatedMinutesRemaining
        return "min", None, None, None
    if o.startswith("1.3.6.1.2.1.33.1.2.4"):          # upsEstimatedChargeRemaining
        return "%", None, None, None
    if o.startswith("1.3.6.1.2.1.33.1.2.5"):          # upsBatteryVoltage (0,1 V)
        return "V", 10, None, None
    if o.startswith("1.3.6.1.2.1.33.1.3.3.1.3") or o.startswith("1.3.6.1.2.1.33.1.4.4.1.2") \
            or o.startswith("1.3.6.1.2.1.33.1.5.3.1.2"):  # tensoes UPS-MIB (1 V)
        return "V", None, None, None
    if o.startswith("1.3.6.1.2.1.33.1.3.3.1.4") or o.startswith("1.3.6.1.2.1.33.1.4.4.1.3"):
        return "A", 10, None, None                    # correntes UPS-MIB (0,1 A)
    if o.startswith("1.3.6.1.2.1.33.1.3.3.1.5"):
        return "W", None, None, None
    if o.startswith("1.3.6.1.2.1.33.1.4.4.1.5"):
        return "%", None, None, None
    if o.startswith("1.3.6.1.2.1.33.1.6.1"):          # upsAlarmsPresent
        return "", None, None, (1, 0)
    if o.startswith("1.3.6.1.4.1.534.1.2.4"):         # Eaton xupsBatCapacity
        return "%", None, None, None  # (a planilha chama de "minutos", mas o OID e carga %)
    if o.startswith("1.3.6.1.4.1.534.1.4.5"):         # Eaton xupsOutputSource
        return "", None, EATON_OUT_SOURCE, None
    if o.startswith("1.3.6.1.4.1.290.9.2.1.1.4.3"):   # potencia direta/refletida (mW)
        return "W", 1000, None, None
    if o.startswith("1.3.6.1.4.1.290.9.2.1.1") or o.startswith("1.3.6.1.4.1.290.9.2.7.1.5"):
        return "", None, {"1": "FALHA", "2": "OK"}, (1, 2)
    if o.startswith("1.3.6.1.4.1.21678.310.4.1"):     # potencia em dBm x10
        return "dBm", 10, None, None
    if "temperatura" in d:
        return "°C", None, None, None
    if "combust" in d or "diesel" in d or "percentual" in d or "%" in d:
        return "%", None, None, None
    if "minutos" in d:
        return "min", None, None, None
    if "porta" in d or "intrus" in d:
        return "", None, None, None
    if "fase" in d or "tens" in d or "entrada" in d or "saida" in d or "saída" in d or "voltage" in d:
        return "V", None, None, None
    if "corrente" in d:
        return "A", None, None, None
    if "potencia" in d or "potência" in d:
        return "", None, None, None
    if "cnr" in d:
        return "dB", None, None, None
    return "", None, None, None


# medidas que aparecem na caixinha do mapa (as demais ficam no popup/grafico)
VISIBLE_PRIORITY = [
    r"pot[eê]ncia direta|pot.*direta",
    r"refletida",
    r"% de utiliza|utiliza[cç][aã]o",
    r"carga da bateria|% da bateria|percentual.*bateria|bateria restante|minutos de bateria",
    r"status sa[ií]da ups|sa[ií]da r$|tens[aã]o sa[ií]da|^sa[ií]da$",
    r"nivel de recep|recep[cç][aã]o rf",
    r"cnr",
    r"temperatura balanceada - geral",
    r"temperatura balanceada - transmiss",
    r"temperatura balanceada - ups",
    r"compressores ativos",
    r"sala tx - temperatura|temperatura",
    r"n[ií]vel.*(diesel|combust)|diesel",
    r"^pot[eê]ncia ativa$|^w$|potência total ativa",
    r"^tens[aã]o rn$|^tens[aã]o r-n$|mains - l1-n|rede fase r|rede - tens[aã]o r-n",
    r"^corrente r$|corrente 3f",
    r"generator - l1-n|gmg fase r|gerador fase r",
    r"frequ[eê]ncia",
    r"n[ií]vel reservat[oó]rio",
    r"chiller [12] on|bomba [12] - ligada",
    r"velocidade",
    r"bat voltage|tens[aã]o vcc do bateria",
    r"alarme presenca de audio|audio",
    r"porta|intrus",
    r"tens[aã]o de entrada na fase r|tens[aã]o rede fase r|^entrada r$",
]
MAX_VISIBLE = 4


def pick_visible(labels):
    chosen = []
    for pat in VISIBLE_PRIORITY:
        for lb in labels:
            if lb in chosen:
                continue
            if re.search(pat, lb.lower()):
                chosen.append(lb)
                break
        if len(chosen) >= MAX_VISIBLE:
            break
    if not chosen:
        chosen = labels[:3]
    return set(chosen)


def is_write_only(desc, access=None):
    d = (desc or "").lower()
    if "somente escrita" in d or "envia 1" in d or d.startswith("comando") or "netx prote" in d:
        return True
    return False


# ---------------------------------------------------------------------------
# Construcao dos dispositivos
# ---------------------------------------------------------------------------

def build_snmp_devices():
    devs = read_table("xio_SNMP_DeviceDefinitions.xlsx")
    polls = read_table("xio_SNMP_PollingDefinitions.xlsx")
    by_dev = {}
    for p in polls:
        by_dev.setdefault(p.get("SNMP server"), []).append(p)

    result = []
    for d in devs:
        netx = d.get("SNMP server")
        ip = col(d, "Host IP")
        community = col(d, "Community") or "public"
        ver = (col(d, "Version") or "V1").upper()
        version = "2c" if ver.startswith("V2") else "1"
        port = as_int(col(d, "Port"), 161)
        metrics = []
        for p in by_dev.get(netx, []):
            oid = (col(p, "Object ID") or "").strip().strip(".")
            desc = col(p, "Description") or col(p, "Name") or oid
            if not oid:
                continue
            label = clean_label(desc)
            if oid.startswith("1.3.6.1.4.1.534.1.2.4"):
                label = "Carga da bateria"
            unit, divisor, vlabels, alarm = snmp_presentation(netx, desc, oid)
            m = {"label": label, "oid": oid, "unit": unit, "divisor": divisor,
                 "community": community, "version": version, "desc": col(p, "Name") or ""}
            if vlabels:
                m["value_labels"] = vlabels
            if alarm:
                m["compare_method"], m["compare_value"] = alarm
            if is_write_only(desc):
                m["enabled"] = False
            metrics.append(m)
        result.append({
            "netx": netx, "ip": ip, "protocol": "snmp", "snmp_port": port,
            "desc": "", "metrics": metrics,
        })
    return result


def norm_rtype(t):
    t = (t or "holding").strip().lower()
    for k in ("coil", "discrete", "input", "holding"):
        if t.startswith(k):
            return k
    return "holding"


def build_modbus_devices():
    devs = read_table("xio_Modbus_DeviceDefinitions.xlsx")
    points = read_table("xio_Modbus_DatapointDefinitions.xlsx")
    by_dev = {}
    for p in points:
        by_dev.setdefault(p.get("Device name"), []).append(p)

    result = []
    for d in devs:
        netx = d.get("Device name")
        ip = col(d, "IP address")
        desc = col(d, "Description") or ""
        ws = flag(col(d, "Wordswap"))
        dws = flag(col(d, "DWordswap"))
        cfg = {
            "port": as_int(col(d, "Port"), 502),
            "unit_id": as_int(col(d, "Slave ID"), 1),
            "word_swap": True if ws is None else ws,
            "dword_swap": True if dws is None else dws,
            "timeout_ms": min(8000, as_int(col(d, "Req. timeout"), 3000) or 3000),
            "description": desc,
        }
        metrics = []
        for p in by_dev.get(netx, []):
            rdesc = col(p, "Description") or ""
            label = clean_label(rdesc)
            dtype = (col(p, "Data type") or "uint16").strip().lower()
            unit, divisor, vlabels, alarm = modbus_presentation(netx, desc, label, dtype)
            m = {
                "label": label,
                "source": "modbus",
                "register_type": norm_rtype(col(p, "Modbus DP type")),
                "address": as_int(col(p, "Address"), 0),
                "data_type": dtype,
                "unit": unit,
                "divisor": divisor,
            }
            pws = flag(col(p, "Wordswap"))
            pdws = flag(col(p, "DWordswap"))
            if pws is not None and pws != cfg["word_swap"]:
                m["word_swap"] = pws
            if pdws is not None and pdws != cfg["dword_swap"]:
                m["dword_swap"] = pdws
            if vlabels:
                m["value_labels"] = vlabels
            if alarm:
                m["compare_method"], m["compare_value"] = alarm
            if is_write_only(rdesc) or m["address"] >= 32000 and "escrita" in rdesc.lower():
                m["enabled"] = False
            metrics.append(m)
        result.append({"netx": netx, "ip": ip, "protocol": "modbus", "modbus": cfg,
                       "desc": desc, "metrics": metrics})
    return result


def finalize_device(raw):
    netx = raw["netx"]
    site = site_for(netx, raw["ip"])
    # rotulos unicos dentro do dispositivo
    seen = {}
    for m in raw["metrics"]:
        base = m["label"]
        if base in seen:
            seen[base] += 1
            m["label"] = f"{base} ({seen[base]})"
        else:
            seen[base] = 1
    visible = pick_visible([m["label"] for m in raw["metrics"] if m.get("enabled", True) is not False])
    for m in raw["metrics"]:
        if m["label"] not in visible:
            m["hide_on_map"] = True
        if not m.get("desc"):
            m.pop("desc", None)
    dev = {
        "id": stable_id(netx),
        "netx_id": netx,
        "site": site,
        "name": DISPLAY_NAMES.get(netx, netx.replace("_", " ")),
        "ip": raw["ip"],
        "kind": kind_for(netx, raw.get("desc")),
        "metrics": raw["metrics"],
        "enabled": True,
        "alert_enabled": True,
        "sound_enabled": True,
    }
    if raw["protocol"] == "modbus":
        dev["protocol"] = "modbus"
        dev["modbus"] = raw["modbus"]
        if raw.get("desc"):
            dev["description"] = raw["desc"]
        dev["poll_interval"] = 15 if netx in FORCE_FG else 30
    if site == "FONTE GRANDE":
        cat = FG_CATEGORY.get(netx)
        if cat is None:
            cat = "MEDIÇÃO DE ENERGIA" if netx.startswith("Medidor ") else "INFRAESTRUTURA"
        dev["category"] = cat
        dev["site"] = cat
    return dev


# ---------------------------------------------------------------------------
# Preserva ajustes feitos pelo usuario (reimportacao)
# ---------------------------------------------------------------------------
KEEP_DEVICE_FIELDS = ("enabled", "alert_enabled", "sound_enabled", "ping_enabled", "report_enabled",
                      "poll_interval", "name")
KEEP_METRIC_FIELDS = ("compare_method", "compare_value", "hide_on_map", "enabled", "divisor", "unit",
                      "value_labels", "word_swap", "dword_swap")


def merge_previous(devices, previous):
    prev_by_id = {d.get("id"): d for d in (previous or {}).get("devices", [])}
    for dev in devices:
        old = prev_by_id.get(dev["id"])
        if not old:
            continue
        for f in KEEP_DEVICE_FIELDS:
            if f in old:
                dev[f] = old[f]
        old_metrics = {m.get("label"): m for m in old.get("metrics", [])}
        for m in dev["metrics"]:
            om = old_metrics.get(m["label"])
            if not om:
                continue
            for f in KEEP_METRIC_FIELDS:
                if f in om:
                    m[f] = om[f]
                elif f in m and f in ("compare_method", "compare_value", "hide_on_map"):
                    m.pop(f, None)
        # metricas adicionadas a mao (fora da planilha) continuam
        known = {m["label"] for m in dev["metrics"]}
        for label, om in old_metrics.items():
            if label not in known and om.get("_manual"):
                dev["metrics"].append(om)
        if old.get("modbus") and dev.get("modbus"):
            for f in ("word_swap", "dword_swap", "timeout_ms"):
                if f in old["modbus"]:
                    dev["modbus"][f] = old["modbus"][f]


def load_json(name, default=None):
    try:
        with open(os.path.join(BASE_DIR, name), encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def save_json(name, data):
    path = os.path.join(BASE_DIR, name)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# Layout inicial do Mapa Abrigos (colunas por abrigo, arrastavel depois)
# ---------------------------------------------------------------------------
BOX_W = 196
COL_PITCH = 204
LABEL_Y = 26
TOP_Y = 84
MAX_COL_H = 1000


def est_height(dev):
    """Altura aproximada da caixinha (196 px de largura, fonte 13/12,5 px):
    cada linha quebra a cada ~24 caracteres."""
    import math
    name_lines = max(1, math.ceil(len(dev["name"]) / 20))
    lines = 0
    for m in dev["metrics"]:
        if m.get("hide_on_map") or m.get("enabled", True) is False:
            continue
        lines += max(1, math.ceil((len(m["label"]) + 12) / 24))
    lines = max(1, lines)
    return 18 + 21 * name_lines + 20 * lines


def build_interior_layout(sites, previous_layout=None, reset=False):
    prev_devs = {} if reset or not previous_layout else previous_layout.get("devices", {})
    devices_pos = {}
    labels = []
    x = 22
    for site in ABRIGOS_ORDER:
        devs = sites.get(site) or []
        if not devs:
            continue
        # quantas colunas esse abrigo precisa
        total = sum(est_height(d) + 12 for d in devs)
        ncols = max(1, -(-total // MAX_COL_H))
        per_col_target = total / ncols
        col_i, y, acc = 0, TOP_Y, 0
        for d in devs:
            h = est_height(d) + 12
            if acc > 0 and acc + h > per_col_target + 40 and col_i < ncols - 1:
                col_i += 1
                y, acc = TOP_Y, 0
            pos = {"x": x + col_i * COL_PITCH, "y": y, "scale": 1}
            key = str(d["id"])
            devices_pos[key] = prev_devs.get(key, pos)
            y += h
            acc += h
        width = ncols * COL_PITCH - (COL_PITCH - BOX_W)
        label_w = 12 + len(site) * 8.2
        labels.append({"id": "auto_" + re.sub(r"[^A-Z0-9]+", "_", site.upper()), "text": site,
                       "x": round(x + width / 2 - label_w / 2), "y": LABEL_Y, "scale": 1})
        x += ncols * COL_PITCH + 12
    if previous_layout and not reset:
        old_labels = {l.get("id"): l for l in previous_layout.get("labels", [])}
        labels = [old_labels.get(l["id"], l) for l in labels] + \
                 [l for lid, l in old_labels.items() if lid not in {x["id"] for x in labels}]
    return {
        "devices": devices_pos,
        "labels": labels,
        "edges": (previous_layout or {}).get("edges", []) if not reset else [],
        "colors": {"online": "#2ecc55", "offline": "#e5384b", "unknown": "#4a5570"},
    }


def main():
    reset_layout = "--reset-layout" in sys.argv
    raw = build_snmp_devices() + build_modbus_devices()
    devices = [finalize_device(r) for r in raw]

    fg_devs = [d for d in devices if d.get("category")]
    ab_devs = [d for d in devices if not d.get("category")]

    prev_ab = load_json("network.json", {})
    prev_fg = load_json("network_fg.json", {})
    merge_previous(ab_devs, prev_ab)
    merge_previous(fg_devs, prev_fg)

    # ---- Mapa Abrigos (formato "sites")
    kind_order = {"tx": 0, "sensor": 1, "power": 2, "ups": 3, "link": 4, "other": 5}
    sites = {s: [] for s in ABRIGOS_ORDER}
    for d in sorted(ab_devs, key=lambda d: (kind_order.get(d["kind"], 9), d["name"])):
        sites.setdefault(d["site"], []).append(d)
    sites = {k: v for k, v in sites.items() if v}
    sites_order = [s for s in ABRIGOS_ORDER if s in sites] + [s for s in sites if s not in ABRIGOS_ORDER]
    anchor = {}
    for s, devs in sites.items():
        link = next((d for d in devs if d["kind"] == "link"), devs[0])
        anchor[s] = link["id"]
    network = {
        "map_name": "Mapa Abrigos",
        "sites_order": sites_order,
        "edges": [],
        "devices": [d for s in sites_order for d in sites[s]],
        "sites": sites,
        "anchor_id": anchor,
    }

    # ---- Mapa FG (formato "categorias")
    cats = {c: [] for c in FG_CATEGORIES_ORDER}
    order_hint = list(DISPLAY_NAMES.keys())
    for d in sorted(fg_devs, key=lambda d: order_hint.index(d["netx_id"]) if d["netx_id"] in order_hint else 999):
        cats.setdefault(d["category"], []).append(d)
    # respeita a ordem manual (arrastar no mapa) que ja estava salva
    for c, devs in cats.items():
        prev_order = [x.get("id") for x in (prev_fg.get("categories", {}) or {}).get(c, [])]
        if prev_order:
            devs.sort(key=lambda d: prev_order.index(d["id"]) if d["id"] in prev_order else 999)
    network_fg = {
        "map_name": "Mapa FG",
        "categories_order": [c for c in FG_CATEGORIES_ORDER if cats.get(c)],
        "devices": [d for c in FG_CATEGORIES_ORDER for d in cats.get(c, [])],
        "categories": {c: v for c, v in cats.items() if v},
    }

    save_json("network.json", network)
    save_json("network_fg.json", network_fg)
    layout = build_interior_layout(sites, load_json("layout_interior.json"), reset=reset_layout)
    save_json("layout_interior.json", layout)

    n_metrics = sum(len(d["metrics"]) for d in devices)
    n_mb = sum(1 for d in devices if d.get("protocol") == "modbus")
    print(f"OK - {len(devices)} dispositivos ({n_mb} Modbus, {len(devices) - n_mb} SNMP), {n_metrics} medidas")
    print(f"     Mapa FG: {len(network_fg['devices'])} dispositivos em {len(network_fg['categories_order'])} categorias")
    print(f"     Mapa Abrigos: {len(network['devices'])} dispositivos em {len(sites_order)} abrigos")
    dup = {}
    for d in devices:
        dup.setdefault(d["ip"], []).append(d["netx_id"])
    for ip, names in dup.items():
        if len(names) > 1 and not all(n.startswith("Medidor") or n.startswith("SD_REDACAO") for n in names):
            print(f"     atencao: IP {ip} repetido em {', '.join(names)} (confira na planilha)")


if __name__ == "__main__":
    main()
