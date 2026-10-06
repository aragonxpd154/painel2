#!/usr/bin/env python3
"""
Importador KNX (NETx/ETS) -> knx_fg.json  - Alarme de Incendio da Fonte Grande
=============================================================================
Le, da pasta planilhas/, as exportacoes do NETx:
    nxaTelegramDefinitions_40.xlsx   -> enderecos de grupo (GA), tipo (DPT), descricao
    nxaGroupAliases_40.xlsx          -> nome dos grupos (MT/S - 01, QTA 1 ...)
    nxaGatewayDefinitions_40.xlsx    -> IP da interface KNX
    nxaKNXObjectDefinitions.xlsx     -> (opcional) quais objetos aceitam leitura
e gera knx_fg.json so com a interface "IPS/S3.1.1 IP Interface MDRC"
(192.168.205.161) - a do alarme de incendio, intrusao e presenca.

Rodar de novo NAO perde o que voce ajustou na tela (nome, inverter,
alerta no Telegram) - os ajustes ficam guardados por endereco de grupo.

Uso:  python importar_knx.py
"""
import json
import os
import re

from importar_planilhas import read_xlsx_rows, PLANILHAS_DIR, BASE_DIR

GATEWAY_NAME = "IPS/S3.1.1 IP Interface MDRC"
OUT = os.path.join(BASE_DIR, "knx_fg.json")


def table(name):
    path = os.path.join(PLANILHAS_DIR, name)
    if not os.path.exists(path):
        return []
    rows = read_xlsx_rows(path)
    header = [(h or "").strip() for h in rows[0]]
    out = []
    for r in rows[1:]:
        first = (str(r[0]).strip() if r and r[0] is not None else "")
        if not first or first.startswith("'"):
            continue
        out.append({header[i]: (r[i].strip() if isinstance(r[i], str) else r[i]) if i < len(r) else None
                    for i in range(len(header))})
    return out


def col(item, prefix):
    for k, v in item.items():
        if k.lower().startswith(prefix.lower()):
            return v
    return None


def classify(desc, ga):
    """tipo do ponto + se e um comando (escrita) que o painel NAO deve mandar sozinho."""
    d = desc.lower()
    main = ga.split("/")[0]
    if main == "1":
        return "temperatura"
    if main == "2":
        return "iluminacao"
    if main == "3":
        return "disjuntor"
    if re.match(r"^(reset|desativar|on/off)", d):
        return "comando"
    if d.startswith("request status"):
        return "solicitar_status"
    if d.startswith("alarm memory"):
        return "memoria"
    if "alimenta" in d:
        return "alimentacao"
    if d.startswith("status do alarme de intrus"):
        return "intrusao_armado"
    if d.startswith("alarme de intrus") or "alarme interno disparado" in d:
        return "intrusao_alarme"
    if "alarme geral" in d:
        return "alarme_geral"
    if "premium" in d:
        return "intrusao_sensor"     # detector premium (movimento/intrusao)
    if re.match(r"^dp-", d):
        return "presenca"            # detector de presenca 6131 (sala)
    if "incendio" in d or "incêndio" in d:
        return "incendio"
    if re.search(r"\b(df\d?|dc|a/d)\b|df\d?-|dc-|a/d-", d):
        return "incendio"            # DF = det. fumaca, DC = det. calor, A/D = acionador
    if re.search(r"\bse-\d", d):
        return "intrusao_sensor"     # SE = sensor de abertura/movimento (zona desativavel)
    return "outro"


# o que dispara alerta no Telegram (por padrao)
ALERTA_PADRAO = {"incendio", "intrusao_alarme", "alimentacao"}
NOMES_GRUPO_PADRAO = {"0": "Alarme Geral", "1": "Temperatura", "2": "Iluminação Externa", "3": "Status dos Disjuntores"}


def main():
    gw_ip, gw_port = "192.168.205.161", 3671
    for g in table("nxaGatewayDefinitions_40.xlsx"):
        if col(g, "Name") == GATEWAY_NAME:
            gw_ip = col(g, "KNX gateway IP") or gw_ip
            gw_port = int(col(g, "Port") or 3671)

    aliases = {}
    for a in table("nxaGroupAliases_40.xlsx"):
        if col(a, "KNX gateway") != GATEWAY_NAME:
            continue
        main_g = str(col(a, "Main grp") or "").strip()
        mid = str(col(a, "Middle grp") or "").strip()
        aliases[(main_g, mid)] = col(a, "Name")

    readable = {}
    for o in table("nxaKNXObjectDefinitions.xlsx"):
        gas = str(col(o, "Group addresses") or "")
        rflag = str(col(o, "Read flag") or "").strip().upper().startswith("T")
        for ga, gw in re.findall(r"(\d+/\d+/\d+)@([^;,]+)", gas):
            if gw.strip() == GATEWAY_NAME:
                readable[ga] = readable.get(ga, False) or rflag

    previous = {}
    if os.path.exists(OUT):
        with open(OUT, encoding="utf-8") as f:
            for p in json.load(f).get("points", []):
                previous[p["ga"]] = p

    points = []
    for t in table("nxaTelegramDefinitions_40.xlsx"):
        if col(t, "KNX gateway") != GATEWAY_NAME:
            continue
        ga = str(col(t, "KNX grp. adr") or "").strip()
        if not re.match(r"^\d+/\d+/\d+$", ga):
            continue
        desc = re.sub(r"\s+", " ", str(col(t, "Description") or ga)).strip()
        m, mid, _ = ga.split("/")
        tipo = classify(desc, ga)
        p = {
            "ga": ga,
            "nome": desc,
            "nome_original": desc,
            "grupo": aliases.get((m, mid)) or aliases.get((m, "")) or NOMES_GRUPO_PADRAO.get(m, "Grupo " + m),
            "secao": aliases.get((m, "")) or NOMES_GRUPO_PADRAO.get(m, ""),
            "tipo": tipo,
            "dpt": str(col(t, "Data Type") or "DPT_1.001").replace("DPT_", ""),
            # le na partida se o ETS diz que aceita leitura OU se o NETx ja le na reconexao
            "legivel": readable.get(ga, False) or str(col(t, "Read on reconnect") or "").upper().startswith("T"),
            "alarme_valor": 0 if tipo == "alimentacao" else 1,   # 12 V: 1 = em operacao
            "alerta_telegram": tipo in ALERTA_PADRAO,
        }
        old = previous.get(ga)
        if old:   # preserva o que foi ajustado na tela
            for k in ("nome", "alarme_valor", "alerta_telegram", "oculto", "editado"):
                if k in old:
                    p[k] = old[k]
        points.append(p)

    def ga_key(p):
        return tuple(int(x) for x in p["ga"].split("/"))
    points.sort(key=ga_key)
    data = {
        "versao": 2,
        "gateway": gw_ip,
        "port": gw_port,
        "enabled": True,
        "nome_interface": GATEWAY_NAME,
        "points": points,
        "migr_alim_12v": True,
    }
    tmp = OUT + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, OUT)
    from collections import Counter
    c = Counter(p["tipo"] for p in points)
    print(f"OK - {len(points)} pontos KNX da interface {gw_ip}:{gw_port}")
    print("    " + ", ".join(f"{k}: {v}" for k, v in sorted(c.items())))


if __name__ == "__main__":
    main()
