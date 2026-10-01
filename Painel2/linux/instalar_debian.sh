#!/bin/bash
# ============================================================================
# Instalador do Sistema Painel - Rede Gazeta no Debian 11/12 (ou Ubuntu)
#
#   sudo bash linux/instalar_debian.sh
#
# - instala o que falta (python3, ping, arping, reportlab para o PDF)
# - copia o projeto para /opt/painel (usuario de sistema "painel")
# - cria o servico "painel" (sobe sozinho no boot e reinicia se cair)
#
# Rodar de novo = ATUALIZAR: copia so o codigo (server.py, paginas, static)
# e PRESERVA os dados (dispositivos, historico, incidentes, agenda, Telegram).
# ============================================================================
set -e
if [ "$(id -u)" != "0" ]; then echo "Rode com sudo: sudo bash $0"; exit 1; fi

ORIGEM="$(cd "$(dirname "$0")/.." && pwd)"
DEST=/opt/painel

echo ">> Pacotes"
apt-get update -qq
apt-get install -y -qq python3 iputils-ping iputils-arping iproute2 python3-reportlab >/dev/null

echo ">> Fuso horario (resumos de domingo/agenda no horario de Brasilia)"
timedatectl set-timezone America/Sao_Paulo 2>/dev/null || true

id painel >/dev/null 2>&1 || useradd --system --home-dir "$DEST" --shell /usr/sbin/nologin painel

if [ ! -f "$DEST/server.py" ]; then
  echo ">> Primeira instalacao: copiando o projeto inteiro para $DEST"
  mkdir -p "$DEST"
  cp -a "$ORIGEM"/. "$DEST"/
else
  echo ">> Atualizacao: copiando so o codigo (dados preservados)"
  cp -a "$ORIGEM"/*.py "$ORIGEM"/*.html "$DEST"/
  cp -a "$ORIGEM"/static "$ORIGEM"/linux "$DEST"/
  [ -f "$ORIGEM/LEIA-ME.txt" ] && cp -a "$ORIGEM/LEIA-ME.txt" "$DEST"/
fi
rm -f "$DEST"/*.bat
mkdir -p "$DEST/arquivos_upload" "$DEST/site_photos" "$DEST/inventario_pdfs" "$DEST/planilhas"
chown -R painel:painel "$DEST"

echo ">> Servico systemd"
cp "$DEST/linux/painel.service" /etc/systemd/system/painel.service
systemctl daemon-reload
systemctl enable painel >/dev/null
systemctl restart painel
sleep 3
systemctl --no-pager --lines=5 status painel || true

IP=$(hostname -I | awk '{print $1}')
echo
echo "Pronto. Acesse: http://$IP/   (login padrao adminfg / @fg123)"
echo "Logs ao vivo:   journalctl -u painel -f"
echo "Reiniciar:      sudo systemctl restart painel"
echo "Reimportar planilhas: cd $DEST && sudo -u painel python3 importar_planilhas.py && sudo systemctl restart painel"
