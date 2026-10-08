#!/bin/bash
# Restaura um backup do Sistema Painel (dados: dispositivos, agenda, usuarios,
# historico, alertas, KNX, planilhas, fotos). O codigo nao e alterado.
#   sudo bash /opt/painel/linux/restaurar_backup.sh /opt/painel/backups/diarios/painel_2026-10-08_0300.tar.gz
set -e
ARQ="$1"; DEST=/opt/painel
if [ "$(id -u)" != "0" ]; then echo "Rode com sudo"; exit 1; fi
if [ ! -f "$ARQ" ]; then echo "Backup nao encontrado: $ARQ"; ls -1t $DEST/backups/diarios/ 2>/dev/null | head; exit 1; fi
echo ">> Parando o painel"; systemctl stop painel
SEG="$DEST/backups/antes_restaurar_$(date +%Y%m%d_%H%M%S).tar.gz"
echo ">> Guardando os dados atuais em $SEG (caso precise voltar)"
(cd "$DEST" && tar czf "$SEG" --exclude=backups --exclude=tile_cache *.json *.log planilhas arquivos_upload site_photos inventario_pdfs 2>/dev/null || true)
echo ">> Restaurando $ARQ"; tar xzf "$ARQ" -C "$DEST"
chown -R painel:painel "$DEST"
echo ">> Iniciando o painel"; systemctl start painel; sleep 2; systemctl --no-pager --lines=3 status painel || true
echo "Pronto."
