#!/bin/bash
# ============================================================================
# Configura no Debian os IPs extras que o Sistema Painel precisa para falar
# DIRETO com os equipamentos (mesmo esquema do PC Windows: uma placa, varios
# IPs, um em cada rede).
#
#   sudo bash linux/configurar_rede.sh            (aplica)
#   sudo bash linux/configurar_rede.sh --testar   (so testa, nao muda nada)
#
# - mantem a conexao atual (ex: 192.168.1.102 por DHCP, internet/Telegram)
# - NAO cria outro gateway (so IPs /24, falando direto com cada rede)
# - confere cada IP com arping antes; se ja tiver dono, tenta o proximo numero
#   (foi um IP repetido que travou os geradores no Windows)
# - usa NetworkManager se ele estiver ativo; senao cria o servico painel-ips
# ============================================================================

# rede : IP preferido (se estiver ocupado, tenta +1, +2 ... ate +15)
REDES=(
  "192.168.200.230/24"   # Sede / enlaces / nobreaks / TX (maioria dos equipamentos)
  "192.168.205.246/24"   # geradores GMG 1 e GMG 2, TX FM reserva
  "172.10.15.230/24"     # gateway dos medidores, SEPAM, CLP refrigeracao
  "192.168.101.230/24"   # NETVX FNT2
)
# um equipamento de cada rede para o teste final (ping e porta)
TESTES=(
  "192.168.200.9"
  "192.168.205.2:502"
  "172.10.15.2:502"
  "192.168.101.20"
)

set -u
SO_TESTAR=0; [ "${1:-}" = "--testar" ] && SO_TESTAR=1
if [ "$(id -u)" != "0" ]; then echo "Rode com sudo: sudo bash $0"; exit 1; fi

command -v arping >/dev/null || apt-get install -y -qq iputils-arping >/dev/null
IFACE=$(ip route show default 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="dev"){print $(i+1); exit}}')
[ -z "$IFACE" ] && IFACE=$(ip -o link show | awk -F': ' '$2!="lo"{print $2; exit}')
echo ">> Placa de rede: $IFACE"
ip -br -4 addr show dev "$IFACE"

IPBIN=$(command -v ip)
livre()  { arping -D -q -c 2 -w 3 -I "$IFACE" "$1" >/dev/null 2>&1; }

APLICAR=()
if [ $SO_TESTAR -eq 0 ]; then
  for alvo in "${REDES[@]}"; do
    ipbase=${alvo%/*}; pref=${alvo#*/}
    rede=${ipbase%.*}; n=${ipbase##*.}
    # ja existe um IP desta rede na placa? entao nao mexe
    atual=$(ip -4 -o addr show dev "$IFACE" | awk '{print $4}' | grep "^${rede//./\\.}\." | head -1)
    if [ -n "$atual" ]; then echo "   $rede.x ja configurado ($atual)"; continue; fi
    escolhido=""
    for k in $(seq 0 15); do
      cand="$rede.$((n + k))"
      if livre "$cand"; then escolhido="$cand"; break; fi
      echo "   $cand OCUPADO (outro equipamento respondeu) - tentando o proximo"
    done
    if [ -z "$escolhido" ]; then echo "   !! nenhum IP livre encontrado em $rede.x - pulei"; continue; fi
    echo "   $rede.x -> $escolhido/$pref (livre)"
    APLICAR+=("$escolhido/$pref")
  done

  if [ ${#APLICAR[@]} -gt 0 ]; then
    if systemctl is-active --quiet NetworkManager && nmcli -t -f DEVICE,STATE dev 2>/dev/null | grep -q "^$IFACE:connected"; then
      CON=$(nmcli -g GENERAL.CONNECTION dev show "$IFACE")
      echo ">> Gravando no NetworkManager (conexao \"$CON\")"
      for a in "${APLICAR[@]}"; do nmcli con mod "$CON" +ipv4.addresses "$a"; done
      nmcli con up "$CON" >/dev/null
    else
      echo ">> Gravando no servico painel-ips (sem NetworkManager)"
      # mantem os IPs que ja estavam no servico (rodar de novo nao perde nada)
      if [ -f /etc/systemd/system/painel-ips.service ]; then
        while read -r antigo; do
          [[ " ${APLICAR[*]} " == *" $antigo "* ]] || APLICAR+=("$antigo")
        done < <(grep -o 'addr add [0-9./]*' /etc/systemd/system/painel-ips.service | awk '{print $3}')
      fi
      {
        echo "[Unit]"
        echo "Description=IPs extras do Sistema Painel (redes dos equipamentos)"
        echo "After=network-online.target"
        echo "Wants=network-online.target"
        echo "Before=painel.service"
        echo "[Service]"
        echo "Type=oneshot"
        echo "RemainAfterExit=yes"
        for a in "${APLICAR[@]}"; do echo "ExecStart=-$IPBIN addr add $a dev $IFACE"; done
        for a in "${APLICAR[@]}"; do echo "ExecStop=-$IPBIN addr del $a dev $IFACE"; done
        echo "[Install]"
        echo "WantedBy=multi-user.target"
      } > /etc/systemd/system/painel-ips.service
      systemctl daemon-reload
      systemctl enable painel-ips >/dev/null
      for a in "${APLICAR[@]}"; do ip addr add "$a" dev "$IFACE" 2>/dev/null; done
    fi
    sleep 3
  fi
fi

echo
echo ">> IPs na placa agora:"
ip -br -4 addr show dev "$IFACE"
echo
echo ">> Teste de caminho (src tem que ser o IP da MESMA rede do equipamento)"
for t in "${TESTES[@]}"; do
  host=${t%%:*}; porta=""; [ "$t" != "$host" ] && porta=${t##*:}
  src=$(ip route get "$host" 2>/dev/null | grep -o 'src [0-9.]*' | awk '{print $2}')
  via=$(ip route get "$host" 2>/dev/null | grep -o 'via [0-9.]*' | awk '{print $2}')
  if ping -c 2 -W 1 "$host" >/dev/null 2>&1; then p="ping OK"; else p="ping SEM resposta"; fi
  linha="   $host  src=$src ${via:+via=$via (ROTEADOR!)}  $p"
  if [ -n "$porta" ]; then
    if timeout 3 bash -c "</dev/tcp/$host/$porta" 2>/dev/null; then linha="$linha  porta $porta OK"
    else linha="$linha  porta $porta FALHOU"; fi
  fi
  echo "$linha"
done
echo
echo "Se tudo deu OK:  sudo systemctl restart painel"
echo "Se 'ping SEM resposta' em uma rede inteira: este PC nao esta no mesmo switch/VLAN"
echo "dessa rede - ligue-o na mesma rede fisica do servidor NETx / PC Windows."
