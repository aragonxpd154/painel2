@echo off
REM Inicia o Sistema Painel (server.py) - Rede Gazeta - e reinicia sozinho
REM se ele cair. Deixe este arquivo NA MESMA PASTA do server.py.
REM Saida (endereco de acesso e erros) fica em painel_log.txt.

cd /d "%~dp0"

REM espera a rede subir no boot do Windows (ping funciona sem sessao interativa)
ping -n 21 127.0.0.1 >nul

REM procura o Python: 1) lancador "py"  2) python no PATH
set PY=
where py >nul 2>&1 && set PY=py -3
if "%PY%"=="" (where python >nul 2>&1 && set PY=python)
if "%PY%"=="" (
  echo Python nao encontrado. Instale o Python 3 em https://www.python.org >> painel_log.txt
  exit /b 1
)

:loop
echo. >> painel_log.txt
echo ===== iniciado em %date% %time% ===== >> painel_log.txt
%PY% server.py 80 >> painel_log.txt 2>&1
echo ===== encerrado em %date% %time% - reiniciando em 10s ===== >> painel_log.txt
ping -n 11 127.0.0.1 >nul
goto loop
