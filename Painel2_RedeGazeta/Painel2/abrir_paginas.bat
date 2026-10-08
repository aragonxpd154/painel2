@echo off
REM Abre as paginas do Sistema Painel depois do logon (espera o server.py subir).
ping -n 41 127.0.0.1 >nul
REM Se usar Chrome no lugar do Edge, troque "msedge" por "chrome".
start msedge "http://127.0.0.1/mapa-fg" "http://127.0.0.1/mapa-interior" "http://127.0.0.1/energia"
