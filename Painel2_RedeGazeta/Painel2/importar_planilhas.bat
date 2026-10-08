@echo off
REM Reimporta as planilhas xio do NETx (pasta planilhas\) sem perder os ajustes.
cd /d "%~dp0"
where py >nul 2>&1 && (py -3 importar_planilhas.py) || (python importar_planilhas.py)
echo.
echo Reinicie o server.py para carregar a nova lista de dispositivos.
pause
