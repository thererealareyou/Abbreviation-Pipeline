@echo off
title Llama.cpp Local Server
echo ===================================================
echo Launching Llama.cpp Local Server...
echo ===================================================

C:\llama-server\llama-server.exe -m "C:\Users\thererealareyou\.cache\huggingface\hub\models--aovchinnikov--T-lite-it-1.0-Q4_K_M-GGUF\snapshots\f6c77ca02a2d31ff615b42a86f6af40ebd7cf330\t-lite-it-1.0-q4_k_m.gguf" -c 8192 -np 8 -b 512 -ub 512 -ngl 999 -fa on -ctk q8_0 -ctv q8_0 --temp 0.0 --port 8000

pause
