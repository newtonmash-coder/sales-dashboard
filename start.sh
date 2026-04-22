#!/bin/bash
# ==========================================================
# start.sh — Render start command
# 1. Downloads all Drive files to disk (preload.py)
# 2. Starts Streamlit (reads from disk, not Drive)
# ==========================================================

echo ">>> Running preload..."
python preload.py

echo ">>> Starting Streamlit..."
streamlit run app.py \
  --server.port $PORT \
  --server.address 0.0.0.0 \
  --server.headless true \
  --server.enableCORS false \
  --server.enableXsrfProtection false \
  --server.enableWebsocketCompression false
