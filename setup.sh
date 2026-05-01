#!/usr/bin/env bash
set -euo pipefail

echo "=== findethedox setup (Linux) ==="

if ! python3 -m pip --version &>/dev/null; then
    echo "Installing pip..."
    TMP_PIP="$(mktemp)"
    curl -sSf https://bootstrap.pypa.io/get-pip.py -o "$TMP_PIP"
    python3 "$TMP_PIP" --user --break-system-packages
    rm "$TMP_PIP"
fi

PIP="$(python3 -c 'import site; print(site.getuserbase())')/bin/pip"

echo "Installing dependencies..."
"$PIP" install --break-system-packages -q -r requirements.txt

echo "Verifying..."
python3 - <<'EOF'
from PyQt6.QtCore import PYQT_VERSION_STR
import wordcloud, fitz, matplotlib
print(f"  PyQt6        {PYQT_VERSION_STR}")
print(f"  wordcloud    {wordcloud.__version__}")
print(f"  pymupdf      {fitz.__version__}")
print(f"  matplotlib   {matplotlib.__version__}")
EOF

echo ""
echo "Setup complete. Run with:"
echo "  python3 main.py /home/mint/allmydox.db"
