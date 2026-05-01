@echo off
echo === findethedox setup (Windows) ===

python -m pip install --upgrade pip
python -m pip install -r requirements.txt

echo.
echo Verifying...
python -c "import PyQt6, wordcloud, fitz, matplotlib; print('All dependencies OK')"

echo.
echo Setup complete. Run with:
echo   python main.py C:\path\to\allmydox.db
pause
