#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

from PyQt6.QtWidgets import QApplication

from app import MainWindow


def main():
    parser = argparse.ArgumentParser(description="Explore an allmydox database visually.")
    parser.add_argument("db", nargs="?", default="allmydox.db",
                        help="Path to the allmydox SQLite database (default: allmydox.db)")
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    if not db_path.exists():
        print(f"Error: database file not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    app = QApplication(sys.argv)
    app.setApplicationName("findethedox")
    window = MainWindow(str(db_path))
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
