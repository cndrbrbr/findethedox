#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

from PyQt6.QtWidgets import QApplication, QFileDialog

from app import MainWindow


def main():
    parser = argparse.ArgumentParser(description="Explore an allmydox database visually.")
    parser.add_argument("db", nargs="?", default=None,
                        help="Path to the allmydox SQLite database (default: browse interactively)")
    parser.add_argument("--docs", default=None, metavar="FOLDER",
                        help="Root folder for documents referenced in the database")
    args = parser.parse_args()

    app = QApplication(sys.argv)
    app.setApplicationName("findethedox")

    # Resolve the database path; prompt with a file dialog if absent or not found
    db_path: str | None = None
    if args.db:
        p = Path(args.db).resolve()
        if p.exists():
            db_path = str(p)
        else:
            print(f"Warning: database file not found: {p}", file=sys.stderr)

    if db_path is None:
        default = Path("allmydox.db").resolve()
        if default.exists():
            db_path = str(default)
        else:
            chosen, _ = QFileDialog.getOpenFileName(
                None, "Open allmydox database", str(Path.home()),
                "SQLite databases (*.db);;All files (*)",
            )
            if not chosen:
                sys.exit(0)
            db_path = chosen

    window = MainWindow(db_path, docs_folder=args.docs)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
