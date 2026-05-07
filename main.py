#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

from PyQt6.QtWidgets import QApplication, QFileDialog

import config as config_mod
from app import MainWindow


def main():
    parser = argparse.ArgumentParser(description="Explore an allmydox database visually.")
    parser.add_argument("db", nargs="?", default=None,
                        help="Path to the allmydox SQLite database (default: browse interactively)")
    parser.add_argument("--docs", default=None, metavar="FOLDER",
                        help="Root folder for documents referenced in the database")
    parser.add_argument("--cache", default=None, metavar="FILE",
                        help="Path to the cache database (default: next to the source DB)")
    args = parser.parse_args()

    app = QApplication(sys.argv)
    app.setApplicationName("findethedox")

    cfg = config_mod.load()

    # Resolve the database path; CLI arg > config > default > file dialog
    db_path: str | None = None
    if args.db:
        p = Path(args.db).resolve()
        if p.exists():
            db_path = str(p)
        else:
            print(f"Warning: database file not found: {p}", file=sys.stderr)

    if db_path is None and cfg.get("db_path"):
        p = Path(cfg["db_path"])
        if p.exists():
            db_path = str(p)

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

    docs_folder: str | None = args.docs or cfg.get("docs_folder") or None
    cache_path:  str | None = args.cache or cfg.get("cache_path") or None

    window = MainWindow(db_path, docs_folder=docs_folder, cache_path=cache_path)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
