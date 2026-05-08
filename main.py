#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

from PyQt6.QtWidgets import QApplication, QFileDialog

import config as config_mod
from app import MainWindow


def main():
    parser = argparse.ArgumentParser(description="Explore one or more allmydox databases visually.")
    parser.add_argument("db", nargs="*", default=None,
                        help="Paths to allmydox SQLite databases (default: browse interactively)")
    parser.add_argument("--docs", default=None, metavar="FOLDER",
                        help="Root folder for documents referenced in the databases")
    parser.add_argument("--cache", default=None, metavar="FILE",
                        help="Path to the cache database (default: next to the first source DB)")
    args = parser.parse_args()

    app = QApplication(sys.argv)
    app.setApplicationName("findethedox")

    cfg = config_mod.load()

    # Resolve database paths: CLI args > config > default > file dialog
    db_paths: list[str] = []

    for raw in (args.db or []):
        p = Path(raw).resolve()
        if p.exists():
            db_paths.append(str(p))
        else:
            print(f"Warning: database file not found: {p}", file=sys.stderr)

    if not db_paths:
        # Support both new list format and legacy single-path format in config
        saved = cfg.get("db_paths") or (
            [cfg["db_path"]] if cfg.get("db_path") else []
        )
        for raw in saved:
            if raw and Path(raw).exists():
                db_paths.append(str(raw))

    if not db_paths:
        default = Path("allmydox.db").resolve()
        if default.exists():
            db_paths = [str(default)]
        else:
            chosen, _ = QFileDialog.getOpenFileName(
                None, "Open allmydox database", str(Path.home()),
                "SQLite databases (*.db);;All files (*)",
            )
            if not chosen:
                sys.exit(0)
            db_paths = [chosen]

    docs_folder: str | None = args.docs or cfg.get("docs_folder") or None
    cache_path:  str | None = args.cache or cfg.get("cache_path") or None

    window = MainWindow(db_paths, docs_folder=docs_folder, cache_path=cache_path)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
