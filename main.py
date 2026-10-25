#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

from PyQt6.QtWidgets import QApplication, QDialog

import cache as cache_mod
import config as config_mod
from app import MainWindow, SetupDialog


def main():
    parser = argparse.ArgumentParser(description="Explore one or more allmydox databases visually.")
    parser.add_argument("db", nargs="*", default=None,
                        help="Paths to allmydox SQLite databases (default: browse interactively)")
    parser.add_argument("--docs", default=None, metavar="FOLDER",
                        help="Root folder for documents referenced in the databases")
    parser.add_argument("--data", default=None, metavar="FOLDER",
                        help="Data folder holding the databases and their cache files "
                             "(one <name>.cache.db per database)")
    args = parser.parse_args()

    app = QApplication(sys.argv)
    app.setApplicationName("findethedox")

    cfg = config_mod.load()

    data_folder: str | None = (
        str(Path(args.data).resolve()) if args.data else cfg.get("data_folder") or None
    )
    if data_folder and not Path(data_folder).is_dir():
        print(f"Warning: data folder not found: {data_folder}", file=sys.stderr)
        data_folder = None

    # Resolve database paths: CLI args > data folder + config > settings dialog
    db_paths: list[str] = []

    for raw in (args.db or []):
        p = Path(raw).resolve()
        if p.exists():
            db_paths.append(str(p))
        else:
            print(f"Warning: database file not found: {p}", file=sys.stderr)

    # Databases outside the data folder; older configs listed every DB in db_paths
    external: list[str] = cfg.get("external_dbs") or (
        cfg.get("db_paths") or ([cfg["db_path"]] if cfg.get("db_path") else [])
    )
    external = [p for p in external if p and Path(p).exists()]
    disabled = set(cfg.get("disabled_dbs") or [])

    if not db_paths:
        found = cache_mod.find_databases(data_folder) if data_folder else []
        db_paths = [p for p in dict.fromkeys(found + external) if p not in disabled]

    docs_folder: str | None = args.docs or cfg.get("docs_folder") or None
    db_docs: dict[str, str] = cfg.get("db_docs") or {}

    if not db_paths:
        dlg = SetupDialog(data_folder or "", external_dbs=external,
                          disabled_dbs=sorted(disabled), db_docs=db_docs)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            sys.exit(0)
        db_paths = dlg.result_db_paths()
        if not db_paths:
            sys.exit(0)
        data_folder = dlg.result_data_folder() or None
        db_docs = dlg.result_db_docs()
        cfg.update({
            "data_folder":  data_folder,
            "external_dbs": dlg.result_external_dbs(),
            "disabled_dbs": dlg.result_disabled_dbs(),
            "db_docs":      db_docs,
        })
        for legacy in ("db_paths", "db_path", "cache_path", "cache_folder"):
            cfg.pop(legacy, None)
        config_mod.save(cfg)

    window = MainWindow(db_paths, data_folder=data_folder,
                        docs_folder=docs_folder, db_docs=db_docs)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
