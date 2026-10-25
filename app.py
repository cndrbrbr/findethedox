"""Main application window for findethedox."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QColor
from PyQt6.QtWidgets import (
    QApplication, QDialog, QDialogButtonBox, QMainWindow, QWidget,
    QHBoxLayout, QVBoxLayout,
    QLineEdit, QLabel, QListWidget, QListWidgetItem,
    QTreeWidget, QTreeWidgetItem, QHeaderView,
    QSplitter, QStatusBar, QProgressDialog, QFileDialog, QMessageBox,
    QPushButton, QInputDialog, QSizePolicy,
)

import cache as cache_mod
import config as config_mod
import query
from cloud_widget import CloudWidget
from doc_viewer import DocViewerDialog

# Status of one source database relative to its cache file
_STATUS_DISPLAY = {
    "current":  ("✓", "#4ec94e"),   # green  — in cache, up to date
    "outdated": ("⚠", "#e5c07b"),   # yellow — in cache, new docs available
    "missing":  ("✗", "#e06c75"),   # red    — cache file is not for this DB
    "no_cache": ("—", "#888888"),   # grey   — cache file doesn't exist yet
}


# ---------------------------------------------------------------------------
# Background workers
# ---------------------------------------------------------------------------

class _IndexWorker(QThread):
    """Ensures allmydox co-occurrence indexes exist on all source DBs."""
    done = pyqtSignal()

    def __init__(self, db_paths: list):
        super().__init__()
        self._db_paths = db_paths

    def run(self):
        for db_path in self._db_paths:
            conn = query.connect(db_path)
            query.ensure_indexes(conn)
            conn.close()
        self.done.emit()


class _CacheWorker(QThread):
    """Builds or updates the per-database score caches in the background."""
    progress = pyqtSignal(str, int, int)   # label, current, total
    done     = pyqtSignal()
    error    = pyqtSignal(str)

    def __init__(self, jobs: list[tuple[str, str, str]]):
        """jobs: (db_path, cache_path, mode) with mode 'build' or 'update'."""
        super().__init__()
        self._jobs = jobs

    def run(self):
        n = len(self._jobs)
        try:
            for i, (db_path, cache_path, mode) in enumerate(self._jobs):
                fn = cache_mod.update if mode == "update" else cache_mod.build
                prefix = f"[{Path(db_path).name}] " if n > 1 else ""

                def report(lbl, cur, tot, i=i, prefix=prefix):
                    # Map each job's own progress onto one overall bar
                    frac = cur / tot if tot else 0
                    self.progress.emit(prefix + lbl, int((i + frac) * 100), n * 100)

                fn(db_path, cache_path, progress=report)
            self.done.emit()
        except Exception as exc:
            self.error.emit(str(exc))


class _SearchWorker(QThread):
    """Runs co-occurrence lookup and document list queries in the background."""
    cooc_ready = pyqtSignal(list, str)   # (list of dicts, word)
    docs_ready = pyqtSignal(list, str)   # (list[DocOccurrence], word)
    not_found  = pyqtSignal(str)
    error      = pyqtSignal(str)

    def __init__(self, db_paths: list, cache_paths: list, word: str, docs_only: bool = False):
        super().__init__()
        self._db_paths    = db_paths
        self._cache_paths = cache_paths
        self._word       = word
        self._docs_only  = docs_only

    def run(self):
        try:
            if not self._docs_only:
                conns = [cache_mod.connect(cp) for cp in self._cache_paths]
                try:
                    rows = cache_mod.cooccurrences(conns, self._word)
                finally:
                    for cc in conns:
                        cc.close()
                if not rows:
                    self.not_found.emit(self._word)
                    return
                self.cooc_ready.emit(rows, self._word)

            occs: list[query.DocOccurrence] = []
            for db_path in self._db_paths:
                dc = query.connect(db_path)
                for occ in query.document_occurrences(dc, self._word):
                    occ.source_db = db_path
                    occs.append(occ)
                dc.close()
            # Deduplicate across sources
            seen: set[tuple] = set()
            unique: list[query.DocOccurrence] = []
            for occ in sorted(occs, key=lambda o: (o.filename, o.pagenumber)):
                key = (occ.filename, occ.folderpath, occ.pagenumber)
                if key not in seen:
                    seen.add(key)
                    unique.append(occ)
            self.docs_ready.emit(unique, self._word)
        except Exception as exc:
            self.error.emit(str(exc))


class _SentenceWorker(QThread):
    """Extracts sentences containing the search word from a document file."""
    ready = pyqtSignal(list)
    error = pyqtSignal(str)

    def __init__(self, filepath: str, word: str):
        super().__init__()
        self._filepath = filepath
        self._word = word

    def run(self):
        try:
            from doc_viewer import sentences_containing
            self.ready.emit(sentences_containing(self._filepath, self._word))
        except Exception as exc:
            self.error.emit(str(exc))


# ---------------------------------------------------------------------------
# Setup dialog — data folder, databases & caches
# ---------------------------------------------------------------------------

class SetupDialog(QDialog):
    """Configure the data folder, which databases to search, and per-database
    documents folders.  Every database gets its own cache file
    (<stem>.cache.db) in the data folder."""

    def __init__(self, data_folder: str = "",
                 external_dbs: list[str] | None = None,
                 disabled_dbs: list[str] | None = None,
                 db_docs: dict[str, str] | None = None,
                 db_drives: dict[str, str] | None = None,
                 parent=None):
        super().__init__(parent)
        self.setWindowTitle("findethedox — Settings")
        self.resize(820, 560)

        self._data_folder: str = data_folder
        # Databases outside the data folder (added via "Add Database…")
        self._external: list[str] = list(external_dbs or [])
        self._disabled: set[str] = set(disabled_dbs or [])
        self._db_paths: list[str] = []
        self._db_docs: dict[str, str] = dict(db_docs or {})
        # Windows drive letter per DB that replaces the share in stored paths
        self._db_drives: dict[str, str] = dict(db_drives or {})
        self._shares: dict[str, str] = {}   # db_path → e.g. \\server\share
        self._worker: _CacheWorker | None = None

        self._build_ui()
        self._apply_dark_theme()
        self._rescan()

    # ------------------------------------------------------------------
    # Public accessors

    def result_db_paths(self) -> list[str]:
        """Databases checked for searching."""
        return [p for p in self._db_paths if p not in self._disabled]

    def result_data_folder(self) -> str:
        return self._data_folder

    def result_external_dbs(self) -> list[str]:
        return list(self._external)

    def result_disabled_dbs(self) -> list[str]:
        return sorted(p for p in self._disabled if p in self._db_paths)

    def result_db_docs(self) -> dict[str, str]:
        return {k: v for k, v in self._db_docs.items() if v}

    def result_db_drives(self) -> dict[str, str]:
        return {k: v for k, v in self._db_drives.items() if v}

    def _cache_path(self, db_path: str) -> str:
        return cache_mod.cache_path_for(db_path, self._data_folder or None)

    # ------------------------------------------------------------------
    # UI

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setSpacing(10)
        root.setContentsMargins(16, 16, 16, 16)

        # Data folder row
        folder_row = QHBoxLayout()
        folder_row.addWidget(QLabel("Data folder:"))
        self._folder_edit = QLineEdit()
        self._folder_edit.setReadOnly(True)
        self._folder_edit.setPlaceholderText("Folder containing the databases and their cache files")
        folder_row.addWidget(self._folder_edit, 1)
        btn_browse = QPushButton("Browse…")
        btn_browse.clicked.connect(self._on_browse_folder)
        folder_row.addWidget(btn_browse)
        btn_rescan = QPushButton("Rescan")
        btn_rescan.setToolTip("Look for new databases in the data folder")
        btn_rescan.clicked.connect(self._rescan)
        folder_row.addWidget(btn_rescan)
        root.addLayout(folder_row)

        self._folder_hint = QLabel()
        self._folder_hint.setStyleSheet("color:#888;font-size:11px;margin-left:2px;")
        root.addWidget(self._folder_hint)

        # Databases header
        db_hdr = QHBoxLayout()
        db_hdr.addWidget(QLabel("Databases:"))
        db_hdr.addStretch()
        self._add_btn = QPushButton("Add Database…")
        self._add_btn.setToolTip("Add a database stored outside the data folder")
        self._add_btn.clicked.connect(self._on_add_database)
        db_hdr.addWidget(self._add_btn)
        root.addLayout(db_hdr)

        # Database tree: database, cache file, drive letter, docs folder
        self._db_tree = QTreeWidget()
        self._db_tree.setColumnCount(4)
        self._db_tree.setHeaderLabels(["Database", "Cache File", "Drive", "Documents Folder"])
        self._db_tree.header().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self._db_tree.header().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self._db_tree.header().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self._db_tree.header().setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        self._db_tree.setSelectionMode(QTreeWidget.SelectionMode.ExtendedSelection)
        self._db_tree.setRootIsDecorated(False)
        self._db_tree.itemDoubleClicked.connect(self._on_tree_double_clicked)
        self._db_tree.itemChanged.connect(self._on_item_changed)
        root.addWidget(self._db_tree, 1)

        docs_hint = QLabel("Tick the databases to search  ·  Double-click Drive or "
                           "Documents Folder to set where its documents are found")
        docs_hint.setStyleSheet("color:#888;font-size:10px;")
        root.addWidget(docs_hint)

        # Remove row
        remove_row = QHBoxLayout()
        remove_row.addStretch()
        self._remove_btn = QPushButton("Remove Selected")
        self._remove_btn.setToolTip("Remove external databases; databases in the "
                                    "data folder are only unticked")
        self._remove_btn.clicked.connect(self._on_remove)
        remove_row.addWidget(self._remove_btn)
        root.addLayout(remove_row)

        # Status + cache update
        status_row = QHBoxLayout()
        self._status_lbl = QLabel()
        status_row.addWidget(self._status_lbl, 1)
        self._update_btn = QPushButton("Build Caches")
        self._update_btn.clicked.connect(self._on_update_cache)
        status_row.addWidget(self._update_btn)
        root.addLayout(status_row)

        # OK / Cancel
        btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Cancel | QDialogButtonBox.StandardButton.Apply
        )
        self._apply_btn = btns.button(QDialogButtonBox.StandardButton.Apply)
        btns.clicked.connect(
            lambda b: self.accept() if b is self._apply_btn else self.reject()
        )
        root.addWidget(btns)

    def _apply_dark_theme(self):
        self.setStyleSheet("""
            QDialog, QWidget { background:#1e1e1e; color:#d4d4d4; }
            QLabel { color:#d4d4d4; }
            QLineEdit {
                background:#2d2d2d; border:1px solid #555;
                border-radius:4px; padding:4px 8px; color:#d4d4d4;
            }
            QTreeWidget {
                background:#2d2d2d; border:1px solid #444; color:#d4d4d4;
                alternate-background-color:#252525;
            }
            QTreeWidget::item { padding:3px 6px; }
            QTreeWidget::item:hover    { background:#3a3a3a; }
            QTreeWidget::item:selected { background:#264f78; color:#d4d4d4; }
            QHeaderView::section {
                background:#333; color:#aaa; border:1px solid #444; padding:3px 6px;
            }
            QPushButton {
                background:#2d2d2d; border:1px solid #555;
                border-radius:4px; padding:4px 12px; color:#d4d4d4;
            }
            QPushButton:hover    { background:#3a3a3a; }
            QPushButton:disabled { color:#666; border-color:#3a3a3a; }
        """)

    # ------------------------------------------------------------------
    # State

    def _rescan(self):
        """Rebuild the database list from the data folder plus external DBs."""
        found = cache_mod.find_databases(self._data_folder) if self._data_folder else []
        in_folder = set(found)
        self._external = [p for p in self._external if p not in in_folder]
        self._db_paths = found + self._external
        for db_path in self._db_paths:
            self._load_db_meta(db_path)
        self._refresh()

    def _load_db_meta(self, db_path: str):
        """Docs folder and drive: cache meta wins over the passed-in config copy."""
        cp = self._cache_path(db_path)
        stored = cache_mod.get_docs_folder(cp, db_path)
        if stored:
            self._db_docs[db_path] = stored
        drive = cache_mod.get_docs_drive(cp)
        if drive:
            self._db_drives[db_path] = drive
        if db_path not in self._shares:
            self._shares[db_path] = query.share_root(db_path)

    def _statuses(self) -> dict[str, str]:
        return {p: cache_mod.status(self._cache_path(p), p) for p in self._db_paths}

    def _refresh(self):
        self._folder_edit.setText(self._data_folder)
        self._folder_hint.setText(
            "  Each database gets its own cache file <name>.cache.db in this folder."
            if self._data_folder else
            "  No data folder set — cache files are stored next to each database."
        )

        statuses = self._statuses()
        self._db_tree.blockSignals(True)
        self._db_tree.clear()
        for db_path in self._db_paths:
            status = statuses[db_path]
            icon, color = _STATUS_DISPLAY[status]
            docs = self._db_docs.get(db_path, "")
            docs_ok = Path(docs).is_dir() if docs else True
            external = db_path in self._external
            label = db_path if external else Path(db_path).name

            drive = self._db_drives.get(db_path, "")
            share = self._shares.get(db_path, "")

            item = QTreeWidgetItem([f"  {icon}  {label}",
                                    Path(self._cache_path(db_path)).name,
                                    drive or "—", docs])
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            item.setCheckState(0, Qt.CheckState.Unchecked if db_path in self._disabled
                               else Qt.CheckState.Checked)
            item.setToolTip(0, db_path + ("  (outside data folder)" if external else ""))
            item.setToolTip(1, self._cache_path(db_path))
            item.setForeground(0, QColor(color))
            item.setForeground(1, QColor("#888888"))
            item.setTextAlignment(2, Qt.AlignmentFlag.AlignCenter)
            item.setForeground(2, QColor("#d4d4d4") if drive else QColor("#888888"))
            item.setToolTip(2, (f"{share}\\…  →  {drive}\\…" if drive else
                                f"Documents are stored under {share}\\… — "
                                "double-click to map that to a drive letter")
                            if share else "Double-click to set a drive letter")
            item.setForeground(3, QColor("#e06c75") if (docs and not docs_ok)
                               else QColor("#888888") if not docs
                               else QColor("#d4d4d4"))
            item.setData(0, Qt.ItemDataRole.UserRole, db_path)
            self._db_tree.addTopLevelItem(item)
        self._db_tree.blockSignals(False)

        self._refresh_status(statuses)

    def _refresh_status(self, statuses: dict[str, str] | None = None):
        if statuses is None:
            statuses = self._statuses()
        active = [statuses[p] for p in self.result_db_paths()]
        n_ok = active.count("current")

        if not self._db_paths:
            self._status_lbl.setText(
                "No databases found — choose a data folder or add a database."
            )
        elif not active:
            self._status_lbl.setText("No database ticked for searching.")
        elif n_ok == len(active):
            self._status_lbl.setText(f"All {len(active)} cache(s) up to date.")
        else:
            self._status_lbl.setText(f"{n_ok} of {len(active)} cache(s) up to date.")

        any_built = any(s != "no_cache" for s in active)
        self._update_btn.setText("Update Caches" if any_built else "Build Caches")
        self._update_btn.setEnabled(bool(active) and n_ok < len(active))
        self._apply_btn.setEnabled(bool(active))
        self._remove_btn.setEnabled(bool(self._db_paths))

    # ------------------------------------------------------------------
    # Actions

    def _on_browse_folder(self):
        folder = QFileDialog.getExistingDirectory(
            self, "Select Data Folder", self._data_folder or str(Path.home())
        )
        if folder:
            self._data_folder = folder
            self._rescan()

    def _on_item_changed(self, item: QTreeWidgetItem, column: int):
        if column != 0:
            return
        db_path = item.data(0, Qt.ItemDataRole.UserRole)
        if item.checkState(0) == Qt.CheckState.Checked:
            self._disabled.discard(db_path)
        else:
            self._disabled.add(db_path)
        self._refresh_status()

    def _on_tree_double_clicked(self, item: QTreeWidgetItem, column: int):
        db_path = item.data(0, Qt.ItemDataRole.UserRole)
        if column == 2:
            self._choose_drive(db_path)
        elif column == 3:
            self._choose_docs_folder(db_path)

    def _choose_drive(self, db_path: str):
        none = "(none)"
        letters = [none] + [f"{chr(c)}:" for c in range(ord("A"), ord("Z") + 1)]
        current = self._db_drives.get(db_path, "")
        share = self._shares.get(db_path, "") or "the stored path"
        choice, ok = QInputDialog.getItem(
            self, f"Drive for {Path(db_path).name}",
            f"Windows drive letter that replaces {share}\nin the stored document paths:",
            letters, letters.index(current) if current in letters else 0, False,
        )
        if ok:
            self._db_drives[db_path] = "" if choice == none else choice
            self._refresh()

    def _choose_docs_folder(self, db_path: str):
        current = self._db_docs.get(db_path, "")
        folder = QFileDialog.getExistingDirectory(
            self, f"Documents folder for {Path(db_path).name}",
            current or str(Path.home()),
        )
        if folder:
            self._db_docs[db_path] = folder
            self._refresh()

    def _on_add_database(self):
        start = self._data_folder or (
            str(Path(self._db_paths[-1]).parent) if self._db_paths else str(Path.home())
        )
        chosen, _ = QFileDialog.getOpenFileName(
            self, "Add allmydox Database", start,
            "SQLite databases (*.db);;All files (*)",
        )
        if not chosen:
            return
        chosen = str(Path(chosen).absolute())
        if cache_mod.is_cache_file(chosen):
            QMessageBox.warning(self, "Add Database",
                                "This is a findethedox cache file, not an allmydox database.")
            return
        if chosen in self._db_paths:
            self._disabled.discard(chosen)
            self._refresh()
            return
        # Two databases with the same name would share one cache file
        new_cache = self._cache_path(chosen)
        for other in self._db_paths:
            if self._cache_path(other) == new_cache:
                QMessageBox.warning(
                    self, "Add Database",
                    f"{Path(chosen).name} would use the same cache file as\n"
                    f"{other}.\n\nRename one of the databases first.",
                )
                return
        self._external.append(chosen)
        self._db_paths.append(chosen)
        self._load_db_meta(chosen)
        self._refresh()

    def _on_remove(self):
        selected = {
            item.data(0, Qt.ItemDataRole.UserRole)
            for item in self._db_tree.selectedItems()
        }
        removed = selected & set(self._external)
        self._external = [p for p in self._external if p not in removed]
        self._db_paths = [p for p in self._db_paths if p not in removed]
        # Databases inside the data folder cannot be removed, only unticked
        self._disabled |= selected - removed
        self._refresh()

    def _on_update_cache(self):
        jobs: list[tuple[str, str, str]] = []
        for db_path in self.result_db_paths():
            cp = self._cache_path(db_path)
            st = cache_mod.status(cp, db_path)
            if st == "outdated":
                jobs.append((db_path, cp, "update"))
            elif st != "current":
                jobs.append((db_path, cp, "build"))
        if not jobs:
            return

        dlg = QProgressDialog("Preparing…", None, 0, 100, self)
        dlg.setWindowTitle("findethedox — Building caches")
        dlg.setWindowModality(Qt.WindowModality.WindowModal)
        dlg.setMinimumWidth(520)
        dlg.setValue(0)
        dlg.show()

        self._update_btn.setEnabled(False)
        self._add_btn.setEnabled(False)

        self._worker = _CacheWorker(jobs)

        def on_progress(lbl: str, cur: int, tot: int):
            dlg.setLabelText(lbl)
            dlg.setValue(int(cur / tot * 100) if tot else 0)

        def on_done():
            dlg.close()
            self._add_btn.setEnabled(True)
            # Docs folders / drives chosen before the cache existed can be stored now
            for db_path, cp, _ in jobs:
                if self._db_docs.get(db_path):
                    cache_mod.set_docs_folder(cp, self._db_docs[db_path])
                cache_mod.set_docs_drive(cp, self._db_drives.get(db_path, ""))
            self._refresh()

        def on_error(msg: str):
            dlg.close()
            self._add_btn.setEnabled(True)
            self._refresh()
            QMessageBox.critical(self, "Cache Error", f"Cache operation failed:\n{msg}")

        self._worker.progress.connect(on_progress)
        self._worker.done.connect(on_done)
        self._worker.error.connect(on_error)
        self._worker.start()


# ---------------------------------------------------------------------------
# Main window
# ---------------------------------------------------------------------------

class MainWindow(QMainWindow):

    def __init__(
        self,
        db_paths,
        data_folder: str | None = None,
        docs_folder: str | None = None,
        db_docs: dict[str, str] | None = None,
        db_drives: dict[str, str] | None = None,
    ):
        super().__init__()
        self.resize(1280, 760)

        if isinstance(db_paths, str):
            db_paths = [db_paths]
        self._db_paths    = db_paths
        self._data_folder = data_folder
        self._docs_folder = docs_folder
        # One cache file per source database
        self._cache_paths: dict[str, str] = {
            p: cache_mod.cache_path_for(p, data_folder) for p in db_paths
        }
        self._db_docs: dict[str, str] = dict(db_docs or {})
        self._db_drives: dict[str, str] = dict(db_drives or {})
        self._cache_conns: list = []                       # set after caches are ready
        self._current_word: str = ""
        self._search_worker: _SearchWorker | None = None
        self._sentence_worker: _SentenceWorker | None = None
        self._sent_ever_shown: bool = False
        self._current_filepath: str = ""

        self._build_ui()
        self._build_menu()
        self._apply_dark_theme()
        self._update_title()

        # Step 1: ensure DB indexes exist (fast, only meaningful on first launch)
        self._index_worker = _IndexWorker(self._db_paths)
        self._index_worker.done.connect(self._on_indexes_ready)
        self._status.showMessage("Checking database indexes…")
        self._index_worker.start()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QHBoxLayout(central)
        root.setContentsMargins(8, 8, 8, 8)

        left = QWidget()
        left_layout = QVBoxLayout(left)
        left_layout.setSpacing(6)

        search_row = QHBoxLayout()
        search_row.addWidget(QLabel("Search:"))
        self._search = QLineEdit()
        self._search.setPlaceholderText("Type a noun, name, or verb and press Enter…")
        self._search.returnPressed.connect(self._on_search)
        self._search.textChanged.connect(self._on_text_changed)
        search_row.addWidget(self._search)
        left_layout.addLayout(search_row)

        clouds_row = QHBoxLayout()
        self._clouds: dict[str, CloudWidget] = {}
        for kind in ("name", "noun", "verb"):
            cw = CloudWidget(kind, self._on_left_click, self._on_right_click)
            self._clouds[kind] = cw
            clouds_row.addWidget(cw)
        left_layout.addLayout(clouds_row)

        right = QWidget()
        right_layout = QVBoxLayout(right)
        right_layout.setContentsMargins(0, 0, 4, 0)
        right_layout.setSpacing(4)

        # --- document list section ---
        doc_widget = QWidget()
        doc_layout = QVBoxLayout(doc_widget)
        doc_layout.setContentsMargins(0, 0, 0, 0)
        doc_layout.setSpacing(4)

        self._doc_title = QLabel("Documents")
        self._doc_title.setStyleSheet("font-weight:bold;font-size:13px;")
        doc_layout.addWidget(self._doc_title)

        self._doc_list = QListWidget()
        self._doc_list.itemDoubleClicked.connect(self._on_doc_clicked)
        self._doc_list.itemClicked.connect(self._on_doc_selected)
        doc_layout.addWidget(self._doc_list)

        hint = QLabel("Click: sentences  ·  Double-click: open")
        hint.setStyleSheet("color:#888;font-size:10px;")
        doc_layout.addWidget(hint)

        # --- sentence list section ---
        self._sent_widget = QWidget()
        sent_layout = QVBoxLayout(self._sent_widget)
        sent_layout.setContentsMargins(0, 0, 0, 0)
        sent_layout.setSpacing(4)

        self._sent_title = QLabel("Sentences")
        self._sent_title.setStyleSheet("font-weight:bold;font-size:13px;")
        sent_layout.addWidget(self._sent_title)

        self._sent_list = QListWidget()
        self._sent_list.setWordWrap(True)
        self._sent_list.setStyleSheet(
            "QListWidget::item { border-bottom: 1px solid #333; padding: 4px 2px; }"
        )
        self._sent_list.itemDoubleClicked.connect(self._on_sentence_double_clicked)
        sent_layout.addWidget(self._sent_list)

        sent_hint = QLabel("Double-click: open document at this sentence")
        sent_hint.setStyleSheet("color:#888;font-size:10px;")
        sent_layout.addWidget(sent_hint)

        self._sent_widget.setVisible(False)

        # inner splitter: doc list | sentence list
        self._inner_splitter = QSplitter(Qt.Orientation.Horizontal)
        self._inner_splitter.addWidget(doc_widget)
        self._inner_splitter.addWidget(self._sent_widget)
        self._inner_splitter.setStretchFactor(0, 0)
        self._inner_splitter.setStretchFactor(1, 1)
        right_layout.addWidget(self._inner_splitter)

        self._right_panel = right
        self._right_panel.setVisible(False)

        self._outer_splitter = QSplitter(Qt.Orientation.Horizontal)
        self._outer_splitter.addWidget(left)
        self._outer_splitter.addWidget(right)
        self._outer_splitter.setStretchFactor(0, 1)
        self._outer_splitter.setStretchFactor(1, 0)
        root.addWidget(self._outer_splitter)

        self._status = QStatusBar()
        self.setStatusBar(self._status)

    def _update_title(self):
        if len(self._db_paths) == 1:
            self.setWindowTitle(f"findethedox by cndrbrbr — {Path(self._db_paths[0]).name}")
        else:
            self.setWindowTitle(f"findethedox by cndrbrbr — {len(self._db_paths)} databases")

    def _build_menu(self):
        file_menu = self.menuBar().addMenu("File")

        open_db = file_menu.addAction("Open Database…")
        open_db.setShortcut("Ctrl+O")
        open_db.triggered.connect(self._on_open_database)

        settings_action = file_menu.addAction("Settings…")
        settings_action.setShortcut("Ctrl+,")
        settings_action.triggered.connect(self._on_setup_dialog)

        toolbar = self.addToolBar("Tools")
        toolbar.setMovable(False)
        rebuild_action = toolbar.addAction("Rebuild Cache")
        rebuild_action.setToolTip("Delete the existing cache and rebuild it from scratch")
        rebuild_action.triggered.connect(self._on_rebuild_cache)

        file_menu.addSeparator()
        exit_action = file_menu.addAction("Exit")
        exit_action.setShortcut("Ctrl+Q")
        exit_action.triggered.connect(self.close)

        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        toolbar.addWidget(spacer)
        close_action = toolbar.addAction("Close")
        close_action.setToolTip("Close findethedox (Ctrl+Q)")
        close_action.triggered.connect(self.close)

    def _apply_dark_theme(self):
        self.setStyleSheet("""
            QMainWindow, QWidget { background:#1e1e1e; color:#d4d4d4; }
            QMenuBar { background:#252526; color:#d4d4d4; }
            QMenuBar::item:selected { background:#3a3a3a; }
            QMenu { background:#252526; color:#d4d4d4; border:1px solid #444; }
            QMenu::item:selected { background:#264f78; }
            QLineEdit {
                background:#2d2d2d; border:1px solid #555;
                border-radius:4px; padding:4px 8px; color:#d4d4d4;
            }
            QListWidget {
                background:#2d2d2d; border:1px solid #444; color:#d4d4d4;
            }
            QListWidget::item:hover    { background:#3a3a3a; }
            QListWidget::item:selected { background:#264f78; }
            QPushButton {
                background:#2d2d2d; border:1px solid #555;
                border-radius:4px; padding:4px 12px; color:#d4d4d4;
            }
            QPushButton:hover { background:#3a3a3a; }
            QSplitter::handle { background:#333; }
            QScrollArea { border:none; }
            QProgressDialog { background:#1e1e1e; color:#d4d4d4; }
        """)

    # ------------------------------------------------------------------
    # Menu actions
    # ------------------------------------------------------------------

    def _on_open_database(self):
        chosen, _ = QFileDialog.getOpenFileName(
            self, "Open allmydox database", str(Path(self._db_paths[0]).parent),
            "SQLite databases (*.db);;All files (*)",
        )
        if not chosen:
            return
        new_win = MainWindow([chosen], data_folder=self._data_folder,
                             docs_folder=self._docs_folder, db_docs=self._db_docs,
                             db_drives=self._db_drives)
        new_win.show()
        self.close()

    def _on_setup_dialog(self):
        cfg = config_mod.load()
        data_folder = self._data_folder or ""
        in_folder = set(cache_mod.find_databases(data_folder)) if data_folder else set()
        external = list(dict.fromkeys(
            [p for p in cfg.get("external_dbs", []) if Path(p).exists()]
            + [p for p in self._db_paths if p not in in_folder]
        ))
        dlg = SetupDialog(
            data_folder,
            external_dbs=external,
            disabled_dbs=cfg.get("disabled_dbs", []),
            db_docs=self._db_docs,
            db_drives=self._db_drives,
            parent=self,
        )
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        new_db_paths     = dlg.result_db_paths()
        new_data_folder  = dlg.result_data_folder() or None
        new_db_docs      = dlg.result_db_docs()
        new_db_drives    = dlg.result_db_drives()
        self._close_caches()
        # Persist per-DB docs folders into each database's cache meta table
        for db_path, folder in new_db_docs.items():
            cache_mod.set_docs_folder(
                cache_mod.cache_path_for(db_path, new_data_folder), folder
            )
        cfg["data_folder"]  = new_data_folder
        cfg["external_dbs"] = dlg.result_external_dbs()
        cfg["disabled_dbs"] = dlg.result_disabled_dbs()
        # '' clears a drive that was removed in the dialog
        for db_path in new_db_paths:
            cache_mod.set_docs_drive(
                cache_mod.cache_path_for(db_path, new_data_folder),
                new_db_drives.get(db_path, ""),
            )
        cfg["db_docs"]      = new_db_docs
        cfg["db_drives"]    = new_db_drives
        for legacy in ("db_paths", "db_path", "cache_path", "cache_folder"):
            cfg.pop(legacy, None)
        config_mod.save(cfg)
        new_win = MainWindow(new_db_paths, data_folder=new_data_folder,
                             docs_folder=self._docs_folder, db_docs=new_db_docs,
                             db_drives=new_db_drives)
        new_win.show()
        self.close()

    def _on_rebuild_cache(self):
        reply = QMessageBox.question(
            self, "Rebuild Caches",
            "This will delete the existing caches and rebuild them from scratch.\n"
            "The operation may take several minutes.\n\nContinue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return
        self._close_caches()
        self._start_cache_worker(
            [(p, cp, "build") for p, cp in self._cache_paths.items()], rebuild=True
        )

    def _close_caches(self):
        for conn in self._cache_conns:
            conn.close()
        self._cache_conns = []

    def _open_caches(self):
        self._close_caches()
        self._cache_conns = [cache_mod.connect(cp) for cp in self._cache_paths.values()]

    # ------------------------------------------------------------------
    # Startup sequence: indexes → cache → show clouds
    # ------------------------------------------------------------------

    def _on_indexes_ready(self):
        # Fill per-DB docs folders from cache meta where config has none
        for db_path, cp in self._cache_paths.items():
            if not self._db_docs.get(db_path):
                stored = cache_mod.get_docs_folder(cp, db_path)
                if stored:
                    self._db_docs[db_path] = stored
            if not self._db_drives.get(db_path):
                drive = cache_mod.get_docs_drive(cp)
                if drive:
                    self._db_drives[db_path] = drive
        # Existing caches are used as-is; the user can update them in Settings
        missing = [(p, cp, "build") for p, cp in self._cache_paths.items()
                   if cache_mod.status(cp, p) in ("no_cache", "missing")]
        if missing:
            self._start_cache_worker(missing)
        else:
            self._open_caches()
            self._load_global()

    def _start_cache_worker(self, jobs: list[tuple[str, str, str]], rebuild: bool = False):
        if rebuild:
            title = "findethedox — cache rebuild"
            label = "Rebuilding search caches…"
        else:
            title = "findethedox — first launch"
            label = "Building search cache — this runs once per database and takes a few minutes."

        self._progress_dlg = QProgressDialog(label, None, 0, 100, self)
        self._progress_dlg.setWindowTitle(title)
        self._progress_dlg.setWindowModality(Qt.WindowModality.WindowModal)
        self._progress_dlg.setMinimumWidth(500)
        self._progress_dlg.setValue(0)
        self._progress_dlg.show()

        self._cache_jobs = jobs
        self._cache_worker = _CacheWorker(jobs)
        self._cache_worker.progress.connect(self._on_cache_progress)
        self._cache_worker.done.connect(self._on_cache_done)
        self._cache_worker.error.connect(self._on_cache_error)
        self._cache_worker.start()

    def _on_cache_progress(self, label: str, current: int, total: int):
        pct = int(current / total * 100) if total else 0
        self._progress_dlg.setLabelText(label)
        self._progress_dlg.setValue(pct)

    def _on_cache_done(self):
        self._progress_dlg.close()
        # Docs folders chosen before the cache existed can be stored now
        for db_path, cp, _ in self._cache_jobs:
            if self._db_docs.get(db_path):
                cache_mod.set_docs_folder(cp, self._db_docs[db_path])
            if self._db_drives.get(db_path):
                cache_mod.set_docs_drive(cp, self._db_drives[db_path])
        self._open_caches()
        self._load_global()

    def _on_cache_error(self, msg: str):
        self._progress_dlg.close()
        self._status.showMessage(f"Cache build failed: {msg}", 0)

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    def _resolve_filepath(self, occ: query.DocOccurrence) -> Path:
        """Resolve document path: per-DB drive → stored path → per-DB docs folder
        → global fallback.  The drive is tried first so an unreachable UNC
        share is not probed when the user has mapped it to a drive letter."""
        drive = self._db_drives.get(occ.source_db, "")
        filepath = None
        if drive:
            filepath = Path(query.map_to_drive(occ.folderpath, drive)) / occ.filename
        if filepath is None or not filepath.exists():
            filepath = Path(occ.folderpath) / occ.filename
        if not filepath.exists():
            db_folder = self._db_docs.get(occ.source_db, "")
            if db_folder:
                filepath = Path(db_folder) / occ.filename
        if not filepath.exists() and self._docs_folder:
            filepath = Path(self._docs_folder) / occ.filename
        return filepath

    def _on_text_changed(self, text: str):
        if not text.strip():
            self._load_global()

    def _on_search(self):
        word = self._search.text().strip()
        if not word:
            self._load_global()
        else:
            self._start_search(word)

    def _on_left_click(self, word: str):
        self._search.setText(word)
        self._start_search(word)

    def _on_right_click(self, word: str):
        self._start_search(word, docs_only=True)

    def _on_doc_clicked(self, item: QListWidgetItem):
        occ: query.DocOccurrence = item.data(Qt.ItemDataRole.UserRole)
        filepath = self._resolve_filepath(occ)
        dlg = DocViewerDialog(str(filepath), occ.pagenumber, self._current_word, parent=self)
        dlg.exec()

    # ------------------------------------------------------------------
    # Data loading — always via cache once built
    # ------------------------------------------------------------------

    def _load_global(self):
        if not self._cache_conns:
            return
        self._status.showMessage("Loading…")
        rows = cache_mod.global_frequencies(self._cache_conns)
        words = [query.WordScore(r["word"], r["kind"], float(r["freq"])) for r in rows]
        self._update_clouds(words)
        names = ", ".join(Path(p).name for p in self._db_paths)
        self._status.showMessage(f"{names}  —  type a word and press Enter to search", 0)

    def _start_search(self, word: str, docs_only: bool = False):
        if not self._cache_conns:
            self._status.showMessage("Cache not ready yet.", 3000)
            return
        self._status.showMessage(f'Searching for "{word}"…')
        self._search_worker = _SearchWorker(
            self._db_paths, list(self._cache_paths.values()), word, docs_only=docs_only
        )
        self._search_worker.cooc_ready.connect(self._on_cooc_ready)
        self._search_worker.docs_ready.connect(self._on_docs_ready)
        self._search_worker.not_found.connect(
            lambda w: self._status.showMessage(f'"{w}" not found in database.', 4000)
        )
        self._search_worker.error.connect(
            lambda msg: self._status.showMessage(f"Search error: {msg}", 5000)
        )
        self._search_worker.start()

    def _on_cooc_ready(self, rows: list, word: str):
        words = [query.WordScore(r["tgt_word"], r["tgt_kind"], float(r["score"])) for r in rows]
        self._update_clouds(words)
        total = sum(r["score"] for r in rows)
        self._status.showMessage(
            f'"{word}" — {len(rows)} related words, total score {total:,.0f}', 0
        )

    def _on_docs_ready(self, occs: list, word: str):
        self._current_word = word
        self._doc_list.clear()
        seen: set[str] = set()
        for occ in occs:
            key = f"{occ.folderpath}/{occ.filename}"
            if key not in seen:
                seen.add(key)
                item = QListWidgetItem(f"{occ.filename}  (p.{occ.pagenumber})")
                item.setData(Qt.ItemDataRole.UserRole, occ)
                self._doc_list.addItem(item)
        self._doc_title.setText(f'"{word}" in {self._doc_list.count()} document(s)')

        # Reset sentence panel for the new search
        self._sent_list.clear()
        self._sent_title.setText("Sentences")
        self._sent_widget.setVisible(False)

        if not self._right_panel.isVisible():
            self._right_panel.setVisible(True)
            total = self._outer_splitter.width()
            self._outer_splitter.setSizes([total - 320, 320])

    def _on_doc_selected(self, item: QListWidgetItem):
        """Single-click on a document: show matching sentences in the right panel."""
        occ: query.DocOccurrence = item.data(Qt.ItemDataRole.UserRole)
        filepath = self._resolve_filepath(occ)
        self._current_filepath = str(filepath)
        self._sent_list.clear()

        if not filepath.exists():
            self._sent_title.setText("File not found")
            self._sent_widget.setVisible(True)
            return

        self._sent_title.setText(f'Loading sentences…')

        if not self._sent_widget.isVisible():
            self._sent_widget.setVisible(True)
            if not self._sent_ever_shown:
                self._sent_ever_shown = True
                total = self._outer_splitter.width()
                right_target = min(660, max(400, total - 600))
                self._outer_splitter.setSizes([total - right_target, right_target])
                self._inner_splitter.setSizes([280, right_target - 280])

        self._sentence_worker = _SentenceWorker(str(filepath), self._current_word)
        self._sentence_worker.ready.connect(self._on_sentences_ready)
        self._sentence_worker.error.connect(
            lambda msg: self._sent_title.setText(f"Error: {msg}")
        )
        self._sentence_worker.start()

    def _on_sentences_ready(self, sentences: list):
        self._sent_list.clear()
        self._sent_title.setText(
            f'"{self._current_word}" — {len(sentences)} sentence(s)'
        )
        for s, page in sentences:
            display = s if len(s) <= 300 else s[:300] + "…"
            item = QListWidgetItem(display)
            item.setData(Qt.ItemDataRole.UserRole, (s, page))
            self._sent_list.addItem(item)

    def _on_sentence_double_clicked(self, item: QListWidgetItem):
        data = item.data(Qt.ItemDataRole.UserRole)
        if not data or not self._current_filepath:
            return
        sentence, page = data
        filepath = self._current_filepath
        if not Path(filepath).exists():
            return
        dlg = DocViewerDialog(
            filepath, page or 1, self._current_word,
            sentence=sentence, parent=self,
        )
        dlg.exec()

    def _update_clouds(self, words: list[query.WordScore]):
        for cw in self._clouds.values():
            cw.update_words(words)
