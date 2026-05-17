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
    QPushButton,
)

import cache as cache_mod
import config as config_mod
import query
from cloud_widget import CloudWidget
from doc_viewer import DocViewerDialog

_CACHE_FILENAME = "findethedox.cache.db"

# Status of one source database relative to the cache file
_STATUS_DISPLAY = {
    "current":  ("✓", "#4ec94e"),   # green  — in cache, up to date
    "outdated": ("⚠", "#e5c07b"),   # yellow — in cache, new docs available
    "missing":  ("✗", "#e06c75"),   # red    — not in cache at all
    "no_cache": ("—", "#888888"),   # grey   — cache file doesn't exist yet
}


def _cache_db_status(cache_path: str, db_path: str) -> str:
    if not Path(cache_path).exists():
        return "no_cache"
    try:
        cc = sqlite3.connect(f"file:{cache_path}?mode=ro", uri=True)
        row = cc.execute(
            "SELECT value FROM meta WHERE key=?", (f"src:{db_path}",)
        ).fetchone()
        cc.close()
        if row is None:
            return "missing"
        last = int(row[0])
    except Exception:
        return "missing"
    try:
        sc = sqlite3.connect(db_path)
        row = sc.execute("SELECT MAX(fileID) FROM documents").fetchone()
        sc.close()
        cur = int(row[0]) if row and row[0] else 0
    except Exception:
        return "missing"
    return "outdated" if cur > last else "current"


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
    """Builds or updates the pre-computed score cache in the background."""
    progress = pyqtSignal(str, int, int)   # label, current, total
    done     = pyqtSignal()
    error    = pyqtSignal(str)

    def __init__(self, db_paths: list, cache_path: str, mode: str = "build"):
        super().__init__()
        self._db_paths   = db_paths
        self._cache_path = cache_path
        self._mode       = mode

    def run(self):
        fn = cache_mod.update if self._mode == "update" else cache_mod.build
        try:
            fn(
                self._db_paths, self._cache_path,
                progress=lambda lbl, cur, tot: self.progress.emit(lbl, cur, tot),
            )
            self.done.emit()
        except Exception as exc:
            self.error.emit(str(exc))


class _SearchWorker(QThread):
    """Runs co-occurrence lookup and document list queries in the background."""
    cooc_ready = pyqtSignal(list, str)   # (list of dicts, word)
    docs_ready = pyqtSignal(list, str)   # (list[DocOccurrence], word)
    not_found  = pyqtSignal(str)
    error      = pyqtSignal(str)

    def __init__(self, db_paths: list, cache_path: str, word: str, docs_only: bool = False):
        super().__init__()
        self._db_paths   = db_paths
        self._cache_path = cache_path
        self._word       = word
        self._docs_only  = docs_only

    def run(self):
        try:
            if not self._docs_only:
                cc = cache_mod.connect(self._cache_path)
                rows = [dict(r) for r in cache_mod.cooccurrences(cc, self._word)]
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
# Setup dialog — databases & cache
# ---------------------------------------------------------------------------

class SetupDialog(QDialog):
    """Configure databases, cache location, and per-database documents folders."""

    def __init__(self, db_paths: list[str], cache_path: str,
                 docs_folder: str = "", db_docs: dict[str, str] | None = None,
                 parent=None):
        super().__init__(parent)
        self.setWindowTitle("findethedox — Settings")
        self.resize(820, 560)

        self._db_paths: list[str] = list(db_paths)
        self._cache_path: str = cache_path
        self._cache_folder: str = str(Path(cache_path).parent)
        self._docs_folder: str = docs_folder
        self._worker: _CacheWorker | None = None

        # Per-DB docs folders: load from cache meta, then fill gaps from passed-in dict
        self._db_docs: dict[str, str] = cache_mod.get_docs_folders(self._cache_path)
        if db_docs:
            for k, v in db_docs.items():
                if k not in self._db_docs and v:
                    self._db_docs[k] = v

        self._build_ui()
        self._apply_dark_theme()
        self._refresh()

    # ------------------------------------------------------------------
    # Public accessors

    def result_db_paths(self) -> list[str]:
        return list(self._db_paths)

    def result_cache_path(self) -> str:
        return self._cache_path

    def result_cache_folder(self) -> str:
        return self._cache_folder

    def result_docs_folder(self) -> str:
        return self._docs_folder

    def result_db_docs(self) -> dict[str, str]:
        return {k: v for k, v in self._db_docs.items() if v}

    # ------------------------------------------------------------------
    # UI

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setSpacing(10)
        root.setContentsMargins(16, 16, 16, 16)

        # Cache folder row
        cache_row = QHBoxLayout()
        cache_row.addWidget(QLabel("Cache folder:"))
        self._cache_edit = QLineEdit()
        self._cache_edit.setReadOnly(True)
        cache_row.addWidget(self._cache_edit, 1)
        btn_browse = QPushButton("Browse…")
        btn_browse.clicked.connect(self._on_browse_cache)
        cache_row.addWidget(btn_browse)
        root.addLayout(cache_row)

        self._cache_file_lbl = QLabel()
        self._cache_file_lbl.setStyleSheet("color:#888;font-size:11px;margin-left:2px;")
        root.addWidget(self._cache_file_lbl)

        # Databases header
        db_hdr = QHBoxLayout()
        db_hdr.addWidget(QLabel("Databases:"))
        db_hdr.addStretch()
        self._add_btn = QPushButton("Add Database…")
        self._add_btn.clicked.connect(self._on_add_database)
        db_hdr.addWidget(self._add_btn)
        root.addLayout(db_hdr)

        # Database tree (two columns: path + docs folder)
        self._db_tree = QTreeWidget()
        self._db_tree.setColumnCount(2)
        self._db_tree.setHeaderLabels(["Database", "Documents Folder"])
        self._db_tree.header().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self._db_tree.header().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self._db_tree.setSelectionMode(QTreeWidget.SelectionMode.ExtendedSelection)
        self._db_tree.setRootIsDecorated(False)
        self._db_tree.itemDoubleClicked.connect(self._on_tree_double_clicked)
        root.addWidget(self._db_tree, 1)

        docs_hint = QLabel("Double-click the Documents Folder column to set a folder per database")
        docs_hint.setStyleSheet("color:#888;font-size:10px;")
        root.addWidget(docs_hint)

        # Remove row
        remove_row = QHBoxLayout()
        remove_row.addStretch()
        self._remove_btn = QPushButton("Remove Selected")
        self._remove_btn.clicked.connect(self._on_remove)
        remove_row.addWidget(self._remove_btn)
        root.addLayout(remove_row)

        # Status + cache update
        status_row = QHBoxLayout()
        self._status_lbl = QLabel()
        status_row.addWidget(self._status_lbl, 1)
        self._update_btn = QPushButton("Build Cache")
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

    def _refresh(self):
        self._cache_edit.setText(self._cache_folder)
        self._cache_file_lbl.setText(f"  {self._cache_path}")
        self._db_tree.clear()

        statuses: list[str] = []
        for db_path in self._db_paths:
            status = _cache_db_status(self._cache_path, db_path)
            statuses.append(status)
            icon, color = _STATUS_DISPLAY[status]
            docs = self._db_docs.get(db_path, "")
            docs_ok = Path(docs).is_dir() if docs else True

            item = QTreeWidgetItem([f"  {icon}  {db_path}", docs])
            item.setForeground(0, QColor(color))
            item.setForeground(1, QColor("#e06c75") if (docs and not docs_ok)
                               else QColor("#888888") if not docs
                               else QColor("#d4d4d4"))
            item.setData(0, Qt.ItemDataRole.UserRole, db_path)
            self._db_tree.addTopLevelItem(item)

        no_dbs      = not self._db_paths
        no_cache    = any(s == "no_cache"  for s in statuses)
        has_missing = any(s == "missing"   for s in statuses)
        has_old     = any(s == "outdated"  for s in statuses)

        if no_dbs:
            self._status_lbl.setText("No databases selected.")
            self._update_btn.setEnabled(False)
            self._update_btn.setText("Build Cache")
        elif no_cache:
            self._status_lbl.setText("Cache not built yet.")
            self._update_btn.setEnabled(True)
            self._update_btn.setText("Build Cache")
        elif has_missing or has_old:
            n_ok = statuses.count("current")
            self._status_lbl.setText(
                f"{n_ok} of {len(statuses)} database(s) up to date in cache."
            )
            self._update_btn.setEnabled(True)
            self._update_btn.setText("Update Cache")
        else:
            self._status_lbl.setText(
                f"All {len(statuses)} database(s) up to date in cache."
            )
            self._update_btn.setEnabled(False)
            self._update_btn.setText("Update Cache")

        self._apply_btn.setEnabled(not no_dbs)
        self._remove_btn.setEnabled(not no_dbs)

    # ------------------------------------------------------------------
    # Actions

    def _on_browse_cache(self):
        folder = QFileDialog.getExistingDirectory(
            self, "Select Cache Folder", self._cache_folder
        )
        if folder:
            self._cache_folder = folder
            self._cache_path = str(Path(folder) / _CACHE_FILENAME)
            self._refresh()

    def _on_tree_double_clicked(self, item: QTreeWidgetItem, column: int):
        if column != 1:
            return
        db_path = item.data(0, Qt.ItemDataRole.UserRole)
        current = self._db_docs.get(db_path, "")
        folder = QFileDialog.getExistingDirectory(
            self, f"Documents folder for {Path(db_path).name}",
            current or str(Path.home()),
        )
        if folder:
            self._db_docs[db_path] = folder
            self._refresh()

    def _on_add_database(self):
        start = (
            str(Path(self._db_paths[-1]).parent)
            if self._db_paths else str(Path.home())
        )
        chosen, _ = QFileDialog.getOpenFileName(
            self, "Add allmydox Database", start,
            "SQLite databases (*.db);;All files (*)",
        )
        if chosen and chosen not in self._db_paths:
            self._db_paths.append(chosen)
            # If the configured cache doesn't exist yet, look for a legacy
            # cache next to the database (e.g. sv25.cache.db beside sv25.db)
            if not Path(self._cache_path).exists():
                legacy = cache_mod.default_cache_path([chosen])
                if Path(legacy).exists():
                    self._cache_path = legacy
                    self._cache_folder = str(Path(legacy).parent)
            self._refresh()

    def _on_remove(self):
        to_remove = {
            item.data(0, Qt.ItemDataRole.UserRole)
            for item in self._db_tree.selectedItems()
        }
        self._db_paths = [p for p in self._db_paths if p not in to_remove]
        self._refresh()

    def _on_update_cache(self):
        cp = self.result_cache_path()
        mode = "update" if Path(cp).exists() else "build"

        dlg = QProgressDialog("Preparing…", None, 0, 100, self)
        dlg.setWindowTitle("findethedox — Building cache")
        dlg.setWindowModality(Qt.WindowModality.WindowModal)
        dlg.setMinimumWidth(520)
        dlg.setValue(0)
        dlg.show()

        self._update_btn.setEnabled(False)
        self._add_btn.setEnabled(False)

        self._worker = _CacheWorker(self._db_paths, cp, mode)

        def on_progress(lbl: str, cur: int, tot: int):
            dlg.setLabelText(f"Step {cur}/{tot}: {lbl}")
            dlg.setValue(int(cur / tot * 100) if tot else 0)

        def on_done():
            dlg.close()
            self._add_btn.setEnabled(True)
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
        docs_folder: str | None = None,
        cache_path: str | None = None,
        db_docs: dict[str, str] | None = None,
    ):
        super().__init__()
        self.resize(1280, 760)

        if isinstance(db_paths, str):
            db_paths = [db_paths]
        self._db_paths    = db_paths
        self._docs_folder = docs_folder
        self._cache_path  = cache_path or cache_mod.default_cache_path(db_paths)
        self._db_docs: dict[str, str] = db_docs or {}
        self._cache_conn  = None                           # set after cache is ready
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
        new_win = MainWindow([chosen], docs_folder=self._docs_folder)
        new_win.show()
        self.close()

    def _on_setup_dialog(self):
        dlg = SetupDialog(
            self._db_paths, self._cache_path,
            docs_folder=self._docs_folder or "",
            db_docs=self._db_docs,
            parent=self,
        )
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        new_db_paths    = dlg.result_db_paths()
        new_cache_path  = dlg.result_cache_path()
        new_docs_folder = dlg.result_docs_folder() or None
        new_db_docs     = dlg.result_db_docs()
        if self._cache_conn:
            self._cache_conn.close()
            self._cache_conn = None
        # Persist per-DB docs folders into the cache meta table
        for db_path, folder in new_db_docs.items():
            cache_mod.set_docs_folder(new_cache_path, db_path, folder)
        cfg = config_mod.load()
        cfg["db_paths"]     = new_db_paths
        cfg["cache_path"]   = new_cache_path
        cfg["cache_folder"] = dlg.result_cache_folder()
        cfg["docs_folder"]  = new_docs_folder
        cfg["db_docs"]      = new_db_docs
        config_mod.save(cfg)
        new_win = MainWindow(new_db_paths, docs_folder=new_docs_folder,
                             cache_path=new_cache_path, db_docs=new_db_docs)
        new_win.show()
        self.close()

    def _on_rebuild_cache(self):
        reply = QMessageBox.question(
            self, "Rebuild Cache",
            "This will delete the existing cache and rebuild it from scratch.\n"
            "The operation may take several minutes.\n\nContinue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return
        if self._cache_conn:
            self._cache_conn.close()
            self._cache_conn = None
        self._start_cache_worker("build")

    def _save_config(self):
        cfg = config_mod.load()
        cfg["db_paths"]    = self._db_paths
        cfg["docs_folder"] = self._docs_folder
        cfg["cache_path"]  = self._cache_path
        config_mod.save(cfg)

    # ------------------------------------------------------------------
    # Startup sequence: indexes → cache → show clouds
    # ------------------------------------------------------------------

    def _on_indexes_ready(self):
        if cache_mod.is_built(self._cache_path):
            # Cache exists — use it as-is; user can rebuild via the toolbar button
            if not self._db_docs:
                self._db_docs = cache_mod.get_docs_folders(self._cache_path)
            self._cache_conn = cache_mod.connect(self._cache_path)
            self._load_global()
        else:
            self._start_cache_worker("build")

    def _start_cache_worker(self, mode: str):
        if mode == "update":
            title = "findethedox — cache update"
            label = "Updating search cache with new documents…"
        else:
            title = "findethedox — first launch"
            label = "Building search cache — this runs once and takes a few minutes."

        self._progress_dlg = QProgressDialog(label, None, 0, 100, self)
        self._progress_dlg.setWindowTitle(title)
        self._progress_dlg.setWindowModality(Qt.WindowModality.WindowModal)
        self._progress_dlg.setMinimumWidth(500)
        self._progress_dlg.setValue(0)
        self._progress_dlg.show()

        self._cache_worker = _CacheWorker(self._db_paths, self._cache_path, mode=mode)
        self._cache_worker.progress.connect(self._on_cache_progress)
        self._cache_worker.done.connect(self._on_cache_done)
        self._cache_worker.error.connect(self._on_cache_error)
        self._cache_worker.start()

    def _on_cache_progress(self, label: str, current: int, total: int):
        pct = int(current / total * 100) if total else 0
        self._progress_dlg.setLabelText(f"Step {current}/{total}: {label}")
        self._progress_dlg.setValue(pct)

    def _on_cache_done(self):
        self._progress_dlg.close()
        self._cache_conn = cache_mod.connect(self._cache_path)
        self._load_global()

    def _on_cache_error(self, msg: str):
        self._progress_dlg.close()
        self._status.showMessage(f"Cache build failed: {msg}", 0)

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    def _resolve_filepath(self, occ: query.DocOccurrence) -> Path:
        """Resolve document path: stored path → per-DB docs folder → global fallback."""
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
        if self._cache_conn is None:
            return
        self._status.showMessage("Loading…")
        rows = cache_mod.global_frequencies(self._cache_conn)
        words = [query.WordScore(r["word"], r["kind"], float(r["freq"])) for r in rows]
        self._update_clouds(words)
        names = ", ".join(Path(p).name for p in self._db_paths)
        self._status.showMessage(f"{names}  —  type a word and press Enter to search", 0)

    def _start_search(self, word: str, docs_only: bool = False):
        if self._cache_conn is None:
            self._status.showMessage("Cache not ready yet.", 3000)
            return
        self._status.showMessage(f'Searching for "{word}"…')
        self._search_worker = _SearchWorker(
            self._db_paths, self._cache_path, word, docs_only=docs_only
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
