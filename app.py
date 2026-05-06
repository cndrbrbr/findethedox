"""Main application window for findethedox."""
from __future__ import annotations

from pathlib import Path

from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QHBoxLayout, QVBoxLayout,
    QLineEdit, QLabel, QListWidget, QListWidgetItem,
    QSplitter, QStatusBar, QProgressDialog, QFileDialog,
)

import cache as cache_mod
import query
from cloud_widget import CloudWidget
from doc_viewer import DocViewerDialog


# ---------------------------------------------------------------------------
# Background workers
# ---------------------------------------------------------------------------

class _IndexWorker(QThread):
    """Ensures allmydox co-occurrence indexes exist (one-time, fast after first run)."""
    done = pyqtSignal()

    def __init__(self, db_path: str):
        super().__init__()
        self._db_path = db_path

    def run(self):
        conn = query.connect(self._db_path)
        query.ensure_indexes(conn)
        conn.close()
        self.done.emit()


class _CacheWorker(QThread):
    """Builds or updates the pre-computed score cache in the background."""
    progress = pyqtSignal(str, int, int)   # label, current, total
    done     = pyqtSignal()
    error    = pyqtSignal(str)

    def __init__(self, db_path: str, cache_path: str, mode: str = "build"):
        super().__init__()
        self._db_path    = db_path
        self._cache_path = cache_path
        self._mode       = mode

    def run(self):
        fn = cache_mod.update if self._mode == "update" else cache_mod.build
        try:
            fn(
                self._db_path, self._cache_path,
                progress=lambda lbl, cur, tot: self.progress.emit(lbl, cur, tot),
            )
            self.done.emit()
        except Exception as exc:
            self.error.emit(str(exc))


# ---------------------------------------------------------------------------
# Main window
# ---------------------------------------------------------------------------

class MainWindow(QMainWindow):

    def __init__(self, db_path: str, docs_folder: str | None = None):
        super().__init__()
        self.setWindowTitle("findethedox")
        self.resize(1280, 760)

        self._db_path     = db_path
        self._docs_folder = docs_folder
        self._cache_path  = cache_mod.default_cache_path(db_path)
        self._conn        = query.connect(db_path)         # for document lookups
        self._cache_conn  = None                           # set after cache is ready
        self._current_word: str = ""

        self._build_ui()
        self._build_menu()
        self._apply_dark_theme()

        # Step 1: ensure DB indexes exist (fast, only meaningful on first launch)
        self._index_worker = _IndexWorker(db_path)
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
        right.setFixedWidth(320)
        right_layout = QVBoxLayout(right)
        right_layout.setSpacing(4)

        self._doc_title = QLabel("Documents")
        self._doc_title.setStyleSheet("font-weight:bold;font-size:13px;")
        right_layout.addWidget(self._doc_title)

        self._doc_list = QListWidget()
        self._doc_list.itemDoubleClicked.connect(self._on_doc_clicked)
        right_layout.addWidget(self._doc_list)

        hint = QLabel("Double-click to open")
        hint.setStyleSheet("color:#888;font-size:10px;")
        right_layout.addWidget(hint)

        self._right_panel = right
        self._right_panel.setVisible(False)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.addWidget(left)
        splitter.addWidget(right)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 0)
        root.addWidget(splitter)

        self._status = QStatusBar()
        self.setStatusBar(self._status)

    def _build_menu(self):
        file_menu = self.menuBar().addMenu("File")

        open_db = file_menu.addAction("Open Database…")
        open_db.setShortcut("Ctrl+O")
        open_db.triggered.connect(self._on_open_database)

        set_docs = file_menu.addAction("Set Documents Folder…")
        set_docs.triggered.connect(self._on_set_docs_folder)

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
            self, "Open allmydox database", str(Path(self._db_path).parent),
            "SQLite databases (*.db);;All files (*)",
        )
        if not chosen:
            return
        new_win = MainWindow(chosen, docs_folder=self._docs_folder)
        new_win.show()
        self.close()

    def _on_set_docs_folder(self):
        start = self._docs_folder or str(Path(self._db_path).parent)
        folder = QFileDialog.getExistingDirectory(self, "Select documents folder", start)
        if folder:
            self._docs_folder = folder
            self._status.showMessage(f"Documents folder: {folder}", 5000)

    # ------------------------------------------------------------------
    # Startup sequence: indexes → cache → show clouds
    # ------------------------------------------------------------------

    def _on_indexes_ready(self):
        if cache_mod.is_built(self._cache_path):
            if cache_mod.needs_update(self._cache_path, self._db_path):
                self._start_cache_worker("update")
            else:
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

        self._cache_worker = _CacheWorker(self._db_path, self._cache_path, mode=mode)
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

    def _on_text_changed(self, text: str):
        if not text.strip():
            self._load_global()

    def _on_search(self):
        word = self._search.text().strip()
        if not word:
            self._load_global()
        else:
            self._load_cooccurrences(word)
            self._show_documents(word)

    def _on_left_click(self, word: str):
        self._search.setText(word)
        self._load_cooccurrences(word)
        self._show_documents(word)

    def _on_right_click(self, word: str):
        self._show_documents(word)

    def _on_doc_clicked(self, item: QListWidgetItem):
        occ: query.DocOccurrence = item.data(Qt.ItemDataRole.UserRole)
        filepath = Path(occ.folderpath) / occ.filename
        if not filepath.exists() and self._docs_folder:
            filepath = Path(self._docs_folder) / occ.filename
        dlg = DocViewerDialog(str(filepath), occ.pagenumber, self._current_word, self)
        dlg.exec()

    # ------------------------------------------------------------------
    # Data loading — always via cache once built
    # ------------------------------------------------------------------

    def _load_global(self):
        if self._cache_conn is None:
            return
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        QApplication.processEvents()
        try:
            self._status.showMessage("Loading…")
            rows = cache_mod.global_frequencies(self._cache_conn)
            words = [query.WordScore(r["word"], r["kind"], float(r["freq"])) for r in rows]
            self._update_clouds(words)
            self._status.showMessage(
                f"{self._db_path}  —  type a word and press Enter to search", 0
            )
        finally:
            QApplication.restoreOverrideCursor()

    def _load_cooccurrences(self, word: str):
        if self._cache_conn is None:
            self._status.showMessage("Cache not ready yet.", 3000)
            return
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        QApplication.processEvents()
        try:
            self._status.showMessage(f'Searching for "{word}"…')
            rows = cache_mod.cooccurrences(self._cache_conn, word)
            if not rows:
                self._status.showMessage(f'"{word}" not found in database.', 4000)
                return
            words = [query.WordScore(r["tgt_word"], r["tgt_kind"], float(r["score"])) for r in rows]
            self._update_clouds(words)
            total = sum(r["score"] for r in rows)
            self._status.showMessage(
                f'"{word}" — {len(rows)} related words, total score {total:,.0f}', 0
            )
        finally:
            QApplication.restoreOverrideCursor()

    def _show_documents(self, word: str):
        self._current_word = word
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        QApplication.processEvents()
        try:
            occs = query.document_occurrences(self._conn, word)
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
            self._right_panel.setVisible(True)
        finally:
            QApplication.restoreOverrideCursor()

    def _update_clouds(self, words: list[query.WordScore]):
        for cw in self._clouds.values():
            cw.update_words(words)
