"""Main application window for findethedox."""
from __future__ import annotations

from pathlib import Path

from PyQt6.QtCore import Qt, QTimer, QThread, pyqtSignal
from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QHBoxLayout, QVBoxLayout,
    QLineEdit, QLabel, QListWidget, QListWidgetItem,
    QSplitter, QStatusBar,
)

import query
from cloud_widget import CloudWidget
from doc_viewer import DocViewerDialog


class _IndexWorker(QThread):
    """Builds missing DB indexes in the background on first launch."""
    done = pyqtSignal()

    def __init__(self, db_path: str):
        super().__init__()
        self._db_path = db_path

    def run(self):
        conn = query.connect(self._db_path)
        query.ensure_indexes(conn)
        conn.close()
        self.done.emit()


class MainWindow(QMainWindow):

    def __init__(self, db_path: str):
        super().__init__()
        self.setWindowTitle("findethedox")
        self.resize(1280, 760)
        self._db_path = db_path
        self._conn = query.connect(db_path)
        self._current_word: str = ""
        self._doc_occurrences: list[query.DocOccurrence] = []

        self._build_ui()
        self._apply_dark_theme()

        # Build missing co-occurrence indexes in background, then load words
        self._index_worker = _IndexWorker(db_path)
        self._index_worker.done.connect(self._on_indexes_ready)
        self._status.showMessage("Preparing database indexes (first launch only)…")
        self._index_worker.start()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QHBoxLayout(central)
        root.setContentsMargins(8, 8, 8, 8)

        # Left side: search + clouds
        left = QWidget()
        left_layout = QVBoxLayout(left)
        left_layout.setSpacing(6)

        # Search bar
        search_row = QHBoxLayout()
        search_row.addWidget(QLabel("Search:"))
        self._search = QLineEdit()
        self._search.setPlaceholderText("Type a noun, name, or verb…")
        self._search.returnPressed.connect(self._on_search)
        self._search.textChanged.connect(self._on_text_changed)
        search_row.addWidget(self._search)
        left_layout.addLayout(search_row)

        # Three word clouds side by side
        clouds_row = QHBoxLayout()
        self._clouds: dict[str, CloudWidget] = {}
        for kind in ("name", "noun", "verb"):
            cw = CloudWidget(kind, self._on_left_click, self._on_right_click)
            self._clouds[kind] = cw
            clouds_row.addWidget(cw)
        left_layout.addLayout(clouds_row)

        # Right side: document panel (hidden until right-click)
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

    def _apply_dark_theme(self):
        self.setStyleSheet("""
            QMainWindow, QWidget { background:#1e1e1e; color:#d4d4d4; }
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
        """)

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
            return
        self._load_cooccurrences(word)

    def _on_left_click(self, word: str):
        self._search.setText(word)
        self._load_cooccurrences(word)

    def _on_right_click(self, word: str):
        self._current_word = word
        self._doc_occurrences = query.document_occurrences(self._conn, word)
        self._doc_list.clear()

        seen_files: dict[str, int] = {}
        for occ in self._doc_occurrences:
            key = f"{occ.folderpath}/{occ.filename}"
            if key not in seen_files:
                seen_files[key] = occ.pagenumber
                item = QListWidgetItem(f"{occ.filename}  (p.{occ.pagenumber})")
                item.setData(Qt.ItemDataRole.UserRole, occ)
                self._doc_list.addItem(item)

        self._doc_title.setText(f'"{word}" in {self._doc_list.count()} document(s)')
        self._right_panel.setVisible(True)
        self._status.showMessage(f'Right-clicked "{word}" — {len(self._doc_occurrences)} occurrence(s)', 4000)

    def _on_doc_clicked(self, item: QListWidgetItem):
        occ: query.DocOccurrence = item.data(Qt.ItemDataRole.UserRole)
        filepath = str(Path(occ.folderpath) / occ.filename)
        dlg = DocViewerDialog(filepath, occ.pagenumber, self._current_word, self)
        dlg.exec()

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _on_indexes_ready(self):
        self._load_global()

    def _load_global(self):
        self._status.showMessage("Loading global word frequencies…")
        words = query.global_frequencies(self._conn)
        self._update_clouds(words)
        self._status.showMessage(
            f"Showing top words from {self._db_path}  —  "
            "type a word and press Enter to search", 0
        )

    def _load_cooccurrences(self, word: str):
        self._status.showMessage(f'Searching for co-occurrences of "{word}"…')
        words = query.cooccurrences(self._conn, word)
        if not words:
            self._status.showMessage(f'"{word}" not found in database.', 4000)
            return
        self._update_clouds(words)
        total = sum(w.score for w in words)
        self._status.showMessage(
            f'"{word}" — {len(words)} related words, total score {total:,.0f}', 0
        )

    def _update_clouds(self, words: list[query.WordScore]):
        for kind, cw in self._clouds.items():
            cw.update_words(words)
