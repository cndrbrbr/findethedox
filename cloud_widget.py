"""Interactive word cloud widget backed by PyQt6."""
from __future__ import annotations

import sys
from typing import Callable

import matplotlib
matplotlib.use("QtAgg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg
from matplotlib.patches import FancyBboxPatch
from wordcloud import WordCloud
from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtWidgets import QWidget, QVBoxLayout, QLabel, QSizePolicy

from query import WordScore

# Colour per word type
_COLOURS = {
    "name": "#2E86AB",
    "noun": "#A23B72",
    "verb": "#F18F01",
}
_TITLE = {"name": "Names", "noun": "Nouns", "verb": "Verbs"}


def _system_font() -> str:
    """Return a readable font path that exists on both Linux and Windows."""
    for family in ("DejaVu Sans", "Arial", "Helvetica", "Liberation Sans"):
        path = fm.findfont(fm.FontProperties(family=family), fallback_to_default=False)
        if path:
            return path
    return fm.findfont(fm.FontProperties())


_FONT_PATH = _system_font()


class CloudWidget(QWidget):
    """
    Displays one interactive word cloud for a given word kind.
    Callbacks:
      on_left_click(word)  — re-centre / new search
      on_right_click(word) — show document list
    """

    def __init__(
        self,
        kind: str,
        on_left_click: Callable[[str], None],
        on_right_click: Callable[[str], None],
        parent=None,
    ):
        super().__init__(parent)
        self.kind = kind
        self._on_left = on_left_click
        self._on_right = on_right_click
        self._word_boxes: list[tuple[str, float, float, float, float]] = []
        self._last_words: list[WordScore] = []
        self._resize_timer = QTimer(self)
        self._resize_timer.setSingleShot(True)
        self._resize_timer.timeout.connect(self._on_resize_timeout)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 4, 4, 4)

        title = QLabel(_TITLE[kind])
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title.setStyleSheet(
            f"font-weight:bold;font-size:13px;color:{_COLOURS[kind]};"
        )
        layout.addWidget(title)

        self._fig, self._ax = plt.subplots(figsize=(5, 3.2))
        self._fig.patch.set_facecolor("#1e1e1e")
        self._ax.set_facecolor("#1e1e1e")
        self._ax.axis("off")

        self._canvas = FigureCanvasQTAgg(self._fig)
        self._canvas.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding
        )
        self._canvas.mpl_connect("button_press_event", self._on_click)
        layout.addWidget(self._canvas)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if self._last_words:
            self._resize_timer.start(150)

    def _on_resize_timeout(self):
        self.update_words(self._last_words)

    def update_words(self, words: list[WordScore]):
        """Render a new set of words. Pass empty list to clear."""
        self._last_words = words
        self._ax.cla()
        self._ax.set_facecolor("#1e1e1e")
        self._ax.axis("off")
        self._word_boxes.clear()

        freqs = {w.word: max(w.score, 1.0) for w in words if w.kind == self.kind}
        if not freqs:
            self._canvas.draw()
            return

        w = max(200, self._canvas.width())
        h = max(120, self._canvas.height())
        max_words = max(10, min(250, (w * h) // 3000))

        # Update figure size to match canvas so font sizes stay proportional
        dpi = self._fig.dpi
        self._fig.set_size_inches(w / dpi, h / dpi, forward=False)

        wc = WordCloud(
            font_path=_FONT_PATH,
            background_color=None,
            mode="RGBA",
            color_func=lambda *a, **kw: _COLOURS[self.kind],
            max_words=max_words,
            width=w,
            height=h,
            prefer_horizontal=0.85,
        ).generate_from_frequencies(freqs)

        # Render each word as a matplotlib text object so we can hit-test clicks
        ax = self._ax
        ax.set_xlim(0, wc.width)
        ax.set_ylim(0, wc.height)

        for (word, _), font_size, (row, col), orient, colour in wc.layout_:
            x, y = col, wc.height - row
            rot = 90 if orient else 0
            txt = ax.text(
                x, y, word,
                fontsize=font_size * 0.72,
                color=colour,
                rotation=rot,
                va="top" if not rot else "center",
                ha="left" if not rot else "center",
            )
            # Store bounding box in data coordinates after draw
            self._word_boxes.append((word, x, y, font_size, rot))

        self._canvas.draw()

    def _on_click(self, event):
        if event.inaxes != self._ax or event.xdata is None:
            return
        word = self._hit_word(event.xdata, event.ydata)
        if word is None:
            return
        if event.button == 1:
            self._on_left(word)
        elif event.button == 3:
            self._on_right(word)

    def _hit_word(self, ex: float, ey: float) -> str | None:
        """Return the word whose bounding box contains (ex, ey), or None."""
        renderer = self._canvas.get_renderer()
        for txt in self._ax.texts:
            try:
                bb = txt.get_window_extent(renderer=renderer)
                # Convert display coords to data coords
                inv = self._ax.transData.inverted()
                x0, y0 = inv.transform((bb.x0, bb.y0))
                x1, y1 = inv.transform((bb.x1, bb.y1))
                if min(x0, x1) <= ex <= max(x0, x1) and min(y0, y1) <= ey <= max(y0, y1):
                    return txt.get_text()
            except Exception:
                continue
        return None
