"""Superfences custom fence rendering ANSI escape sequences (SGR) as colored HTML.

Usage in `zensical.toml`:

    pymdownx.superfences = {
      custom_fences = [
        { name = "ansi", class = "ansi", format = "sirocco_ansi_fence.fence_ansi_format" },
      ],
    }

and in Markdown:

    ``` { .ansi title="sirocco.log" }
    --8<-- "assets/sirocco.log"
    ```
"""

from __future__ import annotations

import html
import re
from typing import Any

SGR_RE = re.compile(r"\x1b\[([0-9;]*)m")
# Other escape sequences (cursor movements, ...) are dropped
OTHER_ESC_RE = re.compile(r"\x1b(\[[0-9;?]*[A-Za-z]|\][^\x07]*\x07|[@-Z\\-_])")

# Standard and bright colors 0-15, xterm defaults
BASIC_COLORS = [
    (0, 0, 0),
    (205, 49, 49),
    (13, 188, 121),
    (229, 229, 16),
    (36, 114, 200),
    (188, 63, 188),
    (17, 168, 205),
    (229, 229, 229),
    (102, 102, 102),
    (241, 76, 76),
    (35, 209, 139),
    (245, 245, 67),
    (59, 142, 234),
    (214, 112, 214),
    (41, 184, 219),
    (255, 255, 255),
]


def _color_256(n: int) -> tuple[int, int, int]:
    if n < 16:
        return BASIC_COLORS[n]
    if n < 232:
        n -= 16
        levels = (0, 95, 135, 175, 215, 255)
        return levels[n // 36], levels[(n // 6) % 6], levels[n % 6]
    v = 8 + (n - 232) * 10
    return v, v, v


class _State:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.fg: tuple[int, int, int] | None = None
        self.bg: tuple[int, int, int] | None = None
        self.bold = False
        self.dim = False
        self.italic = False
        self.underline = False

    def apply(self, params: str) -> None:
        codes = [int(c) if c else 0 for c in params.split(";")] if params else [0]
        i = 0
        while i < len(codes):
            c = codes[i]
            if c == 0:
                self.reset()
            elif c == 1:
                self.bold = True
            elif c == 2:
                self.dim = True
            elif c == 3:
                self.italic = True
            elif c == 4:
                self.underline = True
            elif c == 22:
                self.bold = self.dim = False
            elif c == 23:
                self.italic = False
            elif c == 24:
                self.underline = False
            elif c in (38, 48):
                color = None
                if i + 2 < len(codes) and codes[i + 1] == 5:
                    color = _color_256(codes[i + 2])
                    i += 2
                elif i + 4 < len(codes) and codes[i + 1] == 2:
                    color = (codes[i + 2], codes[i + 3], codes[i + 4])
                    i += 4
                if c == 38:
                    self.fg = color
                else:
                    self.bg = color
            elif c == 39:
                self.fg = None
            elif c == 49:
                self.bg = None
            elif 30 <= c <= 37:
                self.fg = BASIC_COLORS[c - 30]
            elif 40 <= c <= 47:
                self.bg = BASIC_COLORS[c - 40]
            elif 90 <= c <= 97:
                self.fg = BASIC_COLORS[c - 90 + 8]
            elif 100 <= c <= 107:
                self.bg = BASIC_COLORS[c - 100 + 8]
            i += 1

    def style(self) -> str:
        styles = []
        if self.fg is not None:
            styles.append("color: rgb({},{},{})".format(*self.fg))
        if self.bg is not None:
            styles.append("background-color: rgb({},{},{})".format(*self.bg))
        if self.bold:
            styles.append("font-weight: bold")
        if self.dim:
            styles.append("opacity: 0.7")
        if self.italic:
            styles.append("font-style: italic")
        if self.underline:
            styles.append("text-decoration: underline")
        return "; ".join(styles)


def ansi_to_html(source: str) -> str:
    """Convert text with ANSI SGR escape sequences to HTML spans with inline styles."""
    state = _State()
    out = []
    pos = 0
    for match in SGR_RE.finditer(source):
        out.append(_span(source[pos : match.start()], state.style()))
        state.apply(match.group(1))
        pos = match.end()
    out.append(_span(source[pos:], state.style()))
    return "".join(out)


def _span(text: str, style: str) -> str:
    text = html.escape(OTHER_ESC_RE.sub("", text))
    if not text or not style:
        return text
    return f'<span style="{style}">{text}</span>'


def fence_ansi_format(
    source: str,
    _language: str,
    class_name: str,
    _options: dict[str, Any],
    _md: Any,
    **kwargs: Any,
) -> str:
    """Superfences formatter producing the same structure as highlighted code blocks."""
    classes = [f"language-{class_name}", "highlight", *kwargs.get("classes", [])]
    attrs = dict(kwargs.get("attrs", {}))
    title = attrs.pop("title", None)
    id_value = kwargs.get("id_value", "")

    id_attr = f' id="{html.escape(id_value)}"' if id_value else ""
    extra_attrs = "".join(f' {k}="{html.escape(str(v))}"' for k, v in attrs.items())
    filename = f'<span class="filename">{html.escape(title)}</span>' if title else ""
    return (
        f'<div{id_attr} class="{" ".join(classes)}"{extra_attrs}>{filename}'
        f"<pre><code>{ansi_to_html(source)}</code></pre></div>"
    )
