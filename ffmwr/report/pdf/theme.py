from dataclasses import dataclass
from reportlab.lib import colors


@dataclass
class PdfTheme:
    """Declarative theme for PDF styling.

    Centralizes common look-and-feel so tables, titles, and charts stay consistent.
    """

    # Spacing
    padding_top: float = 3.0
    padding_bottom: float = 2.0
    padding_left: float = 2.0
    padding_right: float = 2.0

    # Grid/lines
    grid_thin: float = 0.25
    grid_thick: float = 1.25

    # Colors
    header_bg = colors.HexColor("#e9ecef")
    row_alt_a = colors.white
    row_alt_b = colors.whitesmoke
    grid_color = colors.HexColor("#d0d4d9")
    grid_outer = colors.HexColor("#aeb4bb")

    # Typography
    header_text_color = colors.black
    body_text_color = colors.black

    # Layout
    table_max_width_in = 7.75  # inches usable width for tables

