import math
import pandas as pd


# Percentile color: red(0) → yellow(50) → green(99)
def _pct_bg(val: float) -> str:
    hue = val * 120 / 99  # 0→0 red, 99→120 green
    lum = 40
    text = "white" if (val < 28 or val > 72) else "#111"
    return f"background:hsl({hue:.0f},65%,{lum}%);color:{text}"


# Score gradient: relative within table (min=red, max=green)
def _score_bg(val: float, lo: float, hi: float) -> str:
    if hi == lo:
        ratio = 0.5
    else:
        ratio = (val - lo) / (hi - lo)
    hue = ratio * 110  # 0=red, 110=green
    text = "white" if ratio < 0.3 or ratio > 0.7 else "#111"
    return f"background:hsl({hue:.0f},60%,42%);color:{text}"


def df_to_html(
    df: pd.DataFrame,
    pct_cols: list = None,       # 0-99 percentile → color by absolute value
    score_cols: list = None,     # score → relative gradient
    badge_map: dict = None,      # {col: {value: badge_html}} for badges
    pct_fmt_cols: list = None,   # format as "25.1%"
    float_cols: list = None,     # format as ".3f"
    int_cols: list = None,       # format as int
    table_id: str = "",
) -> str:
    if df is None or df.empty:
        return "<p class='text-muted'>No data.</p>"

    pct_cols    = [c for c in (pct_cols    or []) if c in df.columns]
    score_cols  = [c for c in (score_cols  or []) if c in df.columns]
    pct_fmt_cols= [c for c in (pct_fmt_cols or []) if c in df.columns]
    float_cols  = [c for c in (float_cols  or []) if c in df.columns]
    int_cols    = [c for c in (int_cols    or []) if c in df.columns]

    # Pre-compute score ranges
    ranges = {}
    for col in score_cols:
        lo = df[col].min()
        hi = df[col].max()
        ranges[col] = (lo, hi)

    id_attr = f' id="{table_id}"' if table_id else ""
    html = [f'<table{id_attr} class="table table-sm table-hover table-bordered fantasy-table">']

    # Header
    html.append("<thead class='table-dark'><tr>")
    for col in df.columns:
        html.append(f"<th>{col}</th>")
    html.append("</tr></thead>")

    # Body
    html.append("<tbody>")
    for _, row in df.iterrows():
        row_class = " class=\"mine-row\"" if row.get("Source") == "MINE" else ""
        html.append(f"<tr{row_class}>")
        for col in df.columns:
            val = row[col]
            style = ""
            cell_val = val

            if col in pct_cols:
                try:
                    fval = float(str(val).rstrip("%"))
                    if not math.isnan(fval):
                        style = _pct_bg(fval)
                        cell_val = str(int(round(fval)))
                    else:
                        cell_val = "—"
                except (TypeError, ValueError):
                    cell_val = "—"
            elif col in score_cols:
                try:
                    fval = float(val)
                    lo, hi = ranges[col]
                    style = _score_bg(fval, lo, hi)
                    cell_val = f"{fval:.3f}"
                except (TypeError, ValueError):
                    pass
            elif col in pct_fmt_cols:
                try:
                    cell_val = f"{float(val):.1%}" if pd.notna(val) else "—"
                except (TypeError, ValueError):
                    pass
            elif col in float_cols:
                try:
                    cell_val = f"{float(val):.3f}" if pd.notna(val) else "—"
                except (TypeError, ValueError):
                    pass
            elif col in int_cols:
                try:
                    cell_val = str(int(val)) if pd.notna(val) else "—"
                except (TypeError, ValueError):
                    pass

            # Handle badges
            if badge_map and col in badge_map:
                bmap = badge_map[col]
                if str(cell_val) in bmap:
                    cell_val = bmap[str(cell_val)]

            style_attr = f' style="{style}"' if style else ""
            # Store raw numeric value for accurate JS sorting
            try:
                raw = float(val) if pd.notna(val) else None
                data_val = f' data-val="{raw}"' if raw is not None else ""
            except (TypeError, ValueError):
                data_val = ""
            html.append(f'<td class="text-nowrap"{style_attr}{data_val}>{cell_val}</td>')
        html.append("</tr>")
    html.append("</tbody></table>")
    return "\n".join(html)
