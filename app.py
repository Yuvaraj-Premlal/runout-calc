import io
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st


# ----------------------------
# Config / Business Rules
# ----------------------------
HORIZON_WEEKS_DEFAULT = 12
GOOD_STATUS_VALUE = "good"

DEMAND_REQUIRED_COLS = ["Item", "Quantity", "Aerostar Ship Week"]
INV_REQUIRED_COLS = ["Item", "Available", "Status"]


# ----------------------------
# ISO Week helpers (Friday week-ending)
# ----------------------------
def iso_week_friday(iso_year: int, iso_week: int) -> date:
    """
    Returns the Friday date for ISO year/week.
    ISO week starts Monday. Friday = Monday + 4 days.
    """
    # ISO week 1 contains Jan 4
    jan4 = date(iso_year, 1, 4)
    monday_week1 = jan4 - timedelta(days=jan4.isoweekday() - 1)  # Monday
    monday_target = monday_week1 + timedelta(weeks=iso_week - 1)
    friday = monday_target + timedelta(days=4)
    return friday


def current_iso_year_week(d: date) -> Tuple[int, int]:
    iso = d.isocalendar()
    return int(iso[0]), int(iso[1])


def add_iso_weeks(iso_year: int, iso_week: int, add: int) -> Tuple[int, int]:
    # Move by adding weeks to the Friday of the current week, then re-derive ISO week/year
    base_friday = iso_week_friday(iso_year, iso_week)
    moved = base_friday + timedelta(weeks=add)
    y, w = current_iso_year_week(moved)
    return y, w


# ----------------------------
# Parsing: "YYYY - WkNN"
# ----------------------------
WEEK_RE = re.compile(r"^\s*(\d{4})\s*-\s*Wk\s*0*([0-9]{1,2})\s*$", re.IGNORECASE)


@dataclass(frozen=True)
class WeekKey:
    iso_year: int
    iso_week: int

    @property
    def label(self) -> str:
        return f"{self.iso_year} - Wk{self.iso_week:02d}"

    @property
    def friday(self) -> date:
        return iso_week_friday(self.iso_year, self.iso_week)


def parse_week_label(value: object) -> Optional[WeekKey]:
    if value is None:
        return None
    s = str(value).strip()
    m = WEEK_RE.match(s)
    if not m:
        return None
    y = int(m.group(1))
    w = int(m.group(2))
    if w < 1 or w > 53:
        return None
    return WeekKey(y, w)


# ----------------------------
# IO: robust Excel read (.xls / .xlsx)
# ----------------------------
def read_excel_any(uploaded_file) -> pd.DataFrame:
    """
    Reads .xls or .xlsx from Streamlit uploader.
    Requires:
      - openpyxl for .xlsx
      - xlrd for .xls
    """
    # Streamlit uploaded_file is a BytesIO-like object
    name = uploaded_file.name.lower()
    data = uploaded_file.read()
    bio = io.BytesIO(data)

    if name.endswith(".xlsx") or name.endswith(".xlsm") or name.endswith(".xltx"):
        return pd.read_excel(bio, engine="openpyxl")
    elif name.endswith(".xls"):
        return pd.read_excel(bio, engine="xlrd")
    else:
        # Let pandas try
        return pd.read_excel(bio)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def require_columns(df: pd.DataFrame, cols: List[str], sheet_name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{sheet_name}: missing required columns: {', '.join(missing)}")


# ----------------------------
# Core compute
# ----------------------------
@dataclass
class RunoutRow:
    item: str
    starting_available: float
    first_shortage_week: Optional[str]  # label
    first_shortage_friday: Optional[date]
    shortage_qty: float
    demand_that_week: float


def compute_runout(
    demand_df: pd.DataFrame,
    inv_df: pd.DataFrame,
) -> Tuple[List[RunoutRow], List[str]]:
    """
    Returns:
      - list of runout rows (one per Item)
      - list of week parse errors examples
    """

    demand_df = normalize_columns(demand_df)
    inv_df = normalize_columns(inv_df)

    require_columns(demand_df, DEMAND_REQUIRED_COLS, "Demand")
    require_columns(inv_df, INV_REQUIRED_COLS, "Inventory")

    # Inventory: keep Status == good (case-insensitive), sum Available by Item
    inv_df["Item"] = inv_df["Item"].astype(str).str.strip()
    inv_df["Status_norm"] = inv_df["Status"].astype(str).str.strip().str.lower()
    inv_good = inv_df[inv_df["Status_norm"] == GOOD_STATUS_VALUE].copy()

    inv_good["Available_num"] = pd.to_numeric(inv_good["Available"], errors="coerce").fillna(0.0)
    starting_by_item: Dict[str, float] = (
        inv_good.groupby("Item")["Available_num"].sum().to_dict()
    )

    # Demand: parse week labels, sum Quantity by (Item, WeekLabel)
    demand_df["Item"] = demand_df["Item"].astype(str).str.strip()
    demand_df["Quantity_num"] = pd.to_numeric(demand_df["Quantity"], errors="coerce").fillna(0.0)

    bad_weeks: List[str] = []
    wk_keys: List[Optional[WeekKey]] = []
    for v in demand_df["Aerostar Ship Week"].tolist():
        wk = parse_week_label(v)
        wk_keys.append(wk)
        if wk is None and v is not None and str(v).strip() != "":
            bad_weeks.append(str(v))

    demand_df["WeekKey"] = wk_keys
    demand_ok = demand_df[demand_df["WeekKey"].notna()].copy()

    # explode week label
    demand_ok["WeekLabel"] = demand_ok["WeekKey"].apply(lambda w: w.label)
    demand_ok["WeekFriday"] = demand_ok["WeekKey"].apply(lambda w: w.friday)

    weekly_demand = (
        demand_ok.groupby(["Item", "WeekLabel", "WeekFriday"])["Quantity_num"]
        .sum()
        .reset_index()
    )

    # Items to evaluate: union of inventory items and demand items
    all_items = set(starting_by_item.keys()) | set(weekly_demand["Item"].unique().tolist())

    # Build per-item timelines & compute first shortage
    results: List[RunoutRow] = []

    for item in sorted(all_items):
        starting = float(starting_by_item.get(item, 0.0))
        timeline = weekly_demand[weekly_demand["Item"] == item].copy()
        timeline = timeline.sort_values("WeekFriday")

        if timeline.empty:
            results.append(
                RunoutRow(
                    item=item,
                    starting_available=starting,
                    first_shortage_week=None,
                    first_shortage_friday=None,
                    shortage_qty=0.0,
                    demand_that_week=0.0,
                )
            )
            continue

        remaining = starting
        first_short: Optional[RunoutRow] = None

        for _, row in timeline.iterrows():
            qty = float(row["Quantity_num"])
            remaining -= qty
            if remaining < 0:
                first_short = RunoutRow(
                    item=item,
                    starting_available=starting,
                    first_shortage_week=str(row["WeekLabel"]),
                    first_shortage_friday=row["WeekFriday"],
                    shortage_qty=abs(remaining),
                    demand_that_week=qty,
                )
                break

        if first_short is None:
            # Never shorts within demand data provided
            last = timeline.iloc[-1]
            results.append(
                RunoutRow(
                    item=item,
                    starting_available=starting,
                    first_shortage_week=None,  # treat as safe / no shortage found
                    first_shortage_friday=None,
                    shortage_qty=0.0,
                    demand_that_week=0.0,
                )
            )
        else:
            results.append(first_short)

    # Keep only a few examples of bad week strings
    bad_examples = bad_weeks[:20]
    return results, bad_examples


# ----------------------------
# Kanban rendering (scrollable HTML)
# ----------------------------
def build_horizon_weeks(horizon_weeks: int, include_current: bool) -> List[WeekKey]:
    today = date.today()
    y, w = current_iso_year_week(today)
    if not include_current:
        y, w = add_iso_weeks(y, w, 1)
    out: List[WeekKey] = []
    cy, cw = y, w
    for _ in range(horizon_weeks):
        out.append(WeekKey(cy, cw))
        cy, cw = add_iso_weeks(cy, cw, 1)
    return out


def bucket_for_row(
    r: RunoutRow,
    first_horizon_friday: date,
    last_horizon_friday: date,
    horizon_labels: set,
) -> str:
    if r.first_shortage_week is None or r.first_shortage_friday is None:
        return "No demand / Safe"

    if r.first_shortage_friday < first_horizon_friday:
        return "Already short / Past due"
    if r.first_shortage_friday > last_horizon_friday:
        return "Later"
    if r.first_shortage_week in horizon_labels:
        return r.first_shortage_week
    return "Later"


def kanban_html(columns: List[Tuple[str, str]], cards_by_col: Dict[str, List[RunoutRow]]) -> str:
    # columns: [(key, subtitle)]
    # cards_by_col: key -> list[RunoutRow]
    def esc(s: str) -> str:
        return (
            str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )

    css = """
    <style>
      .kb-wrap { overflow-x:auto; padding: 8px 0; }
      .kb { display:flex; gap:12px; min-width: 1200px; }
      .col { min-width: 260px; background:#fafafa; border:1px solid #ddd; border-radius:14px; display:flex; flex-direction:column; }
      .colhead { padding:10px 10px 8px 10px; border-bottom:1px solid #e6e6e6; }
      .title { font-weight:700; }
      .sub { color:#666; font-size:12px; margin-top:2px; }
      .cards { padding:10px; display:flex; flex-direction:column; gap:8px; }
      .card { background:#fff; border:1px solid #e5e5e5; border-radius:12px; padding:10px; box-shadow:0 1px 2px rgba(0,0,0,.04); }
      .item { font-weight:700; }
      .meta { margin-top:6px; font-size:13px; color:#333; line-height:1.3; }
      .tag { display:inline-block; font-size:12px; padding:2px 8px; border-radius:999px; border:1px solid #ddd; color:#333; margin-top:6px; }
    </style>
    """

    html = [css, '<div class="kb-wrap"><div class="kb">']
    for key, subtitle in columns:
        html.append('<div class="col">')
        html.append(f'<div class="colhead"><div class="title">{esc(key)}</div><div class="sub">{esc(subtitle)}</div></div>')
        html.append('<div class="cards">')

        for r in cards_by_col.get(key, []):
            runout = r.first_shortage_week or "—"
            fri = r.first_shortage_friday.isoformat() if r.first_shortage_friday else ""
            shortage = f"Short {r.shortage_qty:.0f}" if r.shortage_qty > 0 else "OK"
            html.append(
                f"""
                <div class="card">
                  <div class="item">{esc(r.item)}</div>
                  <div class="meta">
                    Avail(good): <b>{r.starting_available:.0f}</b><br/>
                    Run-out: <b>{esc(runout)}</b> {esc(fri)}<br/>
                    Demand that week: <b>{r.demand_that_week:.0f}</b>
                  </div>
                  <span class="tag">{esc(shortage)}</span>
                </div>
                """
            )

        html.append("</div></div>")
    html.append("</div></div>")
    return "".join(html)


# ----------------------------
# Early warning
# ----------------------------
def warning_bucket(r: RunoutRow, this_week_friday: date) -> str:
    if r.first_shortage_friday is None:
        return "Safe / No shortage found"
    delta_days = (r.first_shortage_friday - this_week_friday).days
    if delta_days <= 0:
        return "Critical (this week / past due)"
    if delta_days <= 14:
        return "Imminent (1–2 weeks)"
    if delta_days <= 28:
        return "Watch (3–4 weeks)"
    return "Later (>4 weeks)"


# ----------------------------
# App UI
# ----------------------------
st.set_page_config(page_title="Run-out Kanban + Early Warning", layout="wide")
st.title("Inventory Run-out Kanban (12 weeks) + Early Warning")

st.caption(
    "Rules: Item = part number • Demand = Quantity • Aerostar Ship Week = 'YYYY - WkNN' (ISO) • Due by Friday • "
    "Inventory = Available where Status=good • Run-out = first week cumulative demand exceeds inventory."
)

with st.sidebar:
    st.header("Settings")
    horizon = st.number_input("Kanban horizon (weeks)", min_value=4, max_value=26, value=HORIZON_WEEKS_DEFAULT, step=1)
    include_current = st.checkbox("Include current week", value=True)
    st.divider()
    st.subheader("Early warning thresholds")
    st.write("Fixed buckets: Critical (≤0d), Imminent (≤14d), Watch (≤28d)")

tab1, tab2, tab3 = st.tabs(["Kanban", "Early Warning", "Snapshot Compare"])

with tab1:
    st.subheader("Upload files")

    c1, c2 = st.columns(2)
    with c1:
        demand_file = st.file_uploader("Demand Excel (.xls/.xlsx)", type=["xls", "xlsx", "xlsm", "xltx"], key="demand")
        st.write("Required columns:", ", ".join(DEMAND_REQUIRED_COLS))
    with c2:
        inv_file = st.file_uploader("Inventory Excel (.xls/.xlsx)", type=["xls", "xlsx", "xlsm", "xltx"], key="inv")
        st.write("Required columns:", ", ".join(INV_REQUIRED_COLS))

    run = st.button("Build Kanban", type="primary", disabled=not (demand_file and inv_file))

    if run:
        try:
            demand_df = read_excel_any(demand_file)
            inv_df = read_excel_any(inv_file)

            rows, bad_week_examples = compute_runout(demand_df, inv_df)

            if bad_week_examples:
                st.warning(
                    "Some rows have invalid 'Aerostar Ship Week' labels (expected 'YYYY - WkNN'). "
                    "Examples:\n- " + "\n- ".join(bad_week_examples)
                )

            # store latest in session for other tabs
            st.session_state["latest_rows"] = rows

            # Horizon columns
            horizon_weeks = build_horizon_weeks(int(horizon), include_current=include_current)
            horizon_labels = {w.label for w in horizon_weeks}
            first_friday = horizon_weeks[0].friday
            last_friday = horizon_weeks[-1].friday

            # Column definitions
            columns: List[Tuple[str, str]] = []
            columns.append(("Already short / Past due", "Shortage occurs before visible horizon"))
            for w in horizon_weeks:
                columns.append((w.label, f"Week ending Fri {w.friday.isoformat()}"))
            columns.append(("Later", "Shortage occurs after horizon"))
            columns.append(("No demand / Safe", "No demand (or no shortage found in demand data)"))

            # Place cards
            cards_by_col: Dict[str, List[RunoutRow]] = {k: [] for k, _ in columns}
            for r in rows:
                key = bucket_for_row(r, first_friday, last_friday, horizon_labels)
                cards_by_col.setdefault(key, []).append(r)

            # Sort cards inside columns by earliest shortage then biggest shortage
            def sort_key(rr: RunoutRow):
                d = rr.first_shortage_friday or date.max
                return (d, -rr.shortage_qty, rr.item)

            for k in cards_by_col:
                cards_by_col[k] = sorted(cards_by_col[k], key=sort_key)

            # Render kanban (scrollable)
            st.components.v1.html(kanban_html(columns, cards_by_col), height=650, scrolling=True)

            # Summary table + download
            summary_df = pd.DataFrame([{
                "Item": r.item,
                "StartingAvailable": r.starting_available,
                "FirstShortageWeek": r.first_shortage_week or "",
                "WeekEndingFriday": (r.first_shortage_friday.isoformat() if r.first_shortage_friday else ""),
                "ShortageQty": r.shortage_qty,
                "DemandThatWeek": r.demand_that_week,
            } for r in rows])

            st.download_button(
                "Download run-out summary CSV",
                data=summary_df.to_csv(index=False).encode("utf-8"),
                file_name="runout_summary.csv",
                mime="text/csv",
            )

            with st.expander("Show run-out summary table"):
                st.dataframe(summary_df, use_container_width=True)

        except Exception as e:
            st.error(f"Failed: {e}")

with tab2:
    st.subheader("Early Warning")
    rows: List[RunoutRow] = st.session_state.get("latest_rows", [])
    if not rows:
        st.info("Build the Kanban first (upload files in the Kanban tab).")
    else:
        # This week's Friday (based on current ISO week)
        ty, tw = current_iso_year_week(date.today())
        this_friday = iso_week_friday(ty, tw)

        warn_rows = []
        for r in rows:
            bucket = warning_bucket(r, this_friday)
            warn_rows.append({
                "Bucket": bucket,
                "Item": r.item,
                "RunOutWeek": r.first_shortage_week or "",
                "RunOutFriday": r.first_shortage_friday.isoformat() if r.first_shortage_friday else "",
                "StartingAvailable": r.starting_available,
                "ShortageQty": r.shortage_qty,
                "DemandThatWeek": r.demand_that_week,
            })

        warn_df = pd.DataFrame(warn_rows)

        order = {
            "Critical (this week / past due)": 0,
            "Imminent (1–2 weeks)": 1,
            "Watch (3–4 weeks)": 2,
            "Later (>4 weeks)": 3,
            "Safe / No shortage found": 4,
        }
        warn_df["Order"] = warn_df["Bucket"].map(order).fillna(99)
        warn_df = warn_df.sort_values(["Order", "RunOutFriday", "ShortageQty"], ascending=[True, True, False]).drop(columns=["Order"])

        # Quick KPIs
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Critical", int((warn_df["Bucket"] == "Critical (this week / past due)").sum()))
        c2.metric("Imminent", int((warn_df["Bucket"] == "Imminent (1–2 weeks)").sum()))
        c3.metric("Watch", int((warn_df["Bucket"] == "Watch (3–4 weeks)").sum()))
        c4.metric("Safe/Other", int((warn_df["Bucket"].isin(["Safe / No shortage found", "Later (>4 weeks)"])).sum()))

        st.dataframe(warn_df, use_container_width=True)

        st.download_button(
            "Download early-warning CSV",
            data=warn_df.to_csv(index=False).encode("utf-8"),
            file_name="early_warning.csv",
            mime="text/csv",
        )

with tab3:
    st.subheader("Snapshot Compare (optional early-warning upgrade)")
    st.write(
        "This lets you compare the current run-out summary vs a previous snapshot CSV "
        "to alert on changes (e.g., run-out moved earlier, shortage increased)."
    )

    rows: List[RunoutRow] = st.session_state.get("latest_rows", [])
    if not rows:
        st.info("Build the Kanban first, then come back here.")
    else:
        current_df = pd.DataFrame([{
            "Item": r.item,
            "RunOutFriday": r.first_shortage_friday.isoformat() if r.first_shortage_friday else "",
            "ShortageQty": float(r.shortage_qty),
        } for r in rows])

        prev_file = st.file_uploader("Upload previous snapshot CSV (runout_summary.csv)", type=["csv"], key="prev_csv")
        if prev_file:
            prev_df = pd.read_csv(prev_file)
            # Normalize
            prev_df = prev_df.rename(columns={c: str(c).strip() for c in prev_df.columns})
            # Try to map common column names
            if "WeekEndingFriday" in prev_df.columns and "RunOutFriday" not in prev_df.columns:
                prev_df["RunOutFriday"] = prev_df["WeekEndingFriday"]
            if "ShortageQty" not in prev_df.columns and "ShortageQty" in prev_df.columns:
                pass

            need = {"Item", "RunOutFriday", "ShortageQty"}
            missing = need - set(prev_df.columns)
            if missing:
                st.error(f"Previous CSV missing required columns: {', '.join(sorted(missing))}")
            else:
                merged = current_df.merge(
                    prev_df[["Item", "RunOutFriday", "ShortageQty"]],
                    on="Item",
                    how="outer",
                    suffixes=("_current", "_prev")
                )

                def parse_date_or_none(s):
                    try:
                        ss = str(s).strip()
                        if ss == "" or ss.lower() == "nan":
                            return None
                        return datetime.fromisoformat(ss).date()
                    except Exception:
                        return None

                changes = []
                for _, r in merged.iterrows():
                    item = str(r["Item"]) if pd.notna(r["Item"]) else ""
                    cur_d = parse_date_or_none(r.get("RunOutFriday_current"))
                    prev_d = parse_date_or_none(r.get("RunOutFriday_prev"))
                    cur_s = float(r.get("ShortageQty_current")) if pd.notna(r.get("ShortageQty_current")) else 0.0
                    prev_s = float(r.get("ShortageQty_prev")) if pd.notna(r.get("ShortageQty_prev")) else 0.0

                    movement = ""
                    if prev_d is None and cur_d is not None:
                        movement = "New risk"
                    elif prev_d is not None and cur_d is None:
                        movement = "Recovered / No shortage"
                    elif prev_d is not None and cur_d is not None:
                        if cur_d < prev_d:
                            movement = "Worse (earlier run-out)"
                        elif cur_d > prev_d:
                            movement = "Better (later run-out)"
                        else:
                            movement = "No date change"

                    shortage_change = cur_s - prev_s
                    changes.append({
                        "Item": item,
                        "PrevRunOut": prev_d.isoformat() if prev_d else "",
                        "CurRunOut": cur_d.isoformat() if cur_d else "",
                        "PrevShortage": prev_s,
                        "CurShortage": cur_s,
                        "ShortageDelta": shortage_change,
                        "Movement": movement
                    })

                changes_df = pd.DataFrame(changes)
                # Prioritize meaningful changes
                priority = {
                    "Worse (earlier run-out)": 0,
                    "New risk": 1,
                    "No date change": 2,
                    "Better (later run-out)": 3,
                    "Recovered / No shortage": 4,
                    "": 99,
                }
                changes_df["P"] = changes_df["Movement"].map(priority).fillna(99)
                changes_df = changes_df.sort_values(["P", "ShortageDelta"], ascending=[True, False]).drop(columns=["P"])

                st.dataframe(changes_df, use_container_width=True)

                st.download_button(
                    "Download changes CSV",
                    data=changes_df.to_csv(index=False).encode("utf-8"),
                    file_name="runout_changes.csv",
                    mime="text/csv",
                )
