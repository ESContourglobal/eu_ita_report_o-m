import os
import re
from datetime import date, timedelta
from typing import List, Tuple, Optional
import html
import pandas as pd

from config import setup_logger
from cg_mail import send_email
from db import get_monthly_measures

logger = setup_logger("send_mail")

PLANT_LIST_PATH = os.path.join("inputs", "plant_list.xlsx")
PLANT_LIST_COL = "Plant"


# -----------------------------
# Name utilities (soft)
# -----------------------------
def norm_name(s: str) -> str:
    if pd.isna(s) or s is None:
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def get_df_plants(df: pd.DataFrame) -> pd.Series:
    if "inxieme_name" not in df.columns:
        raise KeyError("Column 'inxieme_name' not found in df")
    return (
        df["inxieme_name"]
        .dropna()
        .astype(str)
        .map(norm_name)
        .loc[lambda x: x != ""]
        .drop_duplicates()
    )


# -----------------------------
# Plant list (inputs/plant_list.xlsx)
# -----------------------------
def load_plant_list(path: str = PLANT_LIST_PATH, col: str = PLANT_LIST_COL) -> pd.Series:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Plant list file not found: {path}")

    pl = pd.read_excel(path)
    if col not in pl.columns:
        raise KeyError(f"Column '{col}' not found in {path}. Available: {list(pl.columns)}")

    return (
        pl[col]
        .dropna()
        .astype(str)
        .map(norm_name)
        .loc[lambda x: x != ""]
        .drop_duplicates()
    )


# -----------------------------
# Checks
# -----------------------------
def check_missing_any_measures_vs_plant_list(
    df: pd.DataFrame,
    plant_list_path: str = PLANT_LIST_PATH,
    plant_list_col: str = PLANT_LIST_COL,
) -> List[str]:
    """
    Plants in plant_list but not present in df (inxieme_name).
    """
    expected = set(load_plant_list(plant_list_path, plant_list_col).tolist())
    present = set(get_df_plants(df).tolist())
    missing = sorted(expected - present)
    return missing


def check_missing_source_measures(
    df: pd.DataFrame,
    source_col: str,
    treat_zero_as_missing: bool = False
) -> List[str]:
    """
    Plants in df where source_col is missing (NaN), optionally also 0.
    Returns list of inxieme_name unique, sorted.
    """
    if "inxieme_name" not in df.columns:
        raise KeyError("Column 'inxieme_name' not found in df")
    if source_col not in df.columns:
        raise KeyError(f"Column '{source_col}' not found in df")

    ser = df[source_col]
    mask = ser.isna()
    if treat_zero_as_missing:
        mask = mask | (ser == 0)

    return (
        df.loc[mask, "inxieme_name"]
        .dropna()
        .astype(str)
        .map(norm_name)
        .drop_duplicates()
        .sort_values()
        .tolist()
    )


# -----------------------------
# Email body formatting
# -----------------------------
def format_plants_block(title: str, plants: List[str]) -> str:
    if not plants:
        return f"{title}\n- None\n"
    lines = [title]
    lines += [f"- {p}" for p in plants]
    return "\n".join(lines) + "\n"

def format_section(title: str, intro: str, plants: list[str]) -> str:
    if not plants:
        return f"**{title}**\n{intro}\n- None\n\n"

    lines = [
        f"**{title}**",
        intro,
    ]
    lines += [f"- {p}" for p in plants]
    return "\n".join(lines) + "\n\n"



def _html_ul(items: list[str]) -> str:
    if not items:
        return "<ul><li><i>None</i></li></ul>"
    safe_items = [f"<li>{html.escape(str(x))}</li>" for x in items]
    return "<ul>" + "".join(safe_items) + "</ul>"

def build_email_body(
    year: int,
    month: int,
    missing_any: list[str],
    missing_edis: list[str],
    missing_kite: list[str],
    responsible_name: str = "",
) -> str:
    month_name = pd.Timestamp(year=year, month=month, day=1).strftime("%B %Y")

    blocks: list[str] = []

    blocks.append("<li><h3>Missing measures</h3>")
    blocks.append("<p>Please note that the production measures are missing for the following plants:</p>")
    blocks.append(_html_ul(missing_any))

    blocks.append("<li><h3>Missing E-Dis measures</h3>")
    blocks.append("<p>Please note that E-Dis production data is missing for the following plants:</p>")
    blocks.append(_html_ul(missing_any))
    blocks.append(_html_ul(missing_edis))

    blocks.append("<li><h3>Missing Kit-E measures</h3>")
    blocks.append("<p>Please note that Kit-E production data is missing for the following plants:</p>")
    blocks.append(_html_ul(missing_any))
    blocks.append(_html_ul(missing_kite))

    body = f"""
    <p>Dear all,</p>
    <p>Please find attached the production report for <b>{html.escape(month_name)}</b>.</p>

    <p>Please note the following checks:</p>
    <ul>
            {''.join(blocks)}
    </ul>



    <hr style="margin-top:20px; margin-bottom:20px;" />
    <p style="font-size:12px; color:#555;">
        Report prepared by <strong>{html.escape(responsible_name)}</strong><br/>
    </p>
    """.strip()

    return body

# -----------------------------
# Report creation
# -----------------------------
def create_attachment_excel(report_path: str, month: int, year: int) -> Tuple[str, pd.DataFrame]:
    df = get_monthly_measures(month, year)

    report_filename = f"Monthly_Report_Production_{year}_{month:02d}.xlsx"
    report_full_path = os.path.join(report_path, report_filename)

    df.to_excel(report_full_path, index=False)
    logger.info(f"Report created at {report_full_path}")

    return report_full_path, df


def build_and_send_email():
    today = date.today()
    first_day_last_month = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    month = first_day_last_month.month
    year = first_day_last_month.year

    report_path = "./reports"
    os.makedirs(report_path, exist_ok=True)
    report_file, df = create_attachment_excel(report_path, month, year)

    treat_zero_as_missing = False

    missing_any = check_missing_any_measures_vs_plant_list(df, PLANT_LIST_PATH, PLANT_LIST_COL)
    missing_edis = check_missing_source_measures(df, "edis_total_prod_MWh", treat_zero_as_missing)
    missing_kite = check_missing_source_measures(df, "kite_total_prod_MWh", treat_zero_as_missing)

    subject = f"Monthly Production Report - {year}-{month:02d}"
    body = build_email_body(
        year=year,
        month=month,
        missing_any=missing_any,
        missing_edis=missing_edis,
        missing_kite=missing_kite,
        responsible_name="EM&CO Team" 
    )

    recipients = ["francesca.rudello@contourglobal.com"]

    ok = send_email(
        subject=subject,
        body=body,
        recipients=recipients,
        attachments=[report_file],
    )
    return ok


if __name__ == "__main__":
    res = build_and_send_email()
    print("Send results:", res)
