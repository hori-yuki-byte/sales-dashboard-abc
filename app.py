import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import datetime as _dt
from datetime import timedelta, date as _date
import requests
import io
import re
import json
from pathlib import Path

DATE_MIN = _date(2016, 1, 1)
DATE_MAX = _date(2036, 12, 31)

st.set_page_config(
    page_title="商談ログ分析ダッシュボード",
    page_icon="📊",
    layout="wide"
)

# =============================================
# 定数・設定
# =============================================

# 商談ログ②の固定接続先
DEFAULT_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1VAb_viGsONfjz04wLOU6M1JHNdApKGmiEE80Nuc5DvY"
    "/export?format=csv&gid=1838854196"
)

# ---- 各指標の定義 ----
# アポUU    : 報告種別 が「アポ」と完全一致
# プレUU    : 報告種別 が「プレ」と完全一致
# 再プレUU  : 報告種別 が「再プレ」と完全一致
# プレ飛びUU: 報告種別 に「プレ飛び」を含む
# 契約UU    : 結果 が「契約」と完全一致
# 次回契約予定UU: 結果 が「次回契約予定」を含む
# 失注UU    : プレ飛び・再プレ飛び・失注（報告種別 or 結果）
APO_PATTERN              = r"^アポ$"
PRE_PATTERN              = r"^プレ$"
RE_PRE_PATTERN           = r"^再プレ$"
PRE_NOSHOWN_PATTERN      = r"プレ飛び"
RE_PRE_NOSHOWN_PATTERN   = r"再プレ飛び"
CONTRACT_PATTERN         = r"^契約$"
NEXT_CONTRACT_PATTERN    = r"次回契約予定"
LOST_PATTERN             = r"^失注$"
PRE_RESCHEDULED_PATTERN  = r"リスケ日程不明|リスケ日程確定"
CONTRACT_NOSHOWN_PATTERN = r"契約予定飛び"   # 報告種別
CONTRACT_ADJUST_PATTERN  = r"契約予定調整"   # 報告種別 + 結果=失注

# プレUU・再プレUUから除外する結果値
PRE_EXCLUDE_RESULTS = [PRE_RESCHEDULED_PATTERN, "^プレ日程確定$"]  # 「再プレ日程確定」は除外しない

# ヘッダーなしCSVに付ける列名（順番はスプレッドシートの列順と一致）
SHEET_COLS = [
    "タイムスタンプ", "顧客ID", "顧客名", "営業担当者",
    "営業日", "報告種別", "結果", "次回アクション",
    "次回アクション日", "zoom録画"
]

# =============================================
# チーム管理（teams.json で永続化）
# =============================================

TEAMS_FILE    = Path(__file__).parent / "teams.json"
SETTINGS_FILE = Path(__file__).parent / "settings.json"

DEFAULT_THRESHOLDS = {
    "成約率":     25.0,
    "プレ言質率": 25.0,
    "再プレ言質率": 50.0,
    "プレ着座率": 75.0,
    "再プレ着座率": 50.0,
    "プレ成約率": 40.0,
}


def load_teams() -> dict:
    """teams.json からチーム定義を読み込む"""
    if TEAMS_FILE.exists():
        with open(TEAMS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_teams(teams: dict):
    """チーム定義を teams.json に保存する"""
    with open(TEAMS_FILE, "w", encoding="utf-8") as f:
        json.dump(teams, f, ensure_ascii=False, indent=2)


def load_settings() -> dict:
    """settings.json からアラート設定を読み込む"""
    if SETTINGS_FILE.exists():
        with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"thresholds": DEFAULT_THRESHOLDS.copy()}


def save_settings(settings: dict):
    """アラート設定を settings.json に保存する"""
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(settings, f, ensure_ascii=False, indent=2)


# =============================================
# データ読み込み・前処理
# =============================================

def fetch_from_sheets_url(url: str) -> pd.DataFrame:
    """Google Sheets URLからデータを取得する"""
    response = requests.get(url, timeout=30)
    if response.status_code != 200:
        raise ConnectionError(f"取得失敗（ステータス: {response.status_code}）")

    raw = None
    for enc in ("utf-8-sig", "shift_jis", "utf-8"):
        try:
            raw = response.content.decode(enc)
            break
        except Exception:
            continue
    if raw is None:
        raise ValueError("CSVのエンコーディングを特定できませんでした")

    return _parse_csv(raw)


def _assign_cols(df: pd.DataFrame) -> pd.DataFrame:
    """ヘッダーなしCSVに列名を付ける"""
    n = len(df.columns)
    names = SHEET_COLS[:n] if n <= len(SHEET_COLS) else SHEET_COLS + [f"列{i}" for i in range(n - len(SHEET_COLS))]
    df.columns = names
    return df


def _parse_csv(raw: str) -> pd.DataFrame:
    """CSV文字列をDataFrameに変換。ヘッダーなしも自動判定"""
    df = pd.read_csv(io.StringIO(raw))
    if re.match(r"\d{4}[/\-]\d{2}", str(df.columns[0])):
        df = pd.read_csv(io.StringIO(raw), header=None)
        df = _assign_cols(df)
    return df


def load_data(uploaded_file) -> pd.DataFrame:
    """アップロードCSVを読み込む"""
    for enc in ("utf-8-sig", "shift_jis"):
        try:
            uploaded_file.seek(0)
            raw = uploaded_file.read().decode(enc)
            return _parse_csv(raw)
        except Exception:
            continue
    raise ValueError("CSVの読み込みに失敗しました")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """列名を正規化し、日付型に変換する"""
    df.columns = df.columns.str.strip()
    # 重複列は最初の1列だけ残す
    df = df.loc[:, ~df.columns.duplicated()]
    # パターン→正規名のマッピング（次回アクション日を先に置き部分一致の誤ヒットを防ぐ）
    _COL_PATTERNS = [
        ("タイムスタンプ", "タイムスタンプ"),
        ("顧客ID",         "顧客ID"),
        ("顧客名",         "顧客名"),
        ("営業担当",       "営業担当者"),
        ("担当者",         "営業担当者"),
        ("営業日",         "営業日"),
        ("報告種別",       "報告種別"),
        ("結果",           "結果"),
        ("次回アクション日", "次回アクション日"),
        ("次回アクション",   "次回アクション"),
    ]
    col_map = {}
    for col in df.columns:
        if "zoom" in col.lower() or "録画" in col:
            col_map[col] = "zoom録画"
        else:
            for pattern, name in _COL_PATTERNS:
                if pattern in col:
                    col_map[col] = name
                    break
    df = df.rename(columns=col_map)
    # 全列：前後スペース・シングルクォート・全角スペースを除去（日付変換前に実施）
    for _c in ["報告種別", "結果", "営業担当者", "顧客名", "顧客ID",
               "次回アクション", "次回アクション日", "営業日", "タイムスタンプ"]:
        if _c in df.columns:
            df[_c] = (df[_c].astype(str)
                      .str.strip()
                      .str.strip("'")
                      .str.replace("\u3000", "", regex=False))
    if "営業日" in df.columns:
        df["営業日"] = pd.to_datetime(df["営業日"], errors="coerce", format="mixed")
    if "タイムスタンプ" in df.columns:
        df["タイムスタンプ"] = pd.to_datetime(df["タイムスタンプ"], errors="coerce", format="mixed")
    if "営業日" not in df.columns and "タイムスタンプ" in df.columns:
        df["営業日"] = df["タイムスタンプ"]
    return df


# =============================================
# KPI計算
# =============================================

def get_col(df, name) -> pd.Series:
    """列名重複でDataFrameが返る場合に備えて必ずSeriesを返す"""
    col = df[name]
    if isinstance(col, pd.DataFrame):
        col = col.iloc[:, 0]
    return col.astype(str)


def col_uu(df, filter_col, pattern, id_col="顧客ID", regex=False, exclude_col=None, exclude_patterns=None) -> int:
    """条件に合う行のユニーク顧客数を返す。exclude_patternsはexclude_col列で除外する"""
    if filter_col not in df.columns:
        return 0
    mask = get_col(df, filter_col).str.contains(pattern, na=False, regex=regex)
    if exclude_col and exclude_patterns and exclude_col in df.columns:
        for ep in exclude_patterns:
            mask &= ~get_col(df, exclude_col).str.contains(ep, na=False, regex=True)
    matched = df[mask]
    if id_col in matched.columns:
        return get_col(matched, id_col).nunique()
    return len(matched)


def calc_kpi(df):
    kpi = {}
    kpi["total_uu"] = get_col(df, "顧客ID").nunique() if "顧客ID" in df.columns else len(df)

    kpi["アポUU"]   = col_uu(df, "報告種別", APO_PATTERN, regex=True)
    kpi["プレUU"]   = col_uu(df, "報告種別", PRE_PATTERN,    regex=True,
                              exclude_col="結果", exclude_patterns=PRE_EXCLUDE_RESULTS)
    kpi["再プレUU"] = col_uu(df, "報告種別", RE_PRE_PATTERN, regex=True,
                              exclude_col="結果", exclude_patterns=PRE_EXCLUDE_RESULTS)
    kpi["プレ飛びUU"]   = col_uu(df, "報告種別", PRE_NOSHOWN_PATTERN, regex=True)
    kpi["契約UU"]         = col_uu(df, "結果", CONTRACT_PATTERN, regex=True)
    kpi["次回契約予定UU"] = col_uu(df, "結果", NEXT_CONTRACT_PATTERN, regex=True)

    # 失注UU：失注のみ（プレ飛び・再プレ飛びは独立指標として分離）
    lost_ids = set()
    if "顧客ID" in df.columns:
        for col in ("報告種別", "結果"):
            if col in df.columns:
                mask = get_col(df, col).str.contains(LOST_PATTERN, na=False, regex=True)
                lost_ids |= set(get_col(df[mask], "顧客ID"))
    kpi["失注UU"] = len(lost_ids)

    # 契約飛びUU：「契約予定飛び」OR（「契約予定調整」AND 結果=失注）
    contract_noshown_ids = set()
    if "顧客ID" in df.columns and "報告種別" in df.columns:
        m1 = get_col(df, "報告種別").str.contains(CONTRACT_NOSHOWN_PATTERN, na=False, regex=True)
        m2 = get_col(df, "報告種別").str.contains(CONTRACT_ADJUST_PATTERN, na=False, regex=True)
        if "結果" in df.columns:
            m2 &= get_col(df, "結果").str.contains(LOST_PATTERN, na=False, regex=True)
        contract_noshown_ids = set(get_col(df[m1 | m2], "顧客ID"))
    kpi["契約飛びUU"] = len(contract_noshown_ids)

    # 再プレ飛びUU（独立指標）
    kpi["再プレ飛びUU"] = col_uu(df, "報告種別", RE_PRE_NOSHOWN_PATTERN, regex=True)

    # プレリスケUU：報告種別=プレ かつ 結果にリスケを含む
    if "報告種別" in df.columns and "結果" in df.columns and "顧客ID" in df.columns:
        pre_riske_mask = (
            get_col(df, "報告種別").str.contains(PRE_PATTERN, na=False, regex=True)
            & get_col(df, "結果").str.contains(PRE_RESCHEDULED_PATTERN, na=False, regex=True)
        )
        re_pre_riske_mask = (
            get_col(df, "報告種別").str.contains(RE_PRE_PATTERN, na=False, regex=True)
            & get_col(df, "結果").str.contains(PRE_RESCHEDULED_PATTERN, na=False, regex=True)
        )
        kpi["プレリスケUU"]   = get_col(df[pre_riske_mask],    "顧客ID").nunique()
        kpi["再プレリスケUU"] = get_col(df[re_pre_riske_mask], "顧客ID").nunique()
        # ブリッジUU：報告種別=アポ かつ 結果にプレ日程確定を含む
        bridge_mask = (
            get_col(df, "報告種別").str.contains(APO_PATTERN, na=False, regex=True)
            & get_col(df, "結果").str.contains(r"プレ日程確定", na=False, regex=True)
        )
        kpi["ブリッジUU"] = get_col(df[bridge_mask], "顧客ID").nunique()
        # 契約リスケUU：報告種別に「契約」を含む かつ 結果にリスケを含む
        contract_riske_mask = (
            get_col(df, "報告種別").str.contains(r"契約", na=False, regex=True)
            & get_col(df, "結果").str.contains(PRE_RESCHEDULED_PATTERN, na=False, regex=True)
        )
        kpi["契約リスケUU"] = get_col(df[contract_riske_mask], "顧客ID").nunique()
    else:
        kpi["プレリスケUU"]   = 0
        kpi["再プレリスケUU"] = 0
        kpi["ブリッジUU"]     = 0
        kpi["契約リスケUU"]   = 0

    # 成約率：契約 ÷（契約＋失注＋プレ飛び＋再プレ飛び＋契約飛び）
    denom = kpi["契約UU"] + kpi["失注UU"] + kpi["プレ飛びUU"] + kpi["再プレ飛びUU"] + kpi["契約飛びUU"]
    kpi["成約率"] = f"{kpi['契約UU'] / denom * 100:.1f}%" if denom > 0 else "-"

    return kpi


def calc_ganchi(df):
    """
    最後のプレ/再プレが起点で契約言質（契約or次回契約予定）に至った顧客数を計算。
    Aルール：最後のプレ/再プレが「プレ」→プレ言質、「再プレ」→再プレ言質
    戻り値: {"プレ言質UU": int, "再プレ言質UU": int, "プレ言質率": str, "再プレ言質率": str}
    """
    zero = {"プレ言質UU": 0, "再プレ言質UU": 0, "プレ言質率": "-", "再プレ言質率": "-"}
    if not {"顧客ID", "報告種別", "営業日"}.issubset(df.columns):
        return zero

    # プレまたは再プレ（リスケ・日程確定除く）の行を抽出
    pre_mask    = get_col(df, "報告種別").str.contains(PRE_PATTERN,    na=False, regex=True)
    re_pre_mask = get_col(df, "報告種別").str.contains(RE_PRE_PATTERN, na=False, regex=True)
    if "結果" in df.columns:
        for ep in PRE_EXCLUDE_RESULTS:
            excl = get_col(df, "結果").str.contains(ep, na=False, regex=True)
            pre_mask    &= ~excl
            re_pre_mask &= ~excl

    pre_re_df = df[pre_mask | re_pre_mask][["顧客ID", "営業日", "報告種別"]]
    if pre_re_df.empty:
        return zero

    # 顧客ごとに最後のプレ/再プレを特定
    last_row = pre_re_df.sort_values("営業日").groupby("顧客ID").last().reset_index()
    last_row["is_pre"] = get_col(last_row, "報告種別").str.contains(PRE_PATTERN, na=False, regex=True)

    pre_ids    = set(get_col(last_row[last_row["is_pre"]],  "顧客ID"))
    re_pre_ids = set(get_col(last_row[~last_row["is_pre"]], "顧客ID"))

    # 契約言質がある顧客ID（契約 or 次回契約予定）
    if "結果" not in df.columns:
        return zero
    ganchi_mask = (
        get_col(df, "結果").str.contains(CONTRACT_PATTERN,      na=False, regex=True)
        | get_col(df, "結果").str.contains(NEXT_CONTRACT_PATTERN, na=False, regex=True)
    )
    ganchi_ids = set(get_col(df[ganchi_mask], "顧客ID"))

    pre_ganchi    = len(pre_ids    & ganchi_ids)
    re_pre_ganchi = len(re_pre_ids & ganchi_ids)

    # 分母はKPIと同じプレUU・再プレUU（リスケ・日程確定除く）
    pre_uu    = col_uu(df, "報告種別", PRE_PATTERN,    regex=True, exclude_col="結果", exclude_patterns=PRE_EXCLUDE_RESULTS)
    re_pre_uu = col_uu(df, "報告種別", RE_PRE_PATTERN, regex=True, exclude_col="結果", exclude_patterns=PRE_EXCLUDE_RESULTS)

    return {
        "プレ言質UU":   pre_ganchi,
        "再プレ言質UU": re_pre_ganchi,
        "プレ言質率":   f"{pre_ganchi    / pre_uu    * 100:.1f}%" if pre_uu    > 0 else "-",
        "再プレ言質率": f"{re_pre_ganchi / re_pre_uu * 100:.1f}%" if re_pre_uu > 0 else "-",
    }


def calc_per_person(df):
    if "営業担当者" not in df.columns:
        return pd.DataFrame()

    rows = []
    for person, grp in df.groupby("営業担当者"):
        kpi = calc_kpi(grp)
        g = calc_ganchi(grp)
        rows.append({
            "営業担当者":     person,
            "総UU":          kpi["total_uu"],
            "アポUU":        kpi["アポUU"],
            "プレUU":        kpi["プレUU"],
            "再プレUU":      kpi["再プレUU"],
            "プレ飛びUU":    kpi["プレ飛びUU"],
            "再プレ飛びUU":  kpi["再プレ飛びUU"],
            "契約飛びUU":    kpi["契約飛びUU"],
            "プレ言質UU":    g["プレ言質UU"],
            "再プレ言質UU":  g["再プレ言質UU"],
            "プレ言質率":    g["プレ言質率"],
            "再プレ言質率":  g["再プレ言質率"],
            "契約UU":        kpi["契約UU"],
            "次回契約予定UU": kpi["次回契約予定UU"],
            "失注UU":        kpi["失注UU"],
            "成約率":        kpi["成約率"],
            "プレリスケUU":   kpi["プレリスケUU"],
            "再プレリスケUU": kpi["再プレリスケUU"],
        })
    df_rows = pd.DataFrame(rows)
    return df_rows.sort_values("総UU", ascending=False) if not df_rows.empty else df_rows


def build_hassei_df(df_all, period_start, period_end):
    """
    発生ベース：期間内にアポした顧客を起点に、アポ日以降の全記録を返す。
    戻り値: (アポ起点のdf, アポUU数)
    """
    need = {"営業日", "報告種別", "顧客ID"}
    if not need.issubset(df_all.columns):
        return pd.DataFrame(), 0

    # 期間内のアポ行から 顧客ID → 最初のアポ日 を取得
    apo_mask = (
        get_col(df_all, "報告種別").str.contains(APO_PATTERN, na=False, regex=True)
        & (df_all["営業日"] >= period_start)
        & (df_all["営業日"] <= period_end)
    )
    apo_df = df_all[apo_mask][["顧客ID", "営業日"]].copy()
    apo_df.columns = ["顧客ID", "アポ日"]
    apo_first = apo_df.groupby("顧客ID")["アポ日"].min().reset_index()
    apo_uu = len(apo_first)

    if apo_uu == 0:
        return pd.DataFrame(), 0

    # 全記録とアポ日をマージし、アポ日以降の記録だけ残す
    df_merged = df_all.merge(apo_first, on="顧客ID", how="inner")
    df_after = df_merged[df_merged["営業日"] >= df_merged["アポ日"]].drop(columns=["アポ日"])

    return df_after, apo_uu


def calc_kpi_hassei(df_all, period_start, period_end):
    """発生ベースKPI：アポ起点コホートの指標を返す"""
    df_after, apo_uu = build_hassei_df(df_all, period_start, period_end)
    if df_after.empty:
        return {k: 0 for k in ["アポUU","プレUU","再プレUU","プレ飛びUU","再プレ飛びUU",
                                "契約飛びUU","契約UU","次回契約予定UU","失注UU","成約率","total_uu"]}

    kpi = calc_kpi(df_after)
    kpi["アポUU"] = apo_uu  # アポUUは期間内のアポ数で上書き
    return kpi


def calc_per_person_hassei(df_all, period_start, period_end):
    """発生ベースの担当者別KPI"""
    if "営業担当者" not in df_all.columns:
        return pd.DataFrame()

    rows = []
    for person, grp in df_all.groupby("営業担当者"):
        kpi = calc_kpi_hassei(grp, period_start, period_end)
        rows.append({
            "営業担当者":     person,
            "アポUU":        kpi["アポUU"],
            "プレUU":        kpi["プレUU"],
            "再プレUU":      kpi["再プレUU"],
            "プレ飛びUU":    kpi["プレ飛びUU"],
            "契約UU":        kpi["契約UU"],
            "次回契約予定UU": kpi["次回契約予定UU"],
            "失注UU":        kpi["失注UU"],
            "成約率":        kpi["成約率"],
        })
    return pd.DataFrame(rows).sort_values("アポUU", ascending=False)


# =============================================
# 着座率・プレ成約率
# =============================================

def calc_chakuza(df) -> dict:
    """プレ着座率・再プレ着座率・プレ成約率を計算する"""
    zero = {"プレ着座率": "-", "再プレ着座率": "-", "プレ成約率": "-",
            "プレ予定UU": 0, "再プレ予定UU": 0}
    if "報告種別" not in df.columns:
        return zero

    pre_uu    = col_uu(df, "報告種別", PRE_PATTERN,    regex=True, exclude_col="結果", exclude_patterns=PRE_EXCLUDE_RESULTS)
    re_pre_uu = col_uu(df, "報告種別", RE_PRE_PATTERN, regex=True, exclude_col="結果", exclude_patterns=PRE_EXCLUDE_RESULTS)

    # プレ飛びUU・再プレ飛びUU・リスケUU（着座率分母用）
    pre_noshown_uu    = col_uu(df, "報告種別", PRE_NOSHOWN_PATTERN,    regex=True)
    re_pre_noshown_uu = col_uu(df, "報告種別", RE_PRE_NOSHOWN_PATTERN, regex=True)
    pre_riske_uu = re_pre_riske_uu = 0
    if "報告種別" in df.columns and "結果" in df.columns and "顧客ID" in df.columns:
        riske_ketsu = get_col(df, "結果").str.contains(PRE_RESCHEDULED_PATTERN, na=False, regex=True)
        pre_riske_mask    = get_col(df, "報告種別").str.contains(PRE_PATTERN,    na=False, regex=True) & riske_ketsu
        re_pre_riske_mask = get_col(df, "報告種別").str.contains(RE_PRE_PATTERN, na=False, regex=True) & riske_ketsu
        pre_riske_uu    = get_col(df[pre_riske_mask],    "顧客ID").nunique()
        re_pre_riske_uu = get_col(df[re_pre_riske_mask], "顧客ID").nunique()

    # プレ予定UU（リスケ含まない着座率用）= プレUU + プレ飛びUU
    pre_yotei_old    = pre_uu + pre_noshown_uu
    re_pre_yotei_old = re_pre_uu + re_pre_noshown_uu

    # プレ予定UU（リスケ含む着座率用）= プレUU + プレ飛びUU + プレリスケUU
    pre_yotei    = pre_uu + pre_noshown_uu + pre_riske_uu
    re_pre_yotei = re_pre_uu + re_pre_noshown_uu + re_pre_riske_uu

    contract_uu = col_uu(df, "結果", CONTRACT_PATTERN, regex=True)

    return {
        "プレ着座率(リスケ含めない)":   f"{pre_uu    / pre_yotei_old    * 100:.1f}%" if pre_yotei_old    > 0 else "-",
        "再プレ着座率(リスケ含めない)": f"{re_pre_uu / re_pre_yotei_old * 100:.1f}%" if re_pre_yotei_old > 0 else "-",
        "プレ着座率":   f"{pre_uu    / pre_yotei    * 100:.1f}%" if pre_yotei    > 0 else "-",
        "再プレ着座率": f"{re_pre_uu / re_pre_yotei * 100:.1f}%" if re_pre_yotei > 0 else "-",
        "プレ成約率":   f"{contract_uu / pre_uu     * 100:.1f}%" if pre_uu       > 0 else "-",
        "プレ予定UU":   pre_yotei,
        "再プレ予定UU": re_pre_yotei,
    }


# =============================================
# アラート
# =============================================

def check_metric_alerts(kpi: dict, ganchi: dict, chakuza: dict, thresholds: dict) -> list:
    """閾値を下回った指標を [(指標名, 現在値文字列, 閾値float), ...] で返す"""
    checks = [
        ("成約率",      kpi.get("成約率", "-")),
        ("プレ言質率",  ganchi.get("プレ言質率", "-")),
        ("再プレ言質率", ganchi.get("再プレ言質率", "-")),
        ("プレ着座率",  chakuza.get("プレ着座率(リスケ含めない)", "-")),
        ("再プレ着座率", chakuza.get("再プレ着座率(リスケ含めない)", "-")),
        ("プレ成約率",  chakuza.get("プレ成約率", "-")),
    ]
    alerts = []
    for name, val_str in checks:
        if val_str == "-":
            continue
        try:
            val = float(val_str.replace("%", ""))
            thr = thresholds.get(name, DEFAULT_THRESHOLDS.get(name, 0))
            if val < thr:
                alerts.append((name, val_str, thr))
        except ValueError:
            pass
    return alerts


def get_followup_alerts(df: pd.DataFrame, days: int = 7) -> pd.DataFrame:
    """営業日から次回アクション日まで days 日以上空いている行を返す"""
    need = {"営業日", "次回アクション日"}
    if not need.issubset(df.columns):
        return pd.DataFrame()
    tmp = df.copy()
    tmp["次回アクション日"] = pd.to_datetime(tmp["次回アクション日"], errors="coerce")
    tmp = tmp.dropna(subset=["営業日", "次回アクション日"])
    tmp["空き日数"] = (tmp["次回アクション日"] - tmp["営業日"]).dt.days
    result = tmp[tmp["空き日数"] >= days]
    show = [c for c in ["営業担当者", "顧客名", "報告種別", "結果", "営業日", "次回アクション日", "空き日数"] if c in result.columns]
    return result[show].sort_values("空き日数", ascending=False)


def render_alerts(kpi: dict, ganchi: dict, chakuza: dict, df_src: pd.DataFrame, thresholds: dict):
    """担当者ごとのアラートバナーと次回予定空き顧客リストを描画する"""

    def _fmt(numerator, denominator, pct_str):
        """「率（分子/分母）」形式の文字列を返す"""
        return f"{pct_str}（{numerator}/{denominator}）"

    def _person_row(person, grp):
        pk  = calc_kpi(grp)
        pg  = calc_ganchi(grp)
        pcz = calc_chakuza(grp)
        triggered = [name for name, _, _ in check_metric_alerts(pk, pg, pcz, thresholds)]
        if not triggered:
            return None  # アラートなしはスキップ

        denom_seiyaku = pk["契約UU"] + pk["失注UU"] + pk["プレ飛びUU"] + pk["再プレ飛びUU"] + pk["契約飛びUU"]
        return {
            "営業担当者":   person,
            "🚨アラート指標": "・".join(triggered),
            "成約率":       _fmt(pk["契約UU"],     denom_seiyaku,         pk["成約率"]),
            "プレ言質率":   _fmt(pg["プレ言質UU"], pk["プレUU"],          pg["プレ言質率"]),
            "再プレ言質率": _fmt(pg["再プレ言質UU"], pk["再プレUU"],       pg["再プレ言質率"]),
            "プレ着座率":   _fmt(pk["プレUU"],      pcz["プレ予定UU"],    pcz["プレ着座率(リスケ含めない)"]),
            "再プレ着座率": _fmt(pk["再プレUU"],    pcz["再プレ予定UU"],  pcz["再プレ着座率(リスケ含めない)"]),
            "プレ成約率":   _fmt(pk["契約UU"],      pk["プレUU"],          pcz["プレ成約率"]),
        }

    # ── 担当者別アラート ──────────────────────────
    if "営業担当者" in df_src.columns and df_src["営業担当者"].nunique() > 1:
        rows = [r for r in (_person_row(p, g) for p, g in df_src.groupby("営業担当者")) if r]
        if rows:
            st.warning(f"🚨 {len(rows)}名の担当者でアラートが発生しています")
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        else:
            st.success("✅ 全担当者アラートなし")
    else:
        # 個人フィルター済みの場合
        metric_alerts = check_metric_alerts(kpi, ganchi, chakuza, thresholds)
        if metric_alerts:
            denom_seiyaku = kpi["契約UU"] + kpi["失注UU"] + kpi["プレ飛びUU"] + kpi["再プレ飛びUU"] + kpi["契約飛びUU"]
            details = {
                "成約率":       _fmt(kpi["契約UU"],       denom_seiyaku,          kpi["成約率"]),
                "プレ言質率":   _fmt(ganchi["プレ言質UU"], kpi["プレUU"],          ganchi["プレ言質率"]),
                "再プレ言質率": _fmt(ganchi["再プレ言質UU"], kpi["再プレUU"],      ganchi["再プレ言質率"]),
                "プレ着座率":   _fmt(kpi["プレUU"],        chakuza["プレ予定UU"], chakuza["プレ着座率(リスケ含めない)"]),
                "再プレ着座率": _fmt(kpi["再プレUU"],      chakuza["再プレ予定UU"], chakuza["再プレ着座率(リスケ含めない)"]),
                "プレ成約率":   _fmt(kpi["契約UU"],        kpi["プレUU"],          chakuza["プレ成約率"]),
            }
            for name, val, thr in metric_alerts:
                st.error(f"🚨 **{name}** {details.get(name, val)}　（目標：{thr}% 以上）")
        else:
            st.success("✅ アラートなし")

    # ── 次回予定7日以上空き ──────────────────────
    followup_df = get_followup_alerts(df_src)
    if not followup_df.empty:
        with st.expander(f"⏰ 次回予定まで7日以上空いている顧客：{len(followup_df)}件", expanded=True):
            st.dataframe(followup_df, use_container_width=True, hide_index=True)




# =============================================
# UI
# =============================================

def main():
    st.title("📊 商談ログ分析ダッシュボード")
    st.caption("スプレッドシートからCSVをエクスポートして読み込んでください")

    # サイドバー
    with st.sidebar:
        st.header("⚙️ 設定")

        st.subheader("データソース")
        sheets_url = st.text_input(
            "スプレッドシートURL（変更不要）",
            value=DEFAULT_SHEET_URL,
            help="商談ログ②に固定接続しています"
        )
        col_reload, col_upload = st.columns(2)
        with col_reload:
            do_fetch = st.button("🔄 取得・更新", use_container_width=True)
        with col_upload:
            uploaded_file = st.file_uploader("CSVで代用", type=["csv"], label_visibility="collapsed")

        st.divider()
        page = st.radio(
            "ページ",
            ["📈 ダッシュボード", "👥 チーム比較", "📅 予定管理"],
            horizontal=True,
            key="page_select",
        )

        with st.expander("🚨 アラート設定"):
            _settings = load_settings()
            _thr = _settings.get("thresholds", DEFAULT_THRESHOLDS.copy())
            _new_thr = {}
            for _k, _d in DEFAULT_THRESHOLDS.items():
                _new_thr[_k] = st.number_input(
                    f"{_k}（%以上）", min_value=0.0, max_value=100.0,
                    value=float(_thr.get(_k, _d)), step=1.0, key=f"thr_{_k}",
                )
            if st.button("保存", key="save_thresholds", use_container_width=True):
                _settings["thresholds"] = _new_thr
                save_settings(_settings)
                st.success("保存しました")
                st.rerun()

        st.divider()
        all_persons = st.session_state.get("all_persons", [])
        teams = load_teams()
        hidden_persons = set(teams.get("__hidden__", []))

        # 非表示メンバーを除いた一覧をフィルター選択肢に使う
        visible_persons = [p for p in all_persons if p not in hidden_persons]
        team_options = [f"【{t}】" for t in teams if t != "__hidden__"]
        selected_filter = st.selectbox(
            "フィルター（チーム / 個人）",
            ["全員"] + team_options + visible_persons,
        )

        # チーム管理UI
        with st.expander("👥 チーム管理"):
            tab_team, tab_hidden = st.tabs(["チーム編集", "非表示メンバー"])

            with tab_team:
                team_names = [t for t in teams if t != "__hidden__"]
                team_action = st.selectbox(
                    "チームを選ぶ",
                    ["新規作成"] + team_names,
                    key="team_manage_select",
                )
                if team_action == "新規作成":
                    new_name = st.text_input("チーム名", key="new_team_name")
                    new_members = st.multiselect("メンバー", visible_persons, key="new_team_members")
                    if st.button("作成", key="create_team") and new_name:
                        if new_name not in teams:
                            teams[new_name] = new_members
                            save_teams(teams)
                            st.success(f"「{new_name}」を作成しました")
                            st.rerun()
                        else:
                            st.warning("同じ名前のチームがすでに存在します")
                else:
                    current = [m for m in teams.get(team_action, []) if m in all_persons]
                    updated_members = st.multiselect(
                        "メンバー（変更して保存）",
                        all_persons,
                        default=current,
                        key=f"edit_team_{team_action}",
                    )
                    col_save, col_del = st.columns(2)
                    if col_save.button("保存", key="save_team", use_container_width=True):
                        teams[team_action] = updated_members
                        save_teams(teams)
                        st.success("保存しました")
                        st.rerun()
                    if col_del.button("削除", key="del_team", use_container_width=True):
                        del teams[team_action]
                        save_teams(teams)
                        st.rerun()

            with tab_hidden:
                st.caption("退職者など、フィルターに表示したくないメンバーを設定します")
                # 非表示リストに保存済みの人はデータから消えていても選択肢に残す
                hidden_options = sorted(set(all_persons) | hidden_persons)
                updated_hidden = st.multiselect(
                    "非表示にするメンバー",
                    hidden_options,
                    default=[m for m in hidden_persons if m in hidden_options],
                    key="hidden_members_edit",
                )
                if st.button("保存", key="save_hidden", use_container_width=True):
                    teams["__hidden__"] = updated_hidden
                    save_teams(teams)
                    st.success("保存しました")
                    st.rerun()

    # =============================================
    # データ読み込み（URLまたはCSV）
    # =============================================
    if sheets_url and (do_fetch or "df_cache" not in st.session_state):
        with st.spinner("スプレッドシートからデータを取得中..."):
            try:
                df_all = fetch_from_sheets_url(sheets_url)
                df_all = normalize_columns(df_all)
                st.session_state["df_cache"] = df_all
                st.success(f"取得完了：{len(df_all):,}行")
            except Exception as e:
                st.error(f"取得エラー：{e}")
                st.info("スプレッドシートが取得できませんでした。CSVアップロードをお試しください。")

    if uploaded_file is not None:
        df_all = load_data(uploaded_file)
        df_all = normalize_columns(df_all)
        st.session_state["df_cache"] = df_all
    elif "df_cache" in st.session_state:
        df_all = st.session_state["df_cache"]
    else:
        st.info("👈 サイドバーにスプレッドシートのURLを入力して「取得・更新」を押してください")
        st.markdown("""
### スプレッドシートの連携方法

**「ウェブに公開」URL（推奨）**
1. スプレッドシートを開く
2. **ファイル → 共有 → ウェブに公開**
3. 対象シート（商談ログ②など）を選んで **CSV** を選択
4. **公開** → URLをコピーして上に貼り付け

公開後は「取得・更新」ボタンを押すだけで最新データに更新されます。
        """)
        return

    if "営業担当者" in df_all.columns:
        st.session_state["all_persons"] = sorted(df_all["営業担当者"].dropna().unique().tolist())

    today = pd.Timestamp.now().normalize()

    # フィルター適用：チーム選択時は df_all をチームメンバーに絞り込む
    if selected_filter.startswith("【") and selected_filter.endswith("】"):
        team_name = selected_filter[1:-1]
        team_members = load_teams().get(team_name, [])
        if "営業担当者" in df_all.columns and team_members:
            df_all = df_all[df_all["営業担当者"].isin(team_members)]
        selected_person = "全員"
        person_label = selected_filter
    elif selected_filter == "全員":
        selected_person = "全員"
        person_label = "全員"
    else:
        selected_person = selected_filter
        person_label = selected_filter

    def pick_period(section_key: str, default_period: int = 1):
        """期間ラジオ＋日付入力を描画し (start, end, label) を返す"""
        period = st.radio(
            "集計期間", ["昨日", "3日", "1週間", "2週間", "その他"],
            index=default_period, horizontal=True, key=f"period_{section_key}",
        )
        if period == "その他":
            c1, c2 = st.columns(2)
            sd = c1.date_input("開始日", value=today.date() - timedelta(weeks=1),
                               min_value=DATE_MIN, max_value=DATE_MAX, key=f"period_s_{section_key}")
            ed = c2.date_input("終了日", value=today.date(),
                               min_value=DATE_MIN, max_value=DATE_MAX, key=f"period_e_{section_key}")
            start, end = pd.Timestamp(sd), pd.Timestamp(ed)
        elif period == "昨日":
            start = today - timedelta(days=1)
            end   = today - timedelta(days=1)
        else:
            days = {"3日": 3, "1週間": 7, "2週間": 14}[period]
            start = today - timedelta(days=days)
            end   = today
        label = f"{period}　{start.strftime('%m/%d')}〜{end.strftime('%m/%d')}"
        # デバッグ欄が同じ期間を使えるよう保存
        st.session_state[f"_start_{section_key}"] = start
        st.session_state[f"_end_{section_key}"]   = end
        return start, end, label

    def render_kpi_section(date_col: str, section_key: str, title: str, default_period: int):
        """KPIサマリーを1セクション描画する。date_colでフィルター日付列を指定。"""
        if date_col not in df_all.columns:
            st.warning(f"列「{date_col}」がデータに見つかりません")
            return

        start, end, period_label = pick_period(section_key, default_period)
        df_filtered = df_all[(df_all[date_col] >= start) & (df_all[date_col] <= end)]

        # 担当者フィルター
        if selected_person != "全員" and "営業担当者" in df_filtered.columns:
            df_person = df_filtered[df_filtered["営業担当者"] == selected_person]
        else:
            df_person = df_filtered

        kpi = calc_kpi(df_person)
        per_person_df = calc_per_person(df_filtered) if selected_person == "全員" else pd.DataFrame()
        g  = calc_ganchi(df_person)
        cz = calc_chakuza(df_person)
        thresholds = load_settings().get("thresholds", DEFAULT_THRESHOLDS.copy())

        st.subheader(f"{title}（{period_label} / {person_label}）")

        # アラートバナー
        with st.expander("🚨 アラート確認", expanded=True):
            render_alerts(kpi, g, cz, df_person, thresholds)

        r1 = st.columns(7)
        r1[0].metric("総UU",           kpi["total_uu"],        help="期間内のユニーク顧客数")
        r1[1].metric("アポUU",         kpi["アポUU"],          help="報告種別＝アポ（完全一致）")
        r1[2].metric("プレUU",         kpi["プレUU"],          help="報告種別＝プレ（リスケ・日程確定除く）")
        r1[3].metric("再プレUU",       kpi["再プレUU"],        help="報告種別＝再プレ（リスケ・日程確定除く）")
        r1[4].metric("契約UU",         kpi["契約UU"],          help="結果＝契約（完全一致）")
        r1[5].metric("次回契約予定UU", kpi["次回契約予定UU"],  help="結果に次回契約予定を含む")
        r1[6].metric("成約率",         kpi["成約率"],          help="契約÷（契約＋失注＋プレ飛び＋再プレ飛び＋契約飛び）")

        r2 = st.columns(7)
        r2[0].metric("プレ飛びUU",     kpi["プレ飛びUU"],      help="報告種別にプレ飛びを含む")
        r2[1].metric("再プレ飛びUU",   kpi["再プレ飛びUU"],    help="報告種別に再プレ飛びを含む")
        r2[2].metric("契約飛びUU",     kpi["契約飛びUU"],      help="契約予定飛び or 契約予定調整＋失注")
        r2[3].metric("失注UU",         kpi["失注UU"],          help="報告種別または結果＝失注")
        r2[4].metric("プレ言質UU",     g["プレ言質UU"],        help="最後がプレ→契約or次回契約予定")
        r2[5].metric("再プレ言質UU",   g["再プレ言質UU"],      help="最後が再プレ→契約or次回契約予定")
        r2[6].metric("プレ言質率",     g["プレ言質率"],        help="プレ言質UU ÷ プレUU")

        r3 = st.columns(7)
        r3[0].metric("再プレ言質率",           g["再プレ言質率"],                    help="再プレ言質UU ÷ 再プレUU")
        r3[1].metric("プレ着座率(リスケ含めない)",   cz["プレ着座率(リスケ含めない)"],   help="プレUU ÷（プレUU+プレ飛びUU）")
        r3[2].metric("再プレ着座率(リスケ含めない)", cz["再プレ着座率(リスケ含めない)"], help="再プレUU ÷（再プレUU+再プレ飛びUU）")
        r3[3].metric("プレ着座率",             cz["プレ着座率"],                     help="プレUU ÷（プレUU+プレ飛びUU+プレリスケUU）")
        r3[4].metric("再プレ着座率",           cz["再プレ着座率"],                   help="再プレUU ÷（再プレUU+再プレ飛びUU+再プレリスケUU）")
        r3[5].metric("プレ成約率",             cz["プレ成約率"],                     help="契約UU ÷ プレUU")

        r4 = st.columns(7)
        r4[0].metric("プレリスケUU",   kpi["プレリスケUU"],    help="報告種別＝プレ かつ 結果にリスケを含む")
        r4[1].metric("再プレリスケUU", kpi["再プレリスケUU"],  help="報告種別＝再プレ かつ 結果にリスケを含む")
        r4[2].metric("ブリッジUU",     kpi["ブリッジUU"],      help="報告種別＝アポ かつ 結果にプレ日程確定を含む")
        r4[3].metric("契約リスケUU",   kpi["契約リスケUU"],    help="報告種別に契約を含む かつ 結果にリスケを含む")

        if selected_person == "全員" and not per_person_df.empty:
            with st.expander("👥 営業担当者別実績を見る"):
                fig = go.Figure()
                fig.add_trace(go.Bar(name="アポUU",   x=per_person_df["営業担当者"], y=per_person_df["アポUU"],   marker_color="#42A5F5"))
                fig.add_trace(go.Bar(name="プレUU",   x=per_person_df["営業担当者"], y=per_person_df["プレUU"],   marker_color="#66BB6A"))
                fig.add_trace(go.Bar(name="再プレUU", x=per_person_df["営業担当者"], y=per_person_df["再プレUU"], marker_color="#AB47BC"))
                fig.add_trace(go.Bar(name="契約UU",   x=per_person_df["営業担当者"], y=per_person_df["契約UU"],   marker_color="#FFA726"))
                fig.update_layout(barmode="group", height=380, xaxis_tickangle=-30)
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(per_person_df, use_container_width=True, hide_index=True)

        return kpi, df_person, per_person_df

    # =============================================
    # ページ1：ダッシュボード
    # =============================================
    if page == "📈 ダッシュボード":
        st.markdown("## 📈 実施ベース")
        st.caption("営業日（実際に商談が行われた日）でフィルター")
        result_jisshi = render_kpi_section("営業日", "jisshi", "KPIサマリー", default_period=2)

        st.divider()

        # =============================================
        # 発生ベース（アポ実施日起点コホート）
        # =============================================
        st.markdown("## 📋 発生ベース")
        st.caption("期間内にアポした顧客を起点に、アポ日以降のプレ・契約・失注を集計")

        h_start, h_end, h_label = pick_period("hassei", default_period=2)

        if selected_person == "全員" or "営業担当者" not in df_all.columns:
            df_hassei_src = df_all
        else:
            df_hassei_src = df_all[df_all["営業担当者"] == selected_person]

        h_kpi = calc_kpi_hassei(df_hassei_src, h_start, h_end)
        h_per_person = calc_per_person_hassei(df_all, h_start, h_end) if selected_person == "全員" else pd.DataFrame()

        h_df_after, _ = build_hassei_df(df_hassei_src, h_start, h_end)
        h_g = calc_ganchi(h_df_after) if not h_df_after.empty else {"プレ言質UU": 0, "再プレ言質UU": 0, "プレ言質率": "-", "再プレ言質率": "-"}

        st.subheader(f"KPIサマリー（{h_label} / {person_label}）")
        st.caption("アポ→プレ→契約 の転換率を見る指標です")

        apo_uu = h_kpi["アポUU"]
        pre_rate = f"{h_kpi['プレUU'] / apo_uu * 100:.1f}%" if apo_uu > 0 else "-"

        hr1 = st.columns(7)
        hr1[0].metric("アポUU（起点）",  h_kpi["アポUU"],          help="期間内にアポした顧客数")
        hr1[1].metric("プレUU",          h_kpi["プレUU"],          help="アポ後にプレした顧客数")
        hr1[2].metric("再プレUU",        h_kpi["再プレUU"],        help="アポ後に再プレした顧客数")
        hr1[3].metric("契約UU",          h_kpi["契約UU"],          help="アポ後に契約した顧客数")
        hr1[4].metric("次回契約予定UU",  h_kpi["次回契約予定UU"],  help="アポ後に次回契約予定の顧客数")
        hr1[5].metric("成約率",          h_kpi["成約率"],          help="契約÷（契約＋失注＋プレ飛び＋再プレ飛び＋契約飛び）")
        hr1[6].metric("アポ→プレ率",     pre_rate,                 help="プレUU ÷ アポUU")

        hr2 = st.columns(7)
        hr2[0].metric("プレ飛びUU",      h_kpi["プレ飛びUU"],      help="アポ後にプレ飛びした顧客数")
        hr2[1].metric("再プレ飛びUU",    h_kpi["再プレ飛びUU"],    help="アポ後に再プレ飛びした顧客数")
        hr2[2].metric("契約飛びUU",      h_kpi["契約飛びUU"],      help="アポ後に契約飛びした顧客数")
        hr2[3].metric("失注UU",          h_kpi["失注UU"],          help="アポ後に失注した顧客数")
        hr2[4].metric("プレ言質UU",      h_g["プレ言質UU"],        help="最後がプレ→契約or次回契約予定")
        hr2[5].metric("再プレ言質UU",    h_g["再プレ言質UU"],      help="最後が再プレ→契約or次回契約予定")
        hr2[6].metric("プレ言質率",      h_g["プレ言質率"],        help="プレ言質UU ÷ プレUU")

        hr3 = st.columns(7)
        hr3[0].metric("再プレ言質率",    h_g["再プレ言質率"],      help="再プレ言質UU ÷ 再プレUU")

        if selected_person == "全員" and not h_per_person.empty:
            with st.expander("👥 営業担当者別実績を見る"):
                fig_h = go.Figure()
                fig_h.add_trace(go.Bar(name="アポUU", x=h_per_person["営業担当者"], y=h_per_person["アポUU"], marker_color="#42A5F5"))
                fig_h.add_trace(go.Bar(name="プレUU", x=h_per_person["営業担当者"], y=h_per_person["プレUU"], marker_color="#66BB6A"))
                fig_h.add_trace(go.Bar(name="契約UU", x=h_per_person["営業担当者"], y=h_per_person["契約UU"], marker_color="#FFA726"))
                fig_h.update_layout(barmode="group", height=380, xaxis_tickangle=-30)
                st.plotly_chart(fig_h, use_container_width=True)
                st.dataframe(h_per_person, use_container_width=True, hide_index=True)

        st.divider()

        kpi = result_jisshi[0] if result_jisshi else None
        df_person = result_jisshi[1] if result_jisshi else pd.DataFrame()
        per_person_df = result_jisshi[2] if result_jisshi else pd.DataFrame()

        # =============================================
        # デバッグ：指標の内訳確認
        # =============================================
        with st.expander("🔍 数値の内訳を確認する（デバッグ用）"):
            debug_metric = st.selectbox(
                "確認したい指標",
                ["プレ", "アポ", "再プレ", "プレ飛び", "契約", "失注", "プレ言質", "再プレ言質", "プレリスケ", "再プレリスケ"],
                key="debug_metric"
            )
            debug_base = st.radio("ベース", ["実施ベース（営業日）", "発生ベース（タイムスタンプ）"], horizontal=True, key="debug_base")

            _base_key = "jisshi" if "実施" in debug_base else "hassei"
            d_start = st.session_state.get(f"_start_{_base_key}", today - timedelta(days=7))
            d_end   = st.session_state.get(f"_end_{_base_key}",   today)
            st.caption(f"集計期間：{d_start.strftime('%m/%d')}〜{d_end.strftime('%m/%d')}")

            if "発生" in debug_base:
                # 発生ベース：アポ起点コホートを使う
                d_src = df_all if selected_person == "全員" else df_all[df_all["営業担当者"] == selected_person] if "営業担当者" in df_all.columns else df_all
                d_df, _ = build_hassei_df(d_src, d_start, d_end)
                if not d_df.empty and selected_person != "全員" and "営業担当者" in d_df.columns:
                    d_df = d_df[d_df["営業担当者"] == selected_person]
            else:
                # 実施ベース：営業日で絞る
                d_df = df_all[(df_all["営業日"] >= d_start) & (df_all["営業日"] <= d_end)].copy()
                if selected_person != "全員" and "営業担当者" in d_df.columns:
                    d_df = d_df[d_df["営業担当者"] == selected_person]

            PAT_MAP = {
                "プレ":     (PRE_PATTERN,          "報告種別", PRE_EXCLUDE_RESULTS),
                "アポ":     (APO_PATTERN,           "報告種別", None),
                "再プレ":   (RE_PRE_PATTERN,        "報告種別", PRE_EXCLUDE_RESULTS),
                "プレ飛び": (PRE_NOSHOWN_PATTERN,   "報告種別", None),
                "契約":     (CONTRACT_PATTERN,      "結果",     None),
                "失注":     (LOST_PATTERN,          "報告種別", None),
            }

            if debug_metric in ("プレリスケ", "再プレリスケ"):
                # リスケ系：報告種別=プレ/再プレ かつ 結果=リスケ日程確定orリスケ日程不明
                hoko_pat = PRE_PATTERN if debug_metric == "プレリスケ" else RE_PRE_PATTERN
                if "報告種別" in d_df.columns and "結果" in d_df.columns:
                    riske_mask = (
                        get_col(d_df, "報告種別").str.contains(hoko_pat, na=False, regex=True)
                        & get_col(d_df, "結果").str.contains(PRE_RESCHEDULED_PATTERN, na=False, regex=True)
                    )
                    d_result = d_df[riske_mask]
                    show_cols = [c for c in ["営業日", "営業担当者", "顧客名", "報告種別", "結果"] if c in d_result.columns]
                    uu = get_col(d_result, "顧客ID").nunique() if "顧客ID" in d_result.columns else "-"
                    st.write(f"**{debug_metric}** の該当行：{len(d_result)}件 / UU：{uu}件")
                    st.dataframe(d_result[show_cols].sort_values("営業日", ascending=False), use_container_width=True, hide_index=True)

                    # 診断：条件ごとにどこで外れているか確認
                    with st.expander("🔍 診断：条件の絞り込み確認"):
                        mask_hoko = get_col(d_df, "報告種別").str.contains(hoko_pat, na=False, regex=True)
                        mask_ketsu = get_col(d_df, "結果").str.contains(PRE_RESCHEDULED_PATTERN, na=False, regex=True)
                        st.write(f"① 報告種別マッチ行数：{mask_hoko.sum()}件")
                        st.write(f"② 結果マッチ行数：{mask_ketsu.sum()}件")
                        st.write(f"③ 両方マッチ（AND）：{(mask_hoko & mask_ketsu).sum()}件")
                        st.write(f"使用パターン　報告種別：`{hoko_pat}`　結果：`{PRE_RESCHEDULED_PATTERN}`")
                        # 報告種別マッチ行の結果値を全表示
                        hoko_all = d_df[mask_hoko]
                        if not hoko_all.empty:
                            diag_cols = [c for c in ["営業日", "営業担当者", "顧客名", "報告種別", "結果"] if c in hoko_all.columns]
                            st.write("報告種別マッチ行の結果（repr）:", [repr(v) for v in get_col(hoko_all, "結果").tolist()])
                            st.dataframe(hoko_all[diag_cols].sort_values("営業日", ascending=False), use_container_width=True, hide_index=True)
                else:
                    st.info("必要な列がありません")
            elif debug_metric in ("プレ言質", "再プレ言質"):
                # 言質系：calc_ganchiと同じロジックで顧客一覧を表示
                pre_mask    = get_col(d_df, "報告種別").str.contains(PRE_PATTERN,    na=False, regex=True) if "報告種別" in d_df.columns else pd.Series(False, index=d_df.index)
                re_pre_mask = get_col(d_df, "報告種別").str.contains(RE_PRE_PATTERN, na=False, regex=True) if "報告種別" in d_df.columns else pd.Series(False, index=d_df.index)
                if "結果" in d_df.columns:
                    for ep in PRE_EXCLUDE_RESULTS:
                        excl_mask = get_col(d_df, "結果").str.contains(ep, na=False, regex=True)
                        pre_mask    &= ~excl_mask
                        re_pre_mask &= ~excl_mask
                pre_re_df = d_df[pre_mask | re_pre_mask]
                if not pre_re_df.empty and "顧客ID" in pre_re_df.columns:
                    last_row = pre_re_df.sort_values("営業日").groupby("顧客ID").last().reset_index()
                    last_row["is_pre"] = get_col(last_row, "報告種別").str.contains(PRE_PATTERN, na=False, regex=True)
                    if debug_metric == "プレ言質":
                        target_ids = set(get_col(last_row[last_row["is_pre"]], "顧客ID"))
                    else:
                        target_ids = set(get_col(last_row[~last_row["is_pre"]], "顧客ID"))
                    ganchi_mask = (
                        get_col(d_df, "結果").str.contains(CONTRACT_PATTERN,      na=False, regex=True)
                        | get_col(d_df, "結果").str.contains(NEXT_CONTRACT_PATTERN, na=False, regex=True)
                    ) if "結果" in d_df.columns else pd.Series(False, index=d_df.index)
                    ganchi_ids = set(get_col(d_df[ganchi_mask], "顧客ID")) if "顧客ID" in d_df.columns else set()
                    hit_ids = target_ids & ganchi_ids
                    st.write(f"**{debug_metric}UU：{len(hit_ids)}件**　（最後のプレ/再プレ該当：{len(target_ids)}件 × 契約言質あり：{len(ganchi_ids)}件）")
                    if hit_ids and "顧客ID" in d_df.columns:
                        # 各顧客の最後のプレ/再プレ行と契約言質行を表示
                        last_pre = last_row[last_row["顧客ID"].isin(hit_ids)][["顧客ID", "営業日", "報告種別"]].rename(columns={"営業日": "最終プレ/再プレ日"})
                        ganchi_rows = d_df[ganchi_mask & d_df["顧客ID"].isin(hit_ids)].sort_values("営業日").groupby("顧客ID").last().reset_index()[["顧客ID", "営業日", "結果"]].rename(columns={"営業日": "言質日"})
                        merged = last_pre.merge(ganchi_rows, on="顧客ID", how="left")
                        if "顧客名" in d_df.columns:
                            names = d_df[["顧客ID", "顧客名"]].drop_duplicates("顧客ID")
                            merged = merged.merge(names, on="顧客ID", how="left")
                        show = [c for c in ["顧客名", "顧客ID", "最終プレ/再プレ日", "報告種別", "言質日", "結果"] if c in merged.columns]
                        st.dataframe(merged[show], use_container_width=True, hide_index=True)
                    else:
                        st.info("該当顧客なし")
                else:
                    st.info("プレ/再プレ行がありません")
            elif debug_metric in PAT_MAP:
                pat, col, excl = PAT_MAP[debug_metric]
                if col in d_df.columns:
                    mask = get_col(d_df, col).str.contains(pat, na=False, regex=True)
                    if excl and "結果" in d_df.columns:
                        for ep in excl:
                            mask &= ~get_col(d_df, "結果").str.contains(ep, na=False, regex=True)
                    d_result = d_df[mask]
                    show_cols = [c for c in ["営業日", "営業担当者", "顧客名", "報告種別", "結果"] if c in d_result.columns]
                    st.write(f"**{debug_metric}** の該当行：{len(d_result)}件 / UU：{get_col(d_result, '顧客ID').nunique() if '顧客ID' in d_result.columns else '-'}件")
                    st.dataframe(d_result[show_cols].sort_values("営業日", ascending=False), use_container_width=True, hide_index=True)

        # =============================================
        # 直近1週間 DAY推移
        # =============================================
        if "営業日" in df_all.columns:
            st.subheader("📅 直近1週間のDAY推移")

            TREND_METRICS = {
                "アポ数":    (APO_PATTERN,            "報告種別", None),
                "プレ数":    (PRE_PATTERN,             "報告種別", PRE_EXCLUDE_RESULTS),
                "契約数":    (CONTRACT_PATTERN,        "結果",     None),
                "再プレ数":  (RE_PRE_PATTERN,          "報告種別", PRE_EXCLUDE_RESULTS),
                "プレリスケ数": (PRE_RESCHEDULED_PATTERN, "結果",  None),
                "プレ飛び数": (PRE_NOSHOWN_PATTERN,   "報告種別", None),
            }
            selected_metric = st.radio("指標を選択", list(TREND_METRICS.keys()), horizontal=True)

            week_start = today - timedelta(weeks=1)
            df_week = df_all[df_all["営業日"] >= week_start].copy()

            pat, col, excl = TREND_METRICS[selected_metric]
            if col in df_week.columns:
                mask = get_col(df_week, col).str.contains(pat, na=False, regex=True)
                if excl:
                    for ep in excl:
                        if "結果" in df_week.columns:
                            mask &= ~get_col(df_week, "結果").str.contains(ep, na=False, regex=True)
                df_filtered = df_week[mask]

                if selected_person != "全員" and "営業担当者" in df_filtered.columns:
                    df_filtered = df_filtered[df_filtered["営業担当者"] == selected_person]

                daily = df_filtered.groupby(df_filtered["営業日"].dt.date).size().reset_index()
                daily.columns = ["日付", "件数"]
                all_days = pd.date_range(week_start, today).date
                daily = daily.set_index("日付").reindex(all_days, fill_value=0).reset_index()
                daily.columns = ["日付", "件数"]

                fig2 = px.bar(daily, x="日付", y="件数", color_discrete_sequence=["#2196F3"])
                fig2.update_layout(height=320, xaxis_tickformat="%m/%d")
                st.plotly_chart(fig2, use_container_width=True)

    # =============================================
    # ページ2：チーム比較
    # =============================================
    elif page == "👥 チーム比較":
        st.markdown("## 👥 チーム比較")
        st.caption("登録済みチームと全体のKPIを並べて比較できます")

        df_raw = st.session_state.get("df_cache", pd.DataFrame())
        all_teams_cfg = load_teams()
        team_names_cfg = [t for t in all_teams_cfg if t != "__hidden__"]

        if not team_names_cfg:
            st.info("サイドバーの「チーム管理」でチームを登録すると、ここにチーム別の比較が表示されます")
        elif "営業日" not in df_raw.columns:
            st.warning("データを取得してください")
        else:
            # 全体 + 各チームのデータビュー
            views = [("全体", df_raw)]
            for tname in team_names_cfg:
                members = all_teams_cfg[tname]
                if members and "営業担当者" in df_raw.columns:
                    views.append((tname, df_raw[df_raw["営業担当者"].isin(members)]))

            CHART_COLORS = ["#90A4AE", "#42A5F5", "#66BB6A", "#AB47BC", "#FFA726",
                            "#EF5350", "#26C6DA", "#D4E157", "#FF7043", "#7E57C2"]

            def render_compare_section(summary_df, section_key, period_label):
                """集計テーブル＋グラフ比較を描画する共通関数"""
                st.dataframe(summary_df, use_container_width=True, hide_index=True)
                st.markdown("#### グラフ比較")
                metric_opts = ["アポUU", "プレUU", "再プレUU", "契約UU", "プレ飛びUU", "失注UU"]
                sel_metric = st.radio("指標を選択", metric_opts, horizontal=True, key=f"comp_metric_{section_key}")
                fig = go.Figure()
                for i, row in summary_df.iterrows():
                    fig.add_trace(go.Bar(
                        name=row["チーム"],
                        x=[row["チーム"]],
                        y=[row[sel_metric]],
                        marker_color=CHART_COLORS[i % len(CHART_COLORS)],
                        text=[row[sel_metric]],
                        textposition="outside",
                    ))
                fig.update_layout(showlegend=False, height=350, yaxis_title=sel_metric)
                st.plotly_chart(fig, use_container_width=True, key=f"chart_compare_{section_key}")

            cp_thresholds = load_settings().get("thresholds", DEFAULT_THRESHOLDS.copy())

            def _alert_flags(k, g, cz):
                """閾値を下回った指標名をカンマ区切りで返す（チーム比較テーブル用）"""
                triggered = [name for name, _, _ in check_metric_alerts(k, g, cz, cp_thresholds)]
                return "🚨 " + "・".join(triggered) if triggered else "✅"

            # ── 実施ベース ──────────────────────────────
            st.markdown("### 📈 実施ベース")
            st.caption("営業日でフィルター")
            j_start, j_end, j_label = pick_period("comp_j", default_period=2)

            j_rows = []
            for label, df_v in views:
                df_cp = df_v[(df_v["営業日"] >= j_start) & (df_v["営業日"] <= j_end)]
                k  = calc_kpi(df_cp)
                g  = calc_ganchi(df_cp)
                cz = calc_chakuza(df_cp)
                j_rows.append({
                    "チーム": label,
                    "アラート": _alert_flags(k, g, cz),
                    "アポUU": k["アポUU"], "プレUU": k["プレUU"],
                    "再プレUU": k["再プレUU"], "契約UU": k["契約UU"],
                    "次回契約予定UU": k["次回契約予定UU"], "プレ飛びUU": k["プレ飛びUU"],
                    "再プレ飛びUU": k["再プレ飛びUU"], "契約飛びUU": k["契約飛びUU"],
                    "失注UU": k["失注UU"], "成約率": k["成約率"],
                    "プレ着座率": cz["プレ着座率"], "再プレ着座率": cz["再プレ着座率"],
                    "プレ成約率": cz["プレ成約率"],
                    "プレ言質率": g["プレ言質率"], "再プレ言質率": g["再プレ言質率"],
                })
            st.subheader(f"チーム別集計（{j_label}）")
            render_compare_section(pd.DataFrame(j_rows), "j", j_label)

            st.divider()

            # ── 発生ベース ──────────────────────────────
            st.markdown("### 📋 発生ベース")
            st.caption("期間内アポを起点に、アポ日以降の転換を集計")
            h_start2, h_end2, h_label2 = pick_period("comp_h", default_period=2)

            h_rows = []
            for label, df_v in views:
                k  = calc_kpi_hassei(df_v, h_start2, h_end2)
                df_after, _ = build_hassei_df(df_v, h_start2, h_end2)
                g  = calc_ganchi(df_after) if not df_after.empty else {"プレ言質率": "-", "再プレ言質率": "-"}
                cz = calc_chakuza(df_after) if not df_after.empty else {"プレ着座率": "-", "再プレ着座率": "-", "プレ成約率": "-"}
                h_rows.append({
                    "チーム": label,
                    "アラート": _alert_flags(k, g, cz),
                    "アポUU": k["アポUU"], "プレUU": k["プレUU"],
                    "再プレUU": k["再プレUU"], "契約UU": k["契約UU"],
                    "次回契約予定UU": k["次回契約予定UU"], "プレ飛びUU": k["プレ飛びUU"],
                    "再プレ飛びUU": k["再プレ飛びUU"], "契約飛びUU": k["契約飛びUU"],
                    "失注UU": k["失注UU"], "成約率": k["成約率"],
                    "プレ着座率": cz["プレ着座率"], "再プレ着座率": cz["再プレ着座率"],
                    "プレ成約率": cz["プレ成約率"],
                    "プレ言質率": g.get("プレ言質率", "-"), "再プレ言質率": g.get("再プレ言質率", "-"),
                })
            st.subheader(f"チーム別集計（{h_label2}）")
            render_compare_section(pd.DataFrame(h_rows), "h", h_label2)


    # =============================================
    # ページ3：予定管理
    # =============================================
    elif page == "📅 予定管理":
        st.markdown("## 📅 予定管理")
        st.caption("次回アクション日をもとに、プレ・再プレ・次回契約予定の件数とリストを確認できます")

        df_raw = st.session_state.get("df_cache", pd.DataFrame())
        all_teams_cfg = load_teams()
        team_names_cfg = [t for t in all_teams_cfg if t != "__hidden__"]

        if df_raw.empty or "次回アクション日" not in df_raw.columns:
            st.warning("データを取得してください")
        else:
            # 次回アクション日を日付型に変換
            df_sch = df_raw.copy()
            df_sch["次回アクション日"] = pd.to_datetime(df_sch["次回アクション日"], errors="coerce")
            df_sch = df_sch.dropna(subset=["次回アクション日"])

            # 予定種別を判定する列を追加
            # 結果：プレ日程確定→プレ予定 / 再プレ日程確定→再プレ予定 / 次回契約予定→契約予定
            # 結果：リスケ日程確定 → 報告種別がプレ→プレ予定 / 再プレ→再プレ予定
            def classify_yotei(row):
                ketsu = str(row.get("結果", "")) if pd.notna(row.get("結果")) else ""
                hoko  = str(row.get("報告種別", "")) if pd.notna(row.get("報告種別")) else ""
                if "プレ日程確定" in ketsu and "再プレ" not in ketsu:
                    return "プレ予定"
                if "再プレ日程確定" in ketsu:
                    return "再プレ予定"
                if "次回契約予定" in ketsu:
                    return "契約予定"
                if "リスケ日程確定" in ketsu:
                    if re.search(r"^再プレ$", hoko):
                        return "再プレ予定"
                    if re.search(r"^プレ$", hoko):
                        return "プレ予定"
                return None

            df_sch["予定種別"] = df_sch.apply(classify_yotei, axis=1)
            df_sch = df_sch.dropna(subset=["予定種別"])

            # 期間選択
            sch_period = st.radio(
                "表示期間",
                ["今日", "1週間", "その他"],
                horizontal=True,
                key="sch_period",
            )
            if sch_period == "今日":
                sch_start = today
                sch_end   = today
            elif sch_period == "1週間":
                sch_start = today
                sch_end   = today + timedelta(weeks=1)
            else:
                sc1, sc2 = st.columns(2)
                sch_sd = sc1.date_input("開始日", value=today.date(), min_value=DATE_MIN, max_value=DATE_MAX, key="sch_s")
                sch_ed = sc2.date_input("終了日", value=(today + timedelta(weeks=1)).date(), min_value=DATE_MIN, max_value=DATE_MAX, key="sch_e")
                sch_start = pd.Timestamp(sch_sd)
                sch_end   = pd.Timestamp(sch_ed)

            sch_label = f"{sch_start.strftime('%m/%d')}〜{sch_end.strftime('%m/%d')}"

            # 期間フィルター
            df_sch = df_sch[
                (df_sch["次回アクション日"] >= sch_start) &
                (df_sch["次回アクション日"] <= sch_end + timedelta(hours=23, minutes=59, seconds=59))
            ]

            ACTION_TARGETS = ["プレ予定", "再プレ予定", "契約予定"]
            # 営業日→次回アクション日の空き日数を計算
            if "営業日" in df_sch.columns:
                df_sch["空き日数"] = (df_sch["次回アクション日"] - df_sch["営業日"]).dt.days

            # 全体 + チームのビュー構築
            views_sch = [("全体", df_sch)]
            for tname in team_names_cfg:
                members = all_teams_cfg[tname]
                if members and "営業担当者" in df_sch.columns:
                    views_sch.append((tname, df_sch[df_sch["営業担当者"].isin(members)]))

            view_labels = [v[0] for v in views_sch]
            view_tabs   = st.tabs(view_labels)

            show_cols = [c for c in ["次回アクション日", "予定種別", "営業担当者", "顧客名", "報告種別", "結果", "空き日数"] if c in df_sch.columns]

            def style_rows(df_display):
                """空き日数が8日以上の行をオレンジ背景にする"""
                def row_style(row):
                    if "空き日数" in row.index and pd.notna(row["空き日数"]) and row["空き日数"] >= 8:
                        return ["background-color: #FFE0B2; color: #000"] * len(row)
                    return [""] * len(row)
                return df_display.style.apply(row_style, axis=1)

            for (label, df_v), vtab in zip(views_sch, view_tabs):
                with vtab:
                    st.subheader(f"予定一覧（{sch_label} / {label}）")

                    if df_v.empty:
                        st.info("該当する予定がありません")
                        continue

                    # サマリー件数（予定種別ごと）
                    summary_cols = st.columns(len(ACTION_TARGETS))
                    for i, action in enumerate(ACTION_TARGETS):
                        count = (df_v["予定種別"] == action).sum()
                        summary_cols[i].metric(action, f"{count}件")

                    if "空き日数" in df_v.columns:
                        overdue = (df_v["空き日数"] >= 8).sum()
                        if overdue > 0:
                            st.warning(f"🟠 営業日から8日以上経過：{overdue}件")

                    st.divider()

                    # 営業担当者を軸にリスト表示
                    if "営業担当者" in df_v.columns:
                        for person, grp in df_v.sort_values("次回アクション日").groupby("営業担当者"):
                            disp = grp[show_cols].sort_values("次回アクション日").reset_index(drop=True)
                            overdue_count = (disp["空き日数"] >= 8).sum() if "空き日数" in disp.columns else 0
                            label_person = f"👤 {person}（{len(grp)}件）" + (f"　🟠{overdue_count}件超過" if overdue_count > 0 else "")
                            with st.expander(label_person, expanded=True):
                                st.dataframe(style_rows(disp), use_container_width=True, hide_index=True)
                    else:
                        disp = df_v[show_cols].sort_values("次回アクション日")
                        st.dataframe(style_rows(disp), use_container_width=True, hide_index=True)




if __name__ == "__main__":
    main()
