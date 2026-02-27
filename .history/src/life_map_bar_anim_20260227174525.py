from __future__ import annotations

from pathlib import Path
import time
import requests
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import geopandas as gpd
import re

def cached_years() -> list[int]:
    years = []
    pat = re.compile(r"life_(\d{4})\.csv$")
    for p in CACHE_DIR.glob("life_*.csv"):
        m = pat.search(p.name)
        if m:
            years.append(int(m.group(1)))
    return sorted(set(years))

def find_latest_available_year(candidate: int, max_back: int = 6) -> int:
    for y in range(candidate, candidate - max_back, -1):
        df = fetch_year_df(y)
        if df is not None and not df.empty:
            return y
    return candidate - max_back

def ensure_cache(start_year: int, end_year: int) -> None:
    have = set(cached_years())
    for y in range(start_year, end_year + 1):
        if y in have:
            continue
        print(f"[CACHE] missing life_{y}.csv -> fetching...")
        df = fetch_year_df(y)

        if df is None or df.empty:
            print(f"[CACHE] year={y} rows=0 -> not cached")
            continue

        print(f"[CACHE] year={y} rows={len(df)} -> cached")
# =========================
# 設定
# =========================
indicator = "WHOSIS_000001"
source = "WHO Global Health Observatory (GHO) OData API"
api_url = f"https://ghoapi.azureedge.net/api/{indicator}"

START_YEAR = 2000
END_YEAR = 2021

# 出力先
BASE_DIR = Path("map_bar_pj")
FRAMES_DIR = BASE_DIR / "frames_map_bar"
CACHE_DIR = BASE_DIR / "cache_life_csv"
BASE_DIR.mkdir(parents=True, exist_ok=True)
FRAMES_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# 右の棒グラフは上位N件だけ表示（重さと視認性のバランス）
TOP_N = 40

# 地図のカラースケール
RANGE_COLOR = (50, 85)
COLOR_SCALE = "Plasma"

# 国コード（tiny_map.py と同等）
some_country_codes = [
    'AFG','AGO','ALB','ARE','ARG','ARM','ATG','AUS','AUT','AZE','BDI','BEL','BEN','BFA','BGD','BGR',
    'BHR','BHS','BIH','BLR','BLZ','BOL','BRA','BRB','BRN','BTN','BWA','CAF','CAN','CHE','CHL','CHN','CIV','CMR','COD',
    'COG','COL','COM','CPV','CRI','CUB','CYP','CZE','DEU','DJI','DNK','DOM','DZA','ECU','EGY','ERI','ESP','EST','ETH',
    'FIN','FJI','FRA','FSM','GAB','GBR','GEO','GHA','GIN','GMB','GNB','GNQ','GRC','GRD','GTM','GUY','HND','HRV','HTI',
    'HUN','IDN','IND','IRL','IRN','IRQ','ISL','ISR','ITA','JAM','JOR','JPN','KAZ','KEN','KGZ','KHM','KIR','KOR','KWT',
    'LAO','LBN','LBR','LBY','LCA','LKA','LSO','LTU','LUX','LVA','MAR','MDA','MDG','MDV','MEX','MKD','MLI','MLT','MMR',
    'MNE','MNG','MOZ','MRT','MUS','MWI','MYS','NAM','NER','NGA','NIC','NLD','NOR','NPL','NZL','OMN','PAK','PAN','PER',
    'PHL','PNG','POL','PRI','PRK','PRT','PRY','PSE','QAT','ROU','RUS','RWA','SAU','SDN','SEN','SGP','SLB','SLE','SLV',
    'SOM','SRB','SSD','STP','SUR','SVK','SVN','SWE','SWZ','SYC','SYR','TCD','TGO','THA','TJK','TKM','TLS','TON','TTO',
    'TUN','TUR','TZA','UGA','UKR','URY','USA','UZB','VCT','VEN','VNM','VUT','WSM','YEM','ZAF','ZMB','ZWE'
]

# =========================
# 大陸マッピング（Natural Earth）
# =========================
def build_continent_map() -> dict[str, str]:
    world = gpd.read_file("data/ne_110m_admin_0_countries.shp") # .shp(Shapefile)拡張子: GIS用のベクタデータ形式を扱う拡張子
    # ISO_A3 が "-99" の行が混ざるので弾く
    world = world[world["ISO_A3"].notna() & (world["ISO_A3"] != "-99")]
    # continent: Africa, Europe, Asia, North America, South America, Oceania
    return dict(zip(world["ISO_A3"], world["CONTINENT"]))

CONTINENT_MAP = build_continent_map()

# 表示順（“大陸ごとにまとまる”ために固定）
CONTINENT_ORDER = ["Asia", "Europe", "Africa", "North America", "South America", "Oceania", "Antarctica"]

# =========================
# データ取得（年ごと）
# =========================
def fetch_year_df(year: int, timeout: int = 30) -> pd.DataFrame:
    cache = CACHE_DIR / f"life_{year}.csv"
    if cache.exists():
        df = pd.read_csv(cache)
        return df

    rows = []
    for code in some_country_codes:
        params = {
            "$filter": (
                f"SpatialDim eq '{code}' "
                f"and date(TimeDimensionBegin) ge {year}-01-01 "
                f"and date(TimeDimensionBegin) lt {year+1}-01-01"
            )
        }
        try:
            js = requests.get(api_url, params=params, timeout=timeout).json()
        except Exception:
            continue

        vals = js.get("value", [])
        if not vals:
            continue

        # まず両性(BTSX)があればそれを優先
        pick = next((v for v in vals if v.get("Dim1") == "SEX_BTSX"), None)

        # BTSXがないなら、とりあえず先頭(または後述の平均)を使う
        if pick is None:
            pick = vals[0]

        life = pick.get("NumericValue", None)
        if life is None:
            continue
        continent = CONTINENT_MAP.get(code, "Other")
        rows.append({"iso3": code, "life": float(life), "continent": continent})

        # API負荷軽減（少しだけ）
        time.sleep(0.01)

    df = pd.DataFrame(rows, columns=["iso3", "life", "continent"])

    # 追加: 空なら保存しない(空CSVを残さなさい)
    if df is None or df.empty:
        print(f"[CACHE] year={year} -> no rows, skip writing csv")
        return pd.DataFrame()

    df.to_csv(cache, index=False)
    return df

# =========================
# 右側の棒グラフ：大陸でグループ化して降順
# =========================
def top_n_grouped(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    # 上位Nをまず取る（全体の降順）
    df_top = df.sort_values("life", ascending=False).head(top_n).copy()

    # 大陸→各大陸内で降順→大陸順で連結
    parts = []
    for cont in CONTINENT_ORDER:
        part = df_top[df_top["continent"] == cont].sort_values("life", ascending=False)
        if not part.empty:
            parts.append(part)
    # どこにも入らないもの
    rest = df_top[~df_top["continent"].isin(CONTINENT_ORDER)].sort_values("life", ascending=False)
    if not rest.empty:
        parts.append(rest)

    out = pd.concat(parts, axis=0) if parts else df_top
    return out

# =========================
# 1フレーム描画（左：地図 / 右：棒）
# =========================
def render_frame(df_year, df_prev, year, out_png, alpha=5, top_n=40) -> None:
    df_bar = df_year.sort_values("life", ascending=False).head(top_n).copy()
    y = df_bar["iso3"].tolist()
    x = df_bar["life"].tolist()
    cont_seq = df_bar["continent"].fillna("Other").tolist()

    # ---- 大陸ごとに棒の色を統一（見分けやすい配色）----
    CONT_COL = {
        "Asia": "#1f77b4",          # blue
        "Europe": "#ff7f0e",        # orange
        "Africa": "#2ca02c",        # green
        "North America": "#d62728", # red
        "South America": "#9467bd", # purple
        "Oceania": "#8c564b",       # brown
        "Other": "#7f7f7f",         # gray
    }
    CONT_ABBR = {
        "Europe": "EU",
        "Asia": "AS",
        "North America": "NA",
        "South America": "SA",
        "Oceania": "OC",
        "Africa": "AF",
        "Other": "OT",
    }
    # --- subtitle: continent counts in Top N（表示順を固定）---
    counts = df_bar["continent"].fillna("Other").value_counts().to_dict()
    cont_order = ["Europe", "Asia", "Africa", "North America", "South America", "Oceania", "Other"]
    subtitle = " / ".join(
        [f"{CONT_ABBR[c]}:{counts.get(c, 0)}" for c in cont_order if counts.get(c, 0) > 0])

    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{"type": "choropleth"}, {"type": "xy"}]],
        column_widths=[0.68, 0.32], # ← 左を拡大(0.62→0.68など)
        horizontal_spacing=0.04,    # ← 間をあける(0.02→0.04など)
        subplot_titles=(
            f"Life Expectancy Map ({year})",
            # f"Top {top_n} Countries ({year} - {subtitle})"
            f"Top {top_n} Countries ({subtitle})"
        )
    )

    # =========================
    # 余白（ここが重要）
    # - 左にカラーバーを押し出すので、left margin を増やす
    # =========================
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=190, r=30, t=60, b=60),  # ← leftを増やす（重なり防止）
        barmode="overlay",
        bargap=0.06,        # 小さいほどバーが太く見える
        bargroupgap=0.0,
        showlegend=False,   # Plotly標準凡例は使わない（自作パネルのみ）
    )

    # =========================
    # 左：地図（colorbarを「もっと左」に出す）
    # =========================
    fig.add_trace(
        go.Choropleth(
            locations=df_year["iso3"],
            z=df_year["life"],
            zmin=RANGE_COLOR[0],
            zmax=RANGE_COLOR[1],
            colorscale=COLOR_SCALE,
            colorbar=dict(
                title="Years",
                x=-0.08,          # ← paper座標で左へ押し出す（負値OK）
                xanchor="left",
                y=0.52,
                len=0.86,
                thickness=18
            ),
            marker_line_color="rgba(80,80,80,0.6)",
            marker_line_width=0.7, # 0.5 → 0.7 (世界地図を強調)
        ),
        row=1, col=1
    )

    # =========================
    # 右：棒グラフ（大陸色）
    # =========================
    present = [c for c in cont_order if c in set(cont_seq)]
    for cont in present:
        mask = [c == cont for c in cont_seq]
        x_cont = [val if m else None for val, m in zip(x, mask)]
        fig.add_trace(
            go.Bar(
                x=x_cont,
                y=y,
                orientation="h",
                marker=dict(color=CONT_COL.get(cont, CONT_COL["Other"])),
                opacity=0.70,
                showlegend=False,
                text=[f"{v:.1f}" if m else "" for v, m in zip(x, mask)],
                textposition="outside",
                hovertemplate="Country: %{y}<br>Life: %{x:.2f}<extra></extra>",
            ),
            row=1, col=2
        )

    fig.update_yaxes(
        row=1, col=2,
        autorange="reversed",
        title_text="ISO3",
        automargin=True
    )
    # 右側バーの最大値(Top40だけでOK)
    x_max = float(df_bar["life"].max())
    pad = 2.3 # 年数の余白(1.0~2.5あたりで調整)
    x_hi = max(RANGE_COLOR[1], x_max + pad)
    fig.update_xaxes(
        row=1, col=2,
        title_text="Years",
        range=[RANGE_COLOR[0], x_hi],
    )

    # =========================
    # 自作：大陸凡例パネル（2行×4列）… Other込み
    # =========================
    legend_items = [
        ("Europe", CONT_COL["Europe"]),
        ("Asia", CONT_COL["Asia"]),
        ("North America", CONT_COL["North America"]),
        ("South America", CONT_COL["South America"]),
        ("Oceania", CONT_COL["Oceania"]),
        ("Africa", CONT_COL["Africa"]),
        ("Other", CONT_COL["Other"]),
        # 8枠目（空欄 or 予備）。空欄にしたいならこのまま。
        ("", "rgba(0,0,0,0)"),
    ]

    # 置き場所（paper座標）※今の見た目に合わせて微調整してOK
    panel_x0 = 0.02
    panel_y0 = 0.00

    cols = 4
    rows = 2

    # --- セルとスウォッチ設計（重なり防止の肝）---
    cell_w = 0.155        # ← 4列にするので少し小さめに
    cell_h = 0.060        # ← 行間も少し確保
    swatch_w = 0.020
    swatch_h = 0.026
    text_pad = 0.010      # ← スウォッチと文字の距離

    panel_w = cols * cell_w
    panel_h = rows * cell_h

    # 背景パネル
    fig.add_shape(
        type="rect",
        xref="paper", yref="paper",
        x0=panel_x0 - 0.014, y0=panel_y0 - 0.016,
        x1=panel_x0 + panel_w + 0.014, y1=panel_y0 + panel_h + 0.024,
        fillcolor="rgba(255,255,255,0.78)",
        line=dict(color="rgba(0,0,0,0.18)", width=1),
        layer="above"
    )

    # タイトル
    fig.add_annotation(
        xref="paper", yref="paper",
        x=panel_x0, y=panel_y0 + panel_h + 0.006,
        xanchor="left", yanchor="bottom",
        text="<b>Continent</b>",
        showarrow=False,
        font=dict(size=18, color="rgba(0,0,0,0.85)"),
    )

    # 2行×4列
    for i, (name, col) in enumerate(legend_items):
        r = i // cols
        c = i % cols

        x_left = panel_x0 + c * cell_w
        y_top  = panel_y0 + (rows - 1 - r) * cell_h

        # 空欄セルはスキップ
        if not name:
            continue

        # 色スウォッチ
        fig.add_shape(
            type="rect",
            xref="paper", yref="paper",
            x0=x_left,
            y0=y_top + (cell_h - swatch_h) / 2,
            x1=x_left + swatch_w,
            y1=y_top + (cell_h - swatch_h) / 2 + swatch_h,
            fillcolor=col,
            line=dict(color="rgba(0,0,0,0.25)", width=1),
            layer="above"
        )

        # ラベル（略称付き）
        abbr = CONT_ABBR.get(name, name)
        fig.add_annotation(
            xref="paper", yref="paper",
            x=x_left + swatch_w + text_pad,
            y=y_top + cell_h / 2,
            xanchor="left", yanchor="middle",
            text=f"{name} ({abbr})",
            showarrow=False,
            font=dict(size=15, color="rgba(0,0,0,0.85)"),
        )

    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(out_png, width=1920, height=1080, scale=1)


def interpolate_df(df0: pd.DataFrame, df1: pd.DataFrame, t: float) -> pd.DataFrame:
    """
    t in [0,1]
    df0/df1: columns = iso3, life, continent, rank(あってもOK)
    """
    a = df0[["iso3", "life", "continent"]].copy()
    b = df1[["iso3", "life"]].copy()

    merged = a.merge(b, on="iso3", how="outer", suffixes=("_0", "_1"))
    # continent は df0 を優先、無ければOther
    merged["continent"] = merged["continent"].fillna("Other")

    # life を補間（片方欠けたら存在する方を使う）
    merged["life_0"] = merged["life_0"].astype(float)
    merged["life_1"] = merged["life_1"].astype(float)

    merged["life"] = merged["life_0"] + (merged["life_1"] - merged["life_0"]) * t
    merged["life"] = merged["life"].where(~merged["life"].isna(), merged["life_0"])
    merged["life"] = merged["life"].where(~merged["life"].isna(), merged["life_1"])

    out = merged[["iso3", "life", "continent"]].dropna(subset=["life"]).copy()
    out = out.sort_values("life", ascending=False).reset_index(drop=True)
    out["rank"] = out.index + 1
    return out

RACE_DIR = BASE_DIR / "frames_map_bar_race"
RACE_DIR.mkdir(parents=True, exist_ok=True)

def main_race() -> None:
    ALPHA = 5
    TOP_N = 40
    steps = 12  # ここを増やすほどヌルヌル（CPUは増える）

    latest = find_latest_available_year(latest, max_back=6)
    # ---- latest year candidate を作ってから「実データがある年」へ落とす ----
    cached = cached_years()
    candidate = max(cached) if cached else END_YEAR
    candidate = max(candidate, time.gmtime().tm_year - 1) # 例: 2025年なら2024まで探す
    start = START_YEAR
    end = latest
    ensure_cache(start, end)

    year_dfs = {}
    for year in range(start, end + 1):
        df = fetch_year_df(year)
        if df.empty:
            continue
        df = df.sort_values("life", ascending=False).reset_index(drop=True)
        df["rank"] = df.index + 1
        year_dfs[year] = df

    frame_idx = 0
    prev_df = None

    for y in range(start, end):
        df0 = year_dfs.get(y)
        df1 = year_dfs.get(y + 1)
        if df0 is None or df1 is None:
            continue

        for s in range(steps):
            t = s / steps
            df_t = interpolate_df(df0, df1, t)
            # 表示用の年ラベル（小数を出したくなければ y だけでもOK）
            label = f"{y}+{t:.2f}"
            out_png = RACE_DIR / f"map_bar_{frame_idx:05d}.png"
            render_frame(df_t, prev_df, label, out_png, alpha=ALPHA, top_n=TOP_N)
            prev_df = df_t
            frame_idx += 1

    # 最後の年を1枚
    df_last = year_dfs.get(end)
    if df_last is not None:
        out_png = RACE_DIR / f"map_bar_{frame_idx:05d}.png"
        render_frame(df_last, prev_df, str(end), out_png, alpha=ALPHA, top_n=TOP_N)

    print(f"[DONE] Race frames saved to: {RACE_DIR}")


if __name__ == "__main__":
    """
    ・実行方法
    </> bat
    .\.venv\Scripts\activate
    python -m src.life_map_bar_anim

    ・map_bar_pj/frames_map_barの連番png → mp4 コマンド
    </> bat
    ffmpeg -framerate 0.3667 -start_number 2000 -i map_bar_pj/frames_map_bar/map_bar_%d.png ^
    -vf "fps=30,format=yuv420p" ^
    -c:v libx264 -crf 18 -preset medium -movflags +faststart ^
    life_map_bar_2000_2021_1min.mp4
    """
    main_race()
