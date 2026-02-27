from __future__ import annotations

from pathlib import Path
import time
import requests
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import geopandas as gpd


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
    cache_path = CACHE_DIR / f"life_{year}.csv"
    if cache_path.exists():
        return pd.read_csv(cache_path)

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
    df.to_csv(cache_path, index=False)
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
        "Asia"
    }
    # --- subtitle: continent counts in Top N（表示順を固定）---
    counts = df_bar["continent"].fillna("Other").value_counts().to_dict()
    cont_order = ["Europe", "Asia", "Africa", "North America", "South America", "Oceania", "Other"]
    subtitle = " / ".join([f"{k}:{counts.get(k, 0)}" for k in cont_order if counts.get(k, 0) > 0])

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
    fig.update_xaxes(
        row=1, col=2,
        title_text="Years",
        range=[RANGE_COLOR[0], RANGE_COLOR[1]]
    )

    # =========================
    # 自作：大陸凡例パネル（2行×3列）… “より右下” & “文字大きく”
    # =========================
    legend_items = [
        ("Europe", CONT_COL["Europe"]),
        ("Asia", CONT_COL["Asia"]),
        ("North America", CONT_COL["North America"]),
        ("South America", CONT_COL["South America"]),
        ("Oceania", CONT_COL["Oceania"]),
        ("Africa", CONT_COL["Africa"]),
        # ("Other", CONT_COL["Other"]),  # 必要なら 3行にするのがおすすめ
    ]

    # 置き場所：右下寄りに調整（paper座標）
    panel_x0 = 0.20   # ←右へ（元0.18）
    panel_y0 = 0.00   # ←下へ（元0.06）
    cols = 3
    rows = 2

    cell_w = 0.14
    cell_h = 0.055
    swatch_w = 0.018
    swatch_h = 0.024

    panel_w = cols * cell_w
    panel_h = rows * cell_h

    # 背景パネル
    fig.add_shape(
        type="rect",
        xref="paper", yref="paper",
        x0=panel_x0 - 0.012, y0=panel_y0 - 0.014,
        x1=panel_x0 + panel_w + 0.012, y1=panel_y0 + panel_h + 0.020,
        fillcolor="rgba(255,255,255,0.78)",
        line=dict(color="rgba(0,0,0,0.18)", width=1),
        layer="above"
    )

    # タイトル（文字サイズUP）
    fig.add_annotation(
        xref="paper", yref="paper",
        x=panel_x0, y=panel_y0 + panel_h + 0.006,
        xanchor="left", yanchor="bottom",
        text="<b>Continent</b>",
        showarrow=False,
        font=dict(size=24, color="rgba(0,0,0,0.85)"),
    )

    # 2行×3列
    for i, (name, col) in enumerate(legend_items):
        r = i // cols
        c = i % cols

        x_left = panel_x0 + c * cell_w
        y_top = panel_y0 + (rows - 1 - r) * cell_h

        # 色スウォッチ
        fig.add_shape(
            type="rect",
            xref="paper", yref="paper",
            x0=x_left, y0=y_top + (cell_h - swatch_h) / 2,
            x1=x_left + swatch_w, y1=y_top + (cell_h - swatch_h) / 2 + swatch_h,
            fillcolor=col,
            line=dict(color="rgba(0,0,0,0.25)", width=1),
            layer="above"
        )

        # ラベル（文字サイズUP）
        fig.add_annotation(
            xref="paper", yref="paper",
            x=x_left + swatch_w + 0.008,
            y=y_top + cell_h / 2,
            xanchor="left", yanchor="middle",
            text=name,
            showarrow=False,
            font=dict(size=20, color="rgba(0,0,0,0.85)"),
        )

    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(out_png, width=1920, height=1080, scale=1)

def main() -> None:
    ALPHA = 5 # ここを好きな閾値に(例: 5位以上変動で強調)
    TOP_N = 40 # 既存設定

    year_dfs = {}
    for year in range(START_YEAR, END_YEAR + 1):
        df = fetch_year_df(year)
        if df.empty:
            continue
        # 降順ランキング(1が最高)
        df = df.sort_values("life", ascending=False).reset_index(drop=True)
        df["rank"] = df.index + 1
        year_dfs[year] = df

    prev_df = None
    for year in range(START_YEAR, END_YEAR + 1):
        df_year = year_dfs.get(year)
        if df_year is None:
            continue

        out_png = FRAMES_DIR / f"map_bar_{year}.png"
        render_frame(df_year, prev_df, year, out_png, alpha=ALPHA, top_n=TOP_N)
        prev_df = df_year

    print(f"[DONE] Frames saved to: {FRAMES_DIR}")


if __name__ == "__main__":
    main()
