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
    # 大陸でまとまる表示にしたいなら top_n_grouped を使う
    # df_bar = top_n_grouped(df_year, top_n).copy()
    df_bar = df_year.sort_values("life", ascending=False).head(top_n).copy()
    y = df_bar["iso3"].tolist()
    x = df_bar["life"].tolist()
    cont_seq = df_bar["continent"].tolist()
    # ---- 大陸ごとに棒の色を統一 ----
    CONT_COL = {
        "Asia": "#1f77b4",          # blue
        "Europe": "#ff7f0e",        # orange
        "Africa": "#2ca02c",        # green
        "North America": "#d62728", # red
        "South America": "#9467bd", # purple
        "Oceania": "#8c564b",       # brown
        "Other": "#7f7f7f",         # gray
    }

    # まず「全体」を薄いグレーで描いて土台にする(全バーの形を確保)
    fig.add_trace(
        go.Bar(
            x=x, y=y,
            orientation="h",
            marker=dict(color="rgba(140,140,140,0.25)"),
            hoverinfo="skip",
            showlegend=False,
        ),
        row=1, col=2
    )

    # 次に、大陸後ごとに"その国だけ"値を残すトレースを重ねる(凡例が出る)
    continents_in_top = df_bar["continent"].fillna("Other").unique().tolist()

    for cont in continents_in_top:
        mask = [c == cont for c in cont_seq]
        x_cont = [val if m else None for val, m in zip(x, mask)] # cont以外は描かない
        fig.add_trace(
            go.Bar(
                x=x_cont, y=y,
                orientation="h",
                marker=dict(color=CONT_COL.get(cont, CONT_COL["Other"])),
                name=cont,
                showlegend=True,
                hovertemplate="Country: %{y}<br>Life: %{x:.2f}<extra></extra>",
            ),
            row=1, col=2
        )

    fig.update_yaxes(row=1, col=2, autorange="reversed", title_text="ISO3", automargin=True)
    fig.update_xaxes(row=1, col=2, autorange="Years", range=[RANGE_COLOR[0]])
    # 前年rankと比較して「動いた国」を判定
    moved = set()
    if df_prev is not None and not df_prev.empty:
        prev_rank = dict(zip(df_prev["iso3"], df_prev["rank"]))
        for _, r in df_bar.iterrows():
            iso = r["iso3"]
            if iso in prev_rank:
                if abs(int(prev_rank[iso]) - int(r["rank"])) >= alpha:
                    moved.add(iso)

    counts = df_bar["continent"].value_counts().to_dict()
    subtitle = " / ".join([f"{k}:{v}" for k, v in counts.items()])
    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{"type": "choropleth"}, {"type": "xy"}]],
        column_widths=[0.62, 0.38],
        horizontal_spacing=0.02,
        subplot_titles=(f"Life Expectancy Map ({year})", f"Top {top_n} Countries ({year} - {subtitle})")
    )

    fig.add_trace(
        go.Choropleth(
            locations=df_year["iso3"],
            z=df_year["life"],
            zmin=RANGE_COLOR[0],
            zmax=RANGE_COLOR[1],
            colorscale=COLOR_SCALE,
            colorbar=dict(title="Years"),
            marker_line_color="rgba(80,80,80,0.6)",
            marker_line_width=0.5,
        ),
        row=1, col=1
    )
    bar_colors = [CONT_COL.get(c, CONT_COL["Other"]) for c in cont_seq]

    # 右側：上が1位に見えるように
    fig.update_yaxes(
        row=1, col=2,
        autorange="reversed", # 上が1位
        title_text="ISO3",
        automargin=True
    )
    fig.update_xaxes(
        row=1, col=2,
        title_text="Years",
        range=[RANGE_COLOR[0], RANGE_COLOR[1]]
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
