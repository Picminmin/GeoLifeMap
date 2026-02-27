from __future__ import annotations

from pathlib import Path
import time
import requests
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.graph_objects as go
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
UPDATE_CACHE = False # True: 最新までチェックして不足分取得 / False: 既存CSVのみ使用
MAX_BACK = 6
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

    # 追加: 空なら保存しない(空CSVを残さない)
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
from plotly.subplots import make_subplots
import plotly.graph_objects as go

def render_frame(df_year, df_prev, year, out_png, alpha=5, top_n=40) -> None:
    # -----------------------------
    # 1) bar 用データ
    # -----------------------------
    df_bar = df_year.sort_values("life", ascending=False).head(top_n).copy()

    # continent が無い/壊れている場合に備えて正規化
    if "continent" not in df_bar.columns:
        # df_year 側で continent を持っていないケースの保険（必要ならあなたの CONTINENT_MAP を使う）
        df_bar["continent"] = "Other"

    df_bar["continent"] = df_bar["continent"].fillna("Other").astype(str).str.strip()

    # 「略称が入ってしまう」ケースの保険（EU/AS/NA… → フル名へ戻す）
    ABBR_TO_FULL = {
        "EU": "Europe",
        "AS": "Asia",
        "AF": "Africa",
        "NA": "North America",
        "SA": "South America",
        "OC": "Oceania",
        "OT": "Other",
    }
    df_bar["continent"] = df_bar["continent"].replace(ABBR_TO_FULL)

    y = df_bar["iso3"].tolist()
    x = df_bar["life"].tolist()
    cont_seq = df_bar["continent"].tolist()

    CONT_COL = {
        "Asia": "#1f77b4",
        "Europe": "#ff7f0e",
        "Africa": "#2ca02c",
        "North America": "#d62728",
        "South America": "#9467bd",
        "Oceania": "#8c564b",
        "Other": "#7f7f7f",
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

    counts = df_bar["continent"].value_counts().to_dict()
    # 右上のサマリは略称で短く
    subtitle = " / ".join([f"{CONT_ABBR.get(k, k)}:{v}" for k, v in counts.items()])

    # -----------------------------
    # 2) figure (map + bar)
    # -----------------------------
    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{"type": "choropleth"}, {"type": "xy"}]],
        # 地図を少し大きく & 右と間隔を少し空ける（好みに応じて微調整OK）
        column_widths=[0.70, 0.30],
        horizontal_spacing=0.05,
        subplot_titles=(
            f"Life Expectancy Map ({year})",
            f"Top {top_n} Countries ({subtitle})"
        )
    )

    # -----------------------------
    # 3) 左：世界地図
    # -----------------------------
    fig.add_trace(
        go.Choropleth(
            locations=df_year["iso3"],
            z=df_year["life"],
            zmin=RANGE_COLOR[0],
            zmax=RANGE_COLOR[1],
            colorscale=COLOR_SCALE,
            colorbar=dict(
                title="Years",
                # 左側へ寄せて「地図と重ならない」位置へ（paper座標）
                x=0.02,
                y=0.52,
                len=0.85,
                thickness=18,
            ),
            marker_line_color="rgba(80,80,80,0.6)",
            marker_line_width=0.5,
        ),
        row=1, col=1
    )

    # -----------------------------
    # 4) 右：棒グラフ（必ず描く：present が空でも fallback）
    # -----------------------------
    cont_order = ["Europe", "Asia", "North America", "South America", "Oceania", "Africa", "Other"]
    present = [c for c in cont_order if c in set(cont_seq)]
    if not present:
        # ここに入る＝cont_seq が想定外。とにかく出す
        present = sorted(set(cont_seq))

    # まず土台（薄いグレー）を置く：棒の形を安定させる
    fig.add_trace(
        go.Bar(
            x=x, y=y,
            orientation="h",
            marker=dict(color="rgba(140,140,140,0.18)"),
            hoverinfo="skip",
            showlegend=False,
        ),
        row=1, col=2
    )

    # 大陸別の色を重ねる（legend は使わず、下の「自作凡例」で表現する想定）
    for cont in present:
        mask = [c == cont for c in cont_seq]
        x_cont = [val if m else None for val, m in zip(x, mask)]
        fig.add_trace(
            go.Bar(
                x=x_cont, y=y,
                orientation="h",
                marker=dict(color=CONT_COL.get(cont, CONT_COL["Other"])),
                opacity=0.80,  # ← ここで棒の主張を弱める（地図を目立たせる）
                hovertemplate="Country: %{y}<br>Life: %{x:.2f}<extra></extra>",
                showlegend=False,
            ),
            row=1, col=2
        )

    # 右：軸設定（上が1位）
    fig.update_yaxes(row=1, col=2, autorange="reversed", title_text="ISO3", automargin=True)
    # 数値ラベルが「見切れ」ないように x の上限に余白
    x_max = float(df_bar["life"].max()) if len(df_bar) else RANGE_COLOR[1]
    x_hi = max(x_max, RANGE_COLOR[1] - 0.0) + 1.2  # 余白（必要なら 1.8 などに）
    fig.update_xaxes(row=1, col=2, title_text="Years", range=[RANGE_COLOR[0], x_hi])

    # -----------------------------
    # 5) World average（収集できた国だけで平均）
    #    → 右の棒グラフ上に「縦線」として表示
    # -----------------------------
    world_avg = float(df_year["life"].mean())

    # subplot(1,2) の x 軸は "x2"、y 軸は "y2" になるのが一般的
    # y2 domain を使うと棒グラフ領域の上下いっぱいに線が伸びる
    fig.add_shape(
        type="line",
        x0=world_avg, x1=world_avg,
        y0=0, y1=1,
        xref="x2",
        yref="y2 domain",
        line=dict(color="rgba(0,0,0,0.55)", width=2, dash="dot"),
    )
    fig.add_annotation(
        x=world_avg,
        y=1.02,
        xref="x2",
        yref="y2 domain",
        text=f"World avg {world_avg:.1f}",
        showarrow=False,
        font=dict(size=12),
        bgcolor="rgba(255,255,255,0.75)",
        bordercolor="rgba(0,0,0,0.15)",
        borderwidth=1,
    )

    # -----------------------------
    # 6) 下の「2×4」凡例（自作）などはあなたの既存実装をこの後に付ける
    #    ※ legend 機構は使わず annotation+shape で作るのが一番安定
    # -----------------------------

    # 仕上げ：余白（タイトルが見切れるのを防ぐ）
    fig.update_layout(
        margin=dict(l=40, r=40, t=90, b=40),
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

def main(update_cache: bool = UPDATE_CACHE) -> None:
    """
    年ごとの静的フレーム（map_bar_2000.png, map_bar_2001.png, ...）を生成する安定版。
    いったんレース（補間）は使わない。
    """
    ALPHA = 5
    top_n = TOP_N

    start = START_YEAR

    if update_cache:
        # 最新候補を検出 → 実データがある年まで戻す → キャッシュ補完
        cached = cached_years()
        candidate = max(cached) if cached else END_YEAR
        candidate = max(candidate, time.gmtime().tm_year - 1)  # 例: 2026年なら2025を候補にする
        latest = find_latest_available_year(candidate, max_back=MAX_BACK)
        end = latest
        ensure_cache(start, end)
    else:
        yrs = cached_years()
        if not yrs:
            raise RuntimeError("No cached CSV found in cache_life_csv. Set UPDATE_CACHE=True first")
        end = max(yrs)

    print(f"[INFO] years: {start}..{end} (update_cache={update_cache})")

    prev_df = None
    for year in range(start, end + 1):
        df = fetch_year_df(year)
        if df is None or df.empty:
            print(f"[SKIP] year={year} empty")
            continue

        df = df.sort_values("life", ascending=False).reset_index(drop=True)
        df["rank"] = df.index + 1

        out_png = FRAMES_DIR / f"map_bar_{year}.png"
        render_frame(df, prev_df, str(year), out_png, alpha=ALPHA, top_n=top_n)

        prev_df = df

    print(f"[DONE] Frames saved to: {FRAMES_DIR}")

def main_race(update_cache: bool = UPDATE_CACHE) -> None:
    ALPHA = 5
    TOP_N = 40
    steps = 12  # ここを増やすほどヌルヌル（CPUは増える）

    start = START_YEAR

    if update_cache:
        # 最新候補を検出 → 実データがある年まで戻す → キャッシュ補完
        cached = cached_years()
        candidate = max(cached) if cached else END_YEAR
        candidate = max(candidate, time.gmtime().tm_year - 1) # 例: 2025年なら2024まで探す
        latest = find_latest_available_year(candidate, max_back=MAX_BACK)
        end = latest
        ensure_cache(start, end)
    else:
        # 既存キャッシュのみ使用
        yrs = cached_years()
        if not yrs:
            raise RuntimeError("No cached CSV found in cache_life_csv. Set UPDATE_CACHE=True first")
        end = max(yrs)
    print(f"[INFO] years: {start}..{end} (update_cache={update_cache})")

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
    main(update_cache=)
    main_race()
