from __future__ import annotations

from pathlib import Path
import time
import requests
import pandas as pd
import plotly.express as px
import imageio.v2 as imageio


# =========================
# 設定
# =========================
indicator = "WHOSIS_000001"
source = "WHO Global Health Observatory (GHO) OData API"
api_url = f"https://ghoapi.azureedge.net/api/{indicator}"

START_YEAR = 2000
END_YEAR = 2021  # 2022以降は欠損が多いことがあるので、まずはここまで推奨

# 出力先（希望どおり）
BASE_DIR = Path("img")
FRAMES_DIR = BASE_DIR / "frames_life"
BASE_DIR.mkdir(parents=True, exist_ok=True)
FRAMES_DIR.mkdir(parents=True, exist_ok=True)

GIF_PATH = BASE_DIR / f"life_expectancy_{START_YEAR}_{END_YEAR}.gif"

# 取得対象の国コード（tiny_map.pyと同等）
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
# 関数
# =========================
def fetch_year_df(year: int, timeout: int = 30) -> pd.DataFrame:
    """指定年の (iso3, life) DataFrame を作る。取れない国はスキップ。"""
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

        # 数値化（Valueを優先）
        v0 = vals[0]
        val = v0.get("Value", None)
        if val is None:
            continue

        try:
            life = float(val)
        except Exception:
            # まれに文字列が混ざる場合の保険
            try:
                life = float(str(val)[:5])
            except Exception:
                continue

        rows.append({"iso3": code, "life": life})

        # 呼び出し負荷を減らす（軽くスロットル）
        time.sleep(0.02)

    return pd.DataFrame(rows, columns=["iso3", "life"])


def render_frame(df: pd.DataFrame, year: int, out_path: Path) -> None:
    """plotlyで1年分の地図を作ってPNG保存する（kaleidoが必要）。"""
    if df.empty:
        raise RuntimeError(f"No data fetched for year={year}.")

    fig = px.choropleth(
        df,
        locations="iso3",
        color="life",
        color_continuous_scale="Plasma",
        range_color=(50, 85),
        title=f"Life expectancy at birth (years), {year} — {source}",
    )

    fig.update_layout(
        coloraxis_colorbar_title="Years",
        margin=dict(l=0, r=0, t=60, b=0),
        annotations=[
            dict(
                text=f"Indicator: {indicator} | Source: {source}",
                x=0.5, y=-0.04, xref="paper", yref="paper",
                showarrow=False, font=dict(size=12)
            )
        ],
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(out_path, width=1600, height=900, scale=2)


def build_gif(frame_paths: list[Path], gif_path: Path, fps: int = 3) -> None:
    """PNG列からGIFを作る。"""
    images = [imageio.imread(p) for p in frame_paths]
    imageio.mimsave(gif_path, images, fps=fps)


def main() -> None:
    frame_paths: list[Path] = []

    for year in range(START_YEAR, END_YEAR + 1):
        out_png = FRAMES_DIR / f"life_{year}.png"

        # 既にあるフレームは再生成しない（作業再開が楽）
        if out_png.exists():
            print(f"[SKIP] {year} (exists): {out_png}")
            frame_paths.append(out_png)
            continue

        print(f"[FETCH] {year} ...")
        df = fetch_year_df(year)

        if df.empty:
            print(f"[WARN] {year}: no data; skipping frame")
            continue

        print(f"[RENDER] {year} -> {out_png} (countries={len(df)})")
        render_frame(df, year, out_png)
        frame_paths.append(out_png)

    if not frame_paths:
        raise RuntimeError("No frames were generated. Check years / API / filters.")

    print(f"[GIF] building: {GIF_PATH}")
    build_gif(frame_paths, GIF_PATH, fps=3)
    print(f"[DONE] saved GIF: {GIF_PATH}")


if __name__ == "__main__":
    """
    国別の出生時平均寿命をグラデーションであら
    """
    main()
