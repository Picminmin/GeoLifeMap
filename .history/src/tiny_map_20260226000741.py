
from pprint import pprint
import requests
import pandas as pd
import plotly.express as px

"""
・実行方法
</> bat
python -m src.tiny_map
・表示される図の説明
世界保健機関(WHO: World Health Organization) が運用する
Global Health Observatory (GHO) のOData API から取得した
2022年の出生時平均寿命 (年) を国別に可視化したものである。
指標コードはWHOSIS_000001である。
"""
indicator = "WHOSIS_000001"
year = 2021
source = "WHO Global Health Observatory (GHO) OData API"

world_health_url = f"https://ghoapi.azureedge.net/api/WHOSIS_000001?$filter=SpatialDim eq \
    'USA' and TimeDimensionValue eq '2021' and Dim1 eq 'SEX_BTSX'"

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

url = "https://ghoapi.azureedge.net/api/WHOSIS_000001"
rows = []
for country_code in some_country_codes:
    r = requests.get(
        "https://ghoapi.azureedge.net/api/WHOSIS_000001",
        params={"$filter": f"SpatialDim eq '{country_code}' and date(TimeDimensionBegin) ge\
            {year}-01-01 and date(TimeDimensionBegin) lt {year+1}-01-01"}
    )
    js = r.json()
    if js.get("value"):
        # 平均寿命は数値(float)にする
        life_expectancy = float(js["value"][0]["Value"][:4])
        rows.append({"iso3":country_code, "life": life_expectancy})
        print(country_code, life_expectancy)

df = pd.DataFrame(rows, columns=["iso3", "life"])
if df.empty:
    raise RuntimeError(f"No data fetched for year={year}. Try a different year or loosen the filter.")
fig = px.choropleth(
    df,
    locations="iso3",
    color="life",
    color_continuous_scale="Plasma",
    range_color=(50, 85),
    title=f"Life expectancy at birth (years), {year} - {source}"
)

fig.update_layout(
    coloraxis_colorbar_title="Years",
    margin=dict(l=0, r=0, t=50, b=0),
    annotations=[
        dict(
            text=f"Indicator: {indicator} | Source: {source} | Retrieved via https://ghoapi.azureedge.net/api/",
            x=0.5, y=-0.05, xref="paper", yref="paper", showarrow=False, font=dict(size=12)
        )
    ],
)

fig.show()

from pathlib import Path

# 保存先ディレクトリ
save_dir = Path("Geo/img")
save_dir.mkdir(parents=True, exist_ok=True)

save_path = save_dir / f"life_expectancy_{year}.png"

fig.write_image(save_path, width=1600, height=900, scale=2)

print(f"Saved to: {save_path}")
