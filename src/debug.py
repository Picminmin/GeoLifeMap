import requests
year = 2000
code = "JPN"  # USAでもOK
url = "https://ghoapi.azureedge.net/api/WHOSIS_000001"
params = {"$filter": f"SpatialDim eq '{code}' and date(TimeDimensionBegin) ge {year}-01-01 and date(TimeDimensionBegin) lt {year+1}-01-01"}
js = requests.get(url, params=params, timeout=30).json()
print("count:", len(js.get("value", [])))
print(js.get("value", [])[:1])
