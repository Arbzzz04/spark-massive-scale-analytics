# Builds the README map assets from the real K-Means cluster output.
# usage: python build_map.py <clusters.csv> <ghcnd-stations.txt> <out_dir>
import os
import sys

import pandas as pd
import plotly.express as px

clusters_csv, stations_txt, out_dir = sys.argv[1:4]
os.makedirs(out_dir, exist_ok=True)

df = pd.read_csv(clusters_csv)
stations = pd.read_fwf(stations_txt, colspecs=[(0, 11), (12, 20), (21, 30), (41, 71)],
                       names=["STATION", "LATITUDE", "LONGITUDE", "NAME"])
m = df.merge(stations, on="STATION", how="inner")

# rank clusters coldest -> hottest, same as BigData_KMeans_Map.py
order = m.groupby("cluster_id")["avg_temp"].mean().sort_values()
labels = {cid: f"Zone {i + 1} · {t:.1f} °C" for i, (cid, t) in enumerate(order.items())}
m["zone"] = m["cluster_id"].map(labels)
zone_order = [labels[c] for c in order.index]
colors = ["#313695", "#4575b4", "#74add1", "#f46d43", "#a50026"]

for col in ["avg_temp", "max_temp", "min_temp", "temp_range"]:
    m[col] = m[col].round(1)
m["LATITUDE"] = m["LATITUDE"].round(3)
m["LONGITUDE"] = m["LONGITUDE"].round(3)
m["NAME"] = m["NAME"].str.strip().str.title()

print(f"stations plotted: {len(m)} of {len(df)}")
print(m.groupby("zone")["STATION"].count().reindex(zone_order))


def make_fig(width=None, height=None, marker=3):
    fig = px.scatter_geo(
        m, lat="LATITUDE", lon="LONGITUDE", color="zone",
        category_orders={"zone": zone_order}, color_discrete_sequence=colors,
        hover_name="NAME",
        hover_data={"STATION": True, "avg_temp": ":.1f", "max_temp": ":.1f", "min_temp": ":.1f",
                    "temp_range": ":.1f", "zone": False, "LATITUDE": False, "LONGITUDE": False},
        labels={"avg_temp": "mean daily max (°C)", "max_temp": "record high (°C)",
                "min_temp": "lowest daily max (°C)", "temp_range": "range (°C)"},
        projection="natural earth",
    )
    fig.update_traces(marker=dict(size=marker, opacity=0.85, line=dict(width=0)))
    fig.update_geos(showcountries=True, countrycolor="#9aa4b1", showcoastlines=True, coastlinecolor="#7d8794",
                    showland=True, landcolor="#f3f1ec", showocean=True, oceancolor="#dfe9f3",
                    showlakes=False, showframe=False, lataxis_range=[-60, 85])
    fig.update_layout(
        title=dict(text=f"K-Means temperature zones · {len(m):,} NOAA weather stations · 1900–2025",
                   x=0.5, xanchor="center", font=dict(size=22, color="#1f2328")),
        legend=dict(title="Zone (mean daily max)", orientation="h", x=0.5, xanchor="center", y=-0.02,
                    font=dict(size=15), itemsizing="constant"),
        margin=dict(l=10, r=10, t=60, b=10), paper_bgcolor="white", width=width, height=height,
        font=dict(family="Segoe UI, Helvetica, Arial, sans-serif"),
    )
    return fig


# interactive page: plotly.js from CDN keeps the file small
make_fig(marker=4).write_html(os.path.join(out_dir, "index.html"), include_plotlyjs="cdn", full_html=True,
                              config={"displaylogo": False, "responsive": True},
                              default_width="100%", default_height="95vh")

# fixed-size page used only to screenshot the static PNG
make_fig(width=1800, height=960, marker=3.2).write_html(
    os.path.join(out_dir, "_static.html"), include_plotlyjs="cdn", config={"staticPlot": True})

# scatter of the two clustering features, coloured to match the map
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(11, 6), dpi=150)
for zone, color in zip(zone_order, colors):
    z = m[m["zone"] == zone]
    ax.scatter(z["avg_temp"], z["temp_range"], s=6, alpha=0.6, color=color, label=f"{zone}  ({len(z):,})",
               linewidths=0)
ax.set_xlabel("mean daily max temperature (°C)")
ax.set_ylabel("temperature range: record high − lowest max (°C)")
ax.set_title("K-Means feature space: each dot is a weather station")
ax.legend(markerscale=3, frameon=False, loc="upper left")
ax.spines[["top", "right"]].set_visible(False)
ax.grid(alpha=0.25)
fig.tight_layout()
fig.savefig(os.path.join(out_dir, "scatter.png"))
