import os
import pandas as pd
import plotly.express as px

if __name__ == "__main__":
    print("starting big-data map visualisation")

    # define file paths for input data and output map
    current_dir = os.path.dirname(__file__)
    data_file = os.path.abspath(os.path.join(current_dir, "data", "temperature_zones_1900_2025.csv"))
    stations_file = os.path.abspath(os.path.join(current_dir, "data", "ghcnd-stations.txt"))
    output_file = os.path.abspath(os.path.join(current_dir, "data", "bigdata_temperature_map.html"))

    # load clustered output
    print("loading clustered output")
    plot_df = pd.read_csv(data_file)

    # load station coordinates
    print("loading station coordinates")
    colspecs = [(0, 11), (12, 20), (21, 30)]
    names = ["STATION", "LATITUDE", "LONGITUDE"]
    stations_df = pd.read_fwf(stations_file, colspecs=colspecs, names=names)

    # join clustered data
    print("joining station data")
    map_df = pd.merge(plot_df, stations_df, on="STATION", how="inner")

    # sort clusters from coldest to hottest
    cluster_temps = map_df.groupby("cluster_id")["avg_temp"].mean().sort_values()

    label_map = {}
    for i, (cid, temp) in enumerate(cluster_temps.items()):
        label_map[cid] = f"zone {i+1} (avg {temp:.1f}c)"

    map_df["climate_zone"] = map_df["cluster_id"].map(label_map)

    # sort so the legend follows temperature order
    map_df = map_df.sort_values(by="avg_temp")

    # define a clear cold-to-hot color palette
    temp_colors = ["#313695", "#4575b4", "#74add1", "#f46d43", "#a50026"]

    # generate the interactive map with plotly
    print("generating map")
    fig = px.scatter_geo(
        map_df,
        lat="LATITUDE",
        lon="LONGITUDE",
        color="climate_zone",
        color_discrete_sequence=temp_colors,
        hover_name="STATION",
        hover_data=["avg_temp", "max_temp", "min_temp", "temp_range"],
        title="global temperature zones (1900 to 2025)",
        projection="natural earth"
    )

    # save the map to an HTML file and display it
    fig.write_html(output_file)
    fig.show()

    print("map visualisation complete")