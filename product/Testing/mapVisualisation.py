import os
import sys
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))
from kMeans import ClimateClusterer

if __name__ == "__main__":
    print("starting map visualization pipeline")
    
    current_dir = os.path.dirname(__file__)
    data_file = os.path.abspath(os.path.join(current_dir, "..", "data", "1950.csv"))
    stations_file = os.path.abspath(os.path.join(current_dir, "..", "data", "ghcnd-stations.txt"))
    
    k = 5

    clusterer = ClimateClusterer(app_name="map-viz")
    
    try:
        pdf = pd.read_csv(
            data_file, header=None,
            nrows=100000, # limit to 100k rows for local testing
            names=["STATION", "DATE", "ELEMENT", "DATA_VALUE", "M_FLAG", "Q_FLAG", "S_FLAG", "OBS_TIME"],
            dtype={"DATA_VALUE": "float64"}
        )
        
        raw_df = clusterer.spark.createDataFrame(pdf, schema=clusterer.noaa_schema)
        clean_df = clusterer.clean_data(raw_df)
        features_df = clusterer.extract_features(clean_df)
        
        # apply kmeans and convert to pandas for plotting
        final_df = clusterer.apply_kmeans(features_df, k=k)
        plot_df = final_df.toPandas()
        
        colspecs = [(0, 11), (12, 20), (21, 30)]
        names = ["STATION", "LATITUDE", "LONGITUDE"]
        stations_df = pd.read_fwf(stations_file, colspecs=colspecs, names=names)
        
        map_df = pd.merge(plot_df, stations_df, on="STATION", how="inner")
        print(f"Matched {len(map_df)} of {len(plot_df)} stations to coordinates")
        
        # --- start of temperature scaling logic ---
        print("calculating temperature scale for legend")
        
        # find the actual average temperature for each random cluster id
        cluster_temps = map_df.groupby("cluster_id")["avg_temp"].mean().sort_values()
        
        # map the random ids to descriptive names, ranked from coldest to hottest
        label_map = {}
        for i, (cid, temp) in enumerate(cluster_temps.items()):
            label_map[cid] = f"zone {i+1} (avg {temp:.1f}c)"
            
        map_df["climate_zone"] = map_df["cluster_id"].map(label_map)
        
        # sort the dataframe so plotly draws the legend from coldest to hottest
        map_df = map_df.sort_values(by="avg_temp")
        
        # generate exactly k colours from a cold-to-hot colormap
        cmap = plt.get_cmap("RdYlBu_r")  # blue=cold, red=hot
        temp_colors = [mcolors.to_hex(cmap(i / (k - 1))) for i in range(k)]
        
        print("generating interactive map")
        fig = px.scatter_geo(
            map_df,
            lat="LATITUDE",
            lon="LONGITUDE",
            color="climate_zone",
            color_discrete_sequence=temp_colors,
            hover_name="STATION",
            hover_data=["avg_temp", "max_temp", "min_temp", "temp_range"], 
            title="global temperature zones (temperature scaled)",
            projection="natural earth"
        )
        
        out_path = os.path.abspath(os.path.join(current_dir, "..", "data", "climate_map.html"))
        fig.write_html(out_path)
        fig.show()
        
    finally:
        clusterer.stop()
        print("visualization complete")