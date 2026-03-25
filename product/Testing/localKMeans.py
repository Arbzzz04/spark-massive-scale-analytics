import os
import sys
import pandas as pd
import matplotlib.pyplot as plt

# add src to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))
from kMeans import ClimateClusterer

if __name__ == "__main__":
    print("starting local test")
    
    # define local paths
    current_dir = os.path.dirname(__file__)
    input_file = os.path.abspath(os.path.join(current_dir, "..", "data", "1950.csv"))
    
    clusterer = ClimateClusterer(app_name="local-kmeans")
    
    try:
        # load data using pandas to bypass windows hadoop errors
        pdf = pd.read_csv(
            input_file, header=None,
            names=["STATION", "DATE", "ELEMENT", "DATA_VALUE", "M_FLAG", "Q_FLAG", "S_FLAG", "OBS_TIME"],
            nrows=100000, # limit to 100k rows for local testing
            # force pandas to treat DATA_VALUE as a decimal (float64)
            dtype={"DATA_VALUE": "float64"}
        )
        
        # hand data to spark in-memory
        raw_df = clusterer.spark.createDataFrame(pdf, schema=clusterer.noaa_schema)
        
        # run pipeline
        print("cleaning data")
        clean_df = clusterer.clean_data(raw_df)
        
        print("extracting features")
        features_df = clusterer.extract_features(clean_df)
        
        print("applying kmeans")
        final_df = clusterer.apply_kmeans(features_df, k=5)
        
        print("generating visualization")
        
        # convert spark dataframe to pandas for plotting
        plot_df = final_df.toPandas()

        # sort clusters from coldest to hottest
        cluster_temps = plot_df.groupby("cluster_id")["avg_temp"].mean().sort_values()
        label_map = {}
        for i, cid in enumerate(cluster_temps.index):
            label_map[cid] = i + 1

        plot_df["zone_id"] = plot_df["cluster_id"].map(label_map)
        
        # create scatter plot
        plt.figure(figsize=(10, 6))
        scatter = plt.scatter(
            plot_df["avg_temp"], 
            plot_df["temp_range"], 
            c=plot_df["zone_id"], 
            cmap="viridis", 
            alpha=0.7
        )
        
        plt.xlabel("average temperature (c)")
        plt.ylabel("temperature range (c)")
        plt.title("k-means temperature zones (1950 sample)")
        plt.colorbar(scatter, label="temperature zone")
        
        # save and show
        plot_path = os.path.abspath(os.path.join(current_dir, "..", "data", "local_clusters.png"))
        plt.savefig(plot_path)
        plt.show()
        
    finally:
        clusterer.stop()
        print("test complete")