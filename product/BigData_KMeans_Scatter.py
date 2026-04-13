import os
import pandas as pd
import matplotlib.pyplot as plt

if __name__ == "__main__":
    print("starting big-data scatter plot")

    # define file paths for input and output plot
    current_dir = os.path.dirname(__file__)
    input_file = os.path.abspath(os.path.join(current_dir, "data", "temperature_zones_1900_2025.csv"))
    output_file = os.path.abspath(os.path.join(current_dir, "data", "bigdata_temperature_scatter.png"))

    # load clustered output
    print("loading clustered output")
    df = pd.read_csv(input_file)

    # sort clusters from coldest to hottest
    cluster_temps = df.groupby("cluster_id")["avg_temp"].mean().sort_values()

    label_map = {}
    for i, cid in enumerate(cluster_temps.index):
        label_map[cid] = i + 1

    df["zone_id"] = df["cluster_id"].map(label_map)

    # generate scatter plot of average temp vs temp range
    print("generating scatter plot")
    plt.figure(figsize=(10, 6))
    scatter = plt.scatter(
        df["avg_temp"],
        df["temp_range"],
        c=df["zone_id"],
        cmap="viridis",
        alpha=0.7
    )

    plt.xlabel("average temperature (c)")
    plt.ylabel("temperature range (c)")
    plt.title("k-means temperature zones (1900 to 2025)")
    plt.colorbar(scatter, label="temperature zone")

    # save the plot to a file and display it
    plt.savefig(output_file)
    plt.show()

    print("scatter plot complete")