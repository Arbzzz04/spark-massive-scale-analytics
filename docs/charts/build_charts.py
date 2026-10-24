# Builds the README performance charts (light + dark) from the benchmark trackers.
# usage (from the repo root): python docs/charts/build_charts.py
import os

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import FuncFormatter

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
PERF = os.path.join(ROOT, "product", "Performance_testing")
OUT = os.path.join(ROOT, "docs", "images")

interim = pd.ExcelFile(os.path.join(PERF, "FYP_performance_testing.xlsx"))
final = pd.ExcelFile(os.path.join(PERF, "FYP_final_performance_tracker.xlsx"))

wc_exec = interim.parse("WordCount perf - Executors")
grep_exec = interim.parse("DistributedGrep perf - Executor")
wc_cores = interim.parse("WordCount performance - Cores")
wc_mem = interim.parse("WordCount performance - Memory")
grep_mem = interim.parse("DistributedGrep - Memory")
wc_data = interim.parse("WordCount perf - Datasets")
grep_data = interim.parse("DistributedGrep - Datasets")
stat_exec = final.parse("StatAnalysis - Executors")
km_exec = final.parse("KMeans - Executors")
stat_part = final.parse("StatAnalysis - Partitions")
km_part = final.parse("KMeans - Partitions")

THEMES = {
    "light": dict(surface="#fcfcfb", ink="#0b0b0b", ink2="#52514e", muted="#898781", grid="#e1e0d9",
                  axis="#c3c2b7", wc="#2a78d6", grep="#eb6834", stat="#1baf7a", km="#eda100",
                  ramp=["#86b6ef", "#2a78d6", "#104281"]),
    "dark": dict(surface="#1a1a19", ink="#f0efec", ink2="#c3c2b7", muted="#898781", grid="#2c2c2a",
                 axis="#383835", wc="#3987e5", grep="#d95926", stat="#199e70", km="#c98500",
                 ramp=["#1c5cab", "#3987e5", "#86b6ef"]),
}


def style(ax, t, title, xlabel, ylabel):
    ax.set_facecolor(t["surface"])
    ax.set_title(title, loc="left", color=t["ink"], fontsize=13, fontweight="semibold", pad=12)
    ax.set_xlabel(xlabel, color=t["ink2"], fontsize=10.5)
    ax.set_ylabel(ylabel, color=t["ink2"], fontsize=10.5)
    ax.tick_params(colors=t["muted"], labelsize=9.5, length=0, pad=6)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(t["axis"])
    ax.grid(axis="y", color=t["grid"], linewidth=0.8)
    ax.set_axisbelow(True)


def line(ax, t, x, y, color, label):
    ax.plot(x, y, color=color, linewidth=2.2, marker="o", markersize=6.5, label=label, zorder=3,
            markeredgecolor=t["surface"], markeredgewidth=1.5, solid_capstyle="round")


def end_label(ax, t, x, y, text, dy=0, ha="left", dx=8):
    ax.annotate(text, (x, y), xytext=(dx, dy), textcoords="offset points", ha=ha, va="center",
                color=t["ink"], fontsize=10, fontweight="semibold")


def legend(ax, t, loc="upper left"):
    lg = ax.legend(loc=loc, frameon=False, fontsize=9.5, handlelength=1.6)
    for txt in lg.get_texts():
        txt.set_color(t["ink2"])


def figure(t):
    fig, axes = plt.subplots(1, 2, figsize=(12.5, 4.6), dpi=160)
    fig.patch.set_facecolor(t["surface"])
    return fig, axes


def save(fig, name, mode):
    fig.tight_layout(w_pad=4)
    fig.savefig(os.path.join(OUT, f"perf_{name}_{mode}.png"), facecolor=fig.get_facecolor())
    plt.close(fig)


minutes = FuncFormatter(lambda v, _: f"{v / 60:.0f}")

for mode, t in THEMES.items():
    plt.rcParams["font.family"] = ["Segoe UI", "DejaVu Sans"]

    # 1 - executor scaling: speed-up relative to the smallest configuration
    fig, (a, b) = figure(t)
    for df, key, name in [(wc_exec, "wc", "Word Count"), (grep_exec, "grep", "Distributed Grep")]:
        s = df["runtime_seconds"].iloc[0] / df["runtime_seconds"]
        line(a, t, df["num_executors"], s, t[key], name)
        end_label(a, t, df["num_executors"].iloc[-1], s.iloc[-1], f"{s.iloc[-1]:.1f}×")
    a.plot([1, 10], [1, 10], color=t["muted"], linewidth=1.2, linestyle=(0, (4, 3)), label="ideal (linear)")
    style(a, t, "Text workloads · 1 → 10 executors (1 core each)", "executors", "speed-up vs 1 executor")
    a.set_xticks(range(1, 11)); a.set_xlim(0.6, 11.2); a.set_ylim(0, 11.5)
    legend(a, t)

    for df, key, name in [(stat_exec, "stat", "Statistical Analysis"), (km_exec, "km", "K-Means")]:
        s = df["runtime_seconds"].iloc[0] / df["runtime_seconds"]
        line(b, t, df["num_executors"], s, t[key], name)
        end_label(b, t, df["num_executors"].iloc[-1], s.iloc[-1], f"{s.iloc[-1]:.1f}×", dy=7 if key == "stat" else -7)
    b.plot([2, 10], [1, 5], color=t["muted"], linewidth=1.2, linestyle=(0, (4, 3)), label="ideal (linear)")
    style(b, t, "Numerical workloads · 2 → 10 executors (4 cores each)", "executors", "speed-up vs 2 executors")
    b.set_xticks(range(2, 11, 2)); b.set_xlim(1.6, 11.2); b.set_ylim(0.5, 5.5)
    legend(b, t)
    save(fig, "executors", mode)

    # 1b - the absolute runtimes behind those speed-ups
    fig, (a, b) = figure(t)
    for df, key, name in [(wc_exec, "wc", "Word Count"), (grep_exec, "grep", "Distributed Grep")]:
        line(a, t, df["num_executors"], df["runtime_seconds"], t[key], name)
        first, last = df["runtime_seconds"].iloc[0], df["runtime_seconds"].iloc[-1]
        end_label(a, t, 1, first, f"{first / 60:.0f} min", dx=-10, ha="right")
        end_label(a, t, 10, last, f"{last / 60:.0f} min", dy=10 if key == "wc" else -10)
    style(a, t, "Text workloads · runtime, 100 GB (1 core per executor)", "executors", "runtime (minutes)")
    a.yaxis.set_major_formatter(minutes); a.set_yticks(range(0, 30001, 6000)); a.set_ylim(0, 32000)
    a.set_xticks(range(1, 11)); a.set_xlim(-0.6, 11.4)
    legend(a, t, loc="upper right")

    for df, key, name in [(stat_exec, "stat", "Statistical Analysis"), (km_exec, "km", "K-Means")]:
        line(b, t, df["num_executors"], df["runtime_seconds"], t[key], name)
        first, last = df["runtime_seconds"].iloc[0], df["runtime_seconds"].iloc[-1]
        end_label(b, t, 2, first, f"{first:,.0f} s", dx=10)
        end_label(b, t, 10, last, f"{last:,.0f} s", dy=10 if key == "stat" else -10)
    style(b, t, "Numerical workloads · runtime, ~100 GB (4 cores per executor)", "executors", "runtime (seconds)")
    b.set_xticks(range(2, 11, 2)); b.set_xlim(1.6, 11.4); b.set_ylim(0, 1400)
    legend(b, t, loc="upper right")
    save(fig, "runtime", mode)

    # 1c - throughput, as plotted in the stat analysis and K-Means notebooks
    fig, ax = plt.subplots(figsize=(8, 4.4), dpi=160)
    fig.patch.set_facecolor(t["surface"])
    for df, key, name in [(stat_exec, "stat", "Statistical Analysis"), (km_exec, "km", "K-Means")]:
        line(ax, t, df["num_executors"], df["throughput_mbps"], t[key], name)
        first, last = df["throughput_mbps"].iloc[0], df["throughput_mbps"].iloc[-1]
        end_label(ax, t, 2, first, f"{first:.0f} MB/s", dx=10, dy=-8 if key == "stat" else 8)
        end_label(ax, t, 10, last, f"{last:.0f} MB/s")
    style(ax, t, "Throughput vs executors · NOAA ~100 GB", "executors (4 cores each)", "throughput (MB/s)")
    ax.set_xticks(range(2, 11, 2)); ax.set_xlim(1.6, 11.8); ax.set_ylim(0, 470)
    legend(ax, t, loc="upper left")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, f"perf_throughput_{mode}.png"), facecolor=fig.get_facecolor())
    plt.close(fig)

    # 2 - data scaling
    fig, (a, b) = figure(t)
    for n, color in zip([2, 6, 10], t["ramp"]):
        g = wc_data[wc_data["num_executor"] == n].sort_values("Dataset_GB")
        line(a, t, g["Dataset_GB"], g["runtime_seconds"], color, f"{n} executors")
        end_label(a, t, 100, g["runtime_seconds"].iloc[-1], f"{g['runtime_seconds'].iloc[-1] / 60:.0f} min")
    style(a, t, "Word Count · runtime vs dataset size", "Wikipedia subset (GB)", "runtime (minutes)")
    a.yaxis.set_major_formatter(minutes); a.set_yticks(range(0, 6001, 1200)); a.set_ylim(0, 6300)
    a.set_xticks(range(10, 101, 10)); a.set_xlim(5, 112)
    legend(a, t)

    g = grep_data.sort_values("Dataset_GB")
    line(b, t, g["Dataset_GB"], g["runtime_seconds"], t["grep"], "Distributed Grep · 6 executors × 4 cores")
    end_label(b, t, 10, g["runtime_seconds"].iloc[0], "95 s", dy=-14, ha="center", dx=0)
    end_label(b, t, 100, g["runtime_seconds"].iloc[-1], "696 s", dy=-14, ha="center", dx=0)
    style(b, t, "Distributed Grep · near-linear in data size", "Wikipedia subset (GB)", "runtime (seconds)")
    b.set_xticks(range(10, 101, 10)); b.set_xlim(5, 105); b.set_ylim(0, 780)
    legend(b, t)
    save(fig, "datasize", mode)

    # 3 - cores and memory
    fig, (a, b) = figure(t)
    line(a, t, wc_cores["total_cores"], wc_cores["runtime_seconds"], t["wc"], "Word Count · 100 GB")
    # label every point with its configuration, as the original notebook chart did
    # the curve falls to the right, so up-and-right of each point is always clear
    for _, r in wc_cores.iterrows():
        cfg = f"{r.num_executors}×{r.executor_cores}"
        if r.total_cores == 4:
            cfg += " · 152 min"
        if r.total_cores == 36:
            a.annotate(f"{cfg} · 21 min", (r.total_cores, r.runtime_seconds), xytext=(0, -12),
                       textcoords="offset points", ha="center", va="top", color=t["ink2"], fontsize=9)
            continue
        a.annotate(cfg, (r.total_cores, r.runtime_seconds), xytext=(7, 7), textcoords="offset points",
                   ha="left", va="bottom", color=t["ink2"], fontsize=9)
    style(a, t, "More cores · Word Count on 100 GB", "total cores · labels show executors × cores per executor",
          "runtime (minutes)")
    a.yaxis.set_major_formatter(minutes); a.set_yticks(range(0, 9001, 1800)); a.set_ylim(0, 10200)
    a.set_xticks(range(4, 37, 4)); a.set_xlim(1, 39)
    legend(a, t, loc="upper right")

    mem_gb = wc_mem["executor_memory"].str.rstrip("G").astype(int)
    line(b, t, mem_gb, wc_mem["runtime_seconds"], t["wc"], "Word Count")
    line(b, t, mem_gb, grep_mem["runtime_seconds"], t["grep"], "Distributed Grep")
    end_label(b, t, 4, 1396, "fastest at 4 GB", dy=16, ha="center", dx=0)
    end_label(b, t, 4, 656, "fastest at 4 GB", dy=-16, ha="center", dx=0)
    style(b, t, "More memory · 6 executors × 4 cores, 100 GB", "memory per executor  [total across 6 executors]",
          "runtime (seconds)")
    b.set_xticks(mem_gb); b.set_ylim(0, 2000)
    b.set_xticklabels([f"{m} GB\n[{m * 6} GB]" for m in mem_gb])
    legend(b, t, loc="upper right")
    save(fig, "resources", mode)

    # 4 - shuffle partitions
    fig, (a, b) = figure(t)
    sp = stat_part.groupby("shuffle_partitions")["runtime_seconds"].mean().reset_index()
    rep = stat_part[stat_part["shuffle_partitions"] == 400]
    a.scatter(rep["shuffle_partitions"], rep["runtime_seconds"], s=22, color=t["stat"], alpha=0.35, zorder=2,
              linewidths=0, label="Stat Analysis · individual runs at 400")
    line(a, t, sp["shuffle_partitions"], sp["runtime_seconds"], t["stat"], "Statistical Analysis (mean)")
    line(a, t, km_part["shuffle_partitions"], km_part["runtime_seconds"], t["km"], "K-Means")
    end_label(a, t, 128, 883.676, "best 884 s", dy=-16, ha="center", dx=0)
    end_label(a, t, 800, 421.378, "best 421 s", dy=-16, ha="center", dx=0)
    style(a, t, "Runtime vs spark.sql.shuffle.partitions", "shuffle partitions (log scale)", "runtime (seconds)")
    a.set_xscale("log", base=2)
    ticks = [16, 32, 64, 128, 200, 400, 800, 1200]
    a.set_xticks(ticks); a.set_xticklabels([str(v) for v in ticks]); a.minorticks_off(); a.set_ylim(0, 1550)
    legend(a, t, loc="upper left")

    km_gb = km_part["total_shuffle_read"].str.extract(r"([\d.]+)")[0].astype(float)
    line(b, t, km_part["shuffle_partitions"], km_gb, t["km"], "K-Means")
    line(b, t, sp["shuffle_partitions"], [0.067] * len(sp), t["stat"], "Statistical Analysis")
    end_label(b, t, 1200, km_gb.iloc[-1], "1.8 GB", dy=0)
    end_label(b, t, 800, 0.067, "≈ 67 MB at every setting", dy=14, ha="right", dx=0)
    style(b, t, "Shuffle volume vs partitions", "shuffle partitions (log scale)", "shuffle read (GB)")
    b.set_xscale("log", base=2)
    b.set_xticks(ticks); b.set_xticklabels([str(v) for v in ticks]); b.minorticks_off(); b.set_ylim(0, 2.1)
    b.set_xlim(12, 1900)
    legend(b, t, loc="upper left")
    save(fig, "partitions", mode)

print("charts written to", OUT)
