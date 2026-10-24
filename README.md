# Massive-Scale Data Analytics with Apache Spark

> BSc Computer Science Final Year Project, Royal Holloway, University of London (2025–26)
> **Arbaaz Ali Jaffer**

Four distributed data-processing programs written in **PySpark** and run on the university's
Big Data cluster (HDFS + YARN) over two **~100 GB datasets**. The project also includes a
performance study of how the Spark configuration (executors, cores, memory, shuffle partitions,
dataset size) affects runtime and throughput.

| # | Program | Dataset | Spark features used |
|---|---------|---------|---------------------|
| 1 | **Word Count** | Wikipedia XML dump (100 GB) | DataFrame API, `spark-xml`, regex cleaning, `explode` / `groupBy` |
| 2 | **Distributed Grep** | Wikipedia XML dump (100 GB) | Same cleaning pipeline, `rlike` pattern matching that returns the matching article titles |
| 3 | **Statistical Analysis** | NOAA GHCN-Daily weather, 1900–2025 (~100 GB) | Explicit schema, `percentile_approx`, MLlib `Bucketizer` frequency distribution |
| 4 | **K-Means Clustering** | NOAA GHCN-Daily weather (~100 GB) | MLlib `VectorAssembler`, `StandardScaler`, `KMeans` to group weather stations into temperature zones |

📄 **Full report:** [`documents/ArbaazJaffer.final.pdf`](documents/ArbaazJaffer.final.pdf)

### 🌍 Final result: global temperature zones from 100 GB of weather data

<img src="docs/images/kmeans_world_map.png" alt="World map of 40,179 NOAA weather stations coloured by K-Means temperature zone">

Spark MLlib K-Means (k = 5) clustered **40,179 weather stations** using 125 years of daily maximum temperatures.
No location data went into the model, yet the zones it found match real climate bands: Canada, Scandinavia and
Russia in the coldest zones, and Mexico, Africa, India, South-East Asia and Australia in the hottest.

> **🗺️ Interactive version:** zoom, pan and hover over any station to see its name and temperatures. Open
> [`docs/map/index.html`](docs/map/index.html), click **Download raw file** (⬇) at the top right of the file view,
> and open the downloaded file in any browser.

---

## Contents

- [Architecture](#architecture)
- [How each program works](#how-each-program-works)
- [Results & performance analysis](#results--performance-analysis)
- [Repository layout](#repository-layout)
- [How it was set up and run on the cluster](#how-it-was-set-up-and-run-on-the-cluster)
- [Running locally](#running-locally)
- [Key learnings](#key-learnings)

---

## Architecture

The project ran on the university's Big Data cluster. Hadoop **HDFS** stored the data, **YARN** handed out the
compute, and **Apache Spark** (through PySpark) ran the jobs. According to the CIM help desk, the "cluster" was
**ten virtual-machine executors on a single physical server**. That detail explains a lot of the scaling behaviour in
the [results](#results--performance-analysis).

### 1. End-to-end workflow

```mermaid
flowchart LR
    DEV["💻 <b>Laptop</b><br/>code + tests"] -->|push| GIT[("<b>GitLab</b>")]
    GIT -->|clone| GW["<b>Gateway</b><br/>SSH · tmux"]
    GW -->|spark-submit| CL["🏛️ <b>Spark on YARN</b><br/>100 GB on HDFS"]
    CL -->|results CSV| VIZ["📊 <b>Laptop</b><br/>plots + map"]
```

Code was written and unit-tested locally, tracked on GitLab (issues, feature branches, merge requests), then
cloned onto the cluster's gateway node over SSH. Each 100 GB job was launched with `time spark-submit` inside a
`tmux` session, so a dropped connection couldn't kill it. Only the small result CSVs came back to the laptop for
plotting, so a chart could be changed without rerunning a 100 GB job.

### 2. Inside the cluster

```mermaid
flowchart LR
    RM["<b>YARN</b><br/>ResourceManager"] -->|launch| DRV["<b>Driver</b><br/>Catalyst · DAG"]
    RM -->|launch| EX["<b>Executors</b><br/>2–10 × N cores"]
    DRV -->|tasks| EX
    DRV -.->|lookup| NN["<b>NameNode</b><br/>block map"]
    DN[("<b>DataNodes</b><br/>128 MB blocks")] -->|read| EX
    EX -->|write| DN
    DRV -.-> UI["<b>Spark UI</b>"]
```

| Component | Role in this project |
|---|---|
| **YARN ResourceManager** | Grants the `--num-executors`, `--executor-cores` and `--executor-memory` requested by `spark-submit`. These were the independent variables in the experiments. |
| **Driver** | Runs the PySpark script. Catalyst optimises the query plan, and the DAG scheduler splits it into stages and tasks. |
| **Executors** | Run tasks in parallel, roughly one HDFS block per task. All 10 share one server's CPU, RAM, disk and network, which is why speed-ups flatten after about 6–8 executors. |
| **NameNode / DataNodes** | HDFS splits files into 128 MB blocks, each replicated 3×. The NameNode tracks where every block lives, so tasks can be scheduled next to their data. |
| **Spark UI** | Used to watch stage progress, executor memory and the DAG, for example to confirm that the regex transformations were pipelined into a single stage. |

### 3. Lifecycle of one job

```mermaid
sequenceDiagram
    actor Me
    participant Y as YARN
    participant D as Driver
    participant E as Executors
    participant H as HDFS
    Me->>Y: time spark-submit
    Y->>D: start driver + executors
    Note over D: lazy plan → action<br/>Catalyst → DAG of stages
    D->>E: 1 task per partition
    E->>H: read 128 MB blocks
    Note over E: narrow stage → shuffle → next stage
    E->>H: write output CSV
    D-->>Me: done · runtime
```

Every program is a chain of **lazy** DataFrame transformations ending in one **action** (`show()` or `write`).
Nothing runs until that action is reached. Then Catalyst optimises the whole chain, merges consecutive *narrow*
operations (`filter`, `withColumn`, `regexp_replace`) into one stage, and starts a new stage only where a *wide*
operation needs a **shuffle**.

### 4. Stages and shuffles

This is the real DAG the Spark UI showed for the Statistical Analysis summary job:

```mermaid
flowchart LR
    subgraph S0["Stage 0 · 838 tasks"]
        direction LR
        A["scan CSV<br/>schema"] --> B["filter TMAX<br/>÷10 · bounds"] --> P["partial<br/>aggregate"]
    end
    subgraph S1["Stage 1 · 1 task"]
        direction LR
        F["final<br/>aggregate"] --> O[("CSV")]
    end
    S0 ==>|shuffle| S1
```

The bulk of the work runs in the 838-task narrow stage, and only small partial results cross the network. How much
each program has to shuffle is what separates their scaling:

| Program | Wide operations | Shuffle cost | Speed-up at 10 executors |
|---|---|---|---|
| Distributed Grep | none: `explode` + `rlike` filter only | none | **10.6×** (vs 1 executor) |
| Word Count | `groupBy(word)` + `orderBy(count)` | large: every distinct word | **9.3×** (vs 1 executor) |
| Statistical Analysis | global aggregate + `groupBy(bucket)` | tiny (~67 MB) | **3.4×** (vs 2 executors) |
| K-Means | `groupBy(STATION)` + a shuffle on every iteration | 1.0–1.8 GB, grows with partitions | **3.0×** (vs 2 executors) |

### 5. Code design

```mermaid
flowchart LR
    T["<b>kMeansTest.py</b><br/>local[1] · mock stations"] --> CC
    L["<b>localKMeans.py</b><br/>100k-row sample"] --> CC
    B["<b>BigData_KMeans.py</b><br/>spark-submit · 100 GB"] --> CC
    CC["<b>ClimateClusterer</b><br/>clean_data · extract_features<br/>apply_kmeans"]
```

The interim programs (Word Count and Grep) are straightforward procedural scripts. For the final phase I switched
to **test-driven, object-oriented code**. `ClimateClusterer` takes an optional `spark_session`, so the same class
runs unchanged in the unit tests, the local prototypes and the full cluster job. The statistical analysis functions
are pure functions tested with **pytest** and PySpark's `assertDataFrameEqual`.

---

## How each program works

### Text workloads: Wikipedia XML (Problems 1 & 2)

```mermaid
flowchart TB
    subgraph PREP["Shared load + clean"]
        direction LR
        W0[("wiki XML<br/>100 GB")] --> W1["spark-xml<br/>1 row / page"] --> W2["articles<br/>only"] --> W3["regex<br/>cleaning"] --> W4["explode<br/>to words"]
    end
    subgraph WC["1 · Word Count"]
        direction LR
        C1["drop junk<br/>words"] --> C2["groupBy<br/>count"] --> C3["top 20"]
    end
    subgraph GR["2 · Distributed Grep"]
        direction LR
        G1["rlike<br/>'queen'"] --> G2["word +<br/>title"]
    end
    PREP --> WC
    PREP --> GR
```

**Shared load + clean** (narrow transformations, no shuffle):
1. Read the XML dump with `spark-xml`, using `rowTag="page"` so that each `<page>` becomes one row.
2. Keep only real articles (`ns == 0`, not redirects, text not null) and pull out `revision.text`.
3. Clean the text with built-in Spark SQL functions rather than UDFs, so Catalyst can optimise them: lowercase it, strip `[[links]]`, `{{templates}}`, `<ref>` and HTML tags and URLs, keep only `a–z`, and drop single letters other than *a* and *i*.
4. `split` + `explode` into one row per word.

#### 1. Word Count: [`product/BigData_WikiWordCount.py`](product/BigData_WikiWordCount.py)
Drops numeric tokens and markup junk words (`infobox`, `wikitable`, `colspan`, …), then
`groupBy("word").count().orderBy(desc)`. The `groupBy` is a wide transformation and triggers the shuffle.

#### 2. Distributed Grep: [`product/BigData_WikiDistributedGrep.py`](product/BigData_WikiDistributedGrep.py)
Keeps the words matching a regex (`"queen"` in the benchmarks) and returns `(matched_string, article title)` pairs.
It is a pure filtering workload with no global aggregation, which is why it scales best of the four programs.

### Numerical workloads: NOAA GHCN-Daily (Problems 3 & 4)

Both programs read the NOAA CSVs (1900–2025, ~100 GB) with an **explicit `StructType` schema**. This skips a
schema-inference pass over 100 GB and lets Catalyst prune unused columns. They then apply the same cleaning:
`TMAX` records only, tenths of °C converted to °C, nulls dropped, and values outside −100…60 °C removed.

#### 3. Statistical Analysis: [`product/BigData_StatAnalysis.py`](product/BigData_StatAnalysis.py)

```mermaid
flowchart LR
    IN[("<b>NOAA CSVs</b><br/>~100 GB")] --> CL["schema read<br/>TMAX · °C · bounds"]
    CL --> S1["count · min · max<br/>approx. median"] --> O1[("summary CSV")]
    CL --> S2["Bucketizer<br/>10 bins · count"] --> S3["range<br/>labels"] --> O2[("distribution CSV")]
```

- **Summary statistics** are calculated in one distributed aggregation. The median uses `percentile_approx`, because an exact median would need a full global sort.
- **Frequency distribution**: MLlib's `Bucketizer` puts every temperature into one of 10 equal-width bins, and a `groupBy(bucket)` counts them. Readable range labels are then joined on.
- Both outputs are written to HDFS as CSV, which is easier to inspect and move than Parquet.

#### 4. K-Means Clustering: [`product/BigData_KMeans.py`](product/BigData_KMeans.py) + [`product/Testing/src/kMeans.py`](product/Testing/src/kMeans.py)

```mermaid
flowchart TB
    subgraph CLU["On the cluster"]
        direction LR
        K0["clean<br/>TMAX"] --> K1["per-station<br/>features"] --> K2["assemble<br/>+ scale"] --> K3["KMeans<br/>k = 5"] --> KO[("clusters<br/>CSV")]
    end
    subgraph LOC["Locally"]
        direction LR
        V1["download"] --> V2["join<br/>lat / long"] --> V3["rank zones<br/>cold → hot"] --> V4["map +<br/>scatter"]
    end
    CLU --> LOC
```

- **`extract_features`** computes `avg_temp`, `max_temp`, `min_temp` and `temp_range` for each station.
- **`apply_kmeans`** runs `VectorAssembler(avg_temp, temp_range)` → `StandardScaler` → `KMeans(k=5, seed=42)`, and writes the clustered stations to HDFS.
- **Locally:** [`BigData_KMeans_Map.py`](product/BigData_KMeans_Map.py) joins the clusters with NOAA station coordinates, ranks the zones from coldest to hottest and draws an interactive Plotly world map. [`BigData_KMeans_Scatter.py`](product/BigData_KMeans_Scatter.py) draws the average temperature against the temperature range.

The map is at the [top of this README](#-final-result-global-temperature-zones-from-100-gb-of-weather-data). The
scatter below shows the same five zones in feature space. The model separates stations by **both** features:
Zone 3, for example, is warm on average but has an extreme range, which is typical of continental interiors.

<img src="docs/images/kmeans_scatter.png" alt="Scatter plot of mean daily max temperature against temperature range, coloured by zone">

The clustered output from the cluster run is included at
[`docs/data/temperature_zones_1900_2025.csv`](docs/data/temperature_zones_1900_2025.csv), so the visualisations can
be rebuilt without Spark using [`docs/map/build_map.py`](docs/map/build_map.py).

> **Design decision:** the first version also clustered on precipitation. That produced clusters with no
> geographic pattern, so precipitation was dropped. Clustering on temperature alone gives zones that line up
> with real climate bands: Scandinavia, Canada and Russia in the coldest zones, and Central Africa, South-East Asia
> and Northern Australia in the hottest.

---

## Results & performance analysis

Every job was timed with `time spark-submit …` on the cluster, and its application ID, shuffle size and notes were
logged in the benchmark trackers
([`FYP_performance_testing.xlsx`](product/Performance_testing/FYP_performance_testing.xlsx) and
[`FYP_final_performance_tracker.xlsx`](product/Performance_testing/FYP_final_performance_tracker.xlsx)). The charts
below are built from those spreadsheets by [`docs/charts/build_charts.py`](docs/charts/build_charts.py). The original
analysis notebooks are in [`product/Performance_testing/`](product/Performance_testing).

### At a glance

| | Word Count | Distributed Grep | Statistical Analysis | K-Means |
|---|:---:|:---:|:---:|:---:|
| **Dataset** | Wikipedia · 100 GB | Wikipedia · 100 GB | NOAA · ~100 GB | NOAA · ~100 GB |
| **Slowest → fastest 100 GB run** | 8 h 12 m → 16 m | 4 h 50 m → 11 m | 20 m → 6 m | 15 m → 4 m |
| **Speed-up from scaling out** | **9.3×** | **10.6×** | **3.4×** | **3.0×** |
| **Shuffle per run** | large | none | ~67 MB | 1.0–1.8 GB |
| **Peak throughput** | ~107 MB/s | ~156 MB/s | 293 MB/s | 409 MB/s |
| **Best `shuffle.partitions`** | n/a | n/a | 128 | 800 |

---

### ① Scaling out: what adding executors buys you

> **Takeaway:** the text jobs scale almost perfectly, while the numerical jobs top out at about 3×. The difference is
> how much each one has to shuffle.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/perf_executors_dark.png">
  <img src="docs/images/perf_executors_light.png" alt="Speed-up against number of executors for all four programs, compared with ideal linear scaling">
</picture>

Each line shows speed-up relative to the smallest configuration, against the dashed *ideal* line where doubling the
executors halves the runtime.

- **Word Count and Grep track the ideal line.** Each partition is tokenised and filtered on its own, so the work
  splits cleanly. Grep even finishes slightly *above* the line (10.6× from 10 executors); a likely cause is that a
  single 4 GB executor was memory-constrained.
- **Statistical Analysis and K-Means peel away after 6 executors.** Their fixed costs (shuffle, the final single-task
  stage, and for K-Means a shuffle on every iteration) don't shrink as executors are added. This is Amdahl's law in
  practice. K-Means still reaches the higher throughput (409 vs 293 MB/s), because feature extraction shrinks
  hundreds of millions of records down to one row per station before the iterative stage starts.
- **All 10 executors share one physical server.** CPU, memory bandwidth, disk and network are shared, so no workload
  could scale indefinitely here.

**The runtimes behind those speed-ups:**

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/perf_runtime_dark.png">
  <img src="docs/images/perf_runtime_light.png" alt="Runtime against number of executors for Word Count and Grep in minutes, and for Statistical Analysis and K-Means in seconds">
</picture>

Word Count on one single-core executor took **8 h 12 min**, and ten executors brought it down to **53 min**. Grep went
from **4 h 50 min to 27 min**. On the 4-core executors, Statistical Analysis dropped from 1,207 s to 357 s and K-Means
from 759 s to 256 s. The biggest single drop for both came between 4 and 6 executors, where runtime nearly halved.

**Throughput as executors are added (numerical workloads):**

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/perf_throughput_dark.png">
  <img src="docs/images/perf_throughput_light.png" width="70%" alt="Throughput in MB/s against number of executors for Statistical Analysis and K-Means">
</picture>

Throughput rose from 87 to 293 MB/s for Statistical Analysis and from 138 to 409 MB/s for K-Means. The biggest jump
for both was from 4 to 6 executors. After that, Statistical Analysis levels off between 8 and 10 executors (the
shared server's bandwidth limit), while K-Means slows between 6 and 8 and then climbs again.

---

### ② Scaling the data: 10 GB → 100 GB

> **Takeaway:** Grep's runtime grows in step with the data. Word Count needs enough executors, or its runtime grows
> faster than the data does.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/perf_datasize_dark.png">
  <img src="docs/images/perf_datasize_light.png" alt="Runtime against Wikipedia subset size for Word Count at 2, 6 and 10 executors, and for Distributed Grep">
</picture>

- **Word Count with 2 executors** goes from 10 to 92 minutes as the data grows 10×. With **10 executors** the same
  growth only takes it from 3 to 16 minutes: the extra parallelism absorbs the extra data (Gustafson's law).
- **Grep is near-linear:** 10× the data costs 7.3× the time (95 s → 696 s). Throughput actually *rises* from
  ~108 MB/s to ~147 MB/s, because fixed start-up costs are spread over more data.

**Full Word Count sweep** (every run from the tracker; the fastest per dataset size is in bold). The chart above
plots the 2-, 6- and 10-executor columns:

| Dataset | 2 executors | 4 executors | 6 executors | 8 executors | 10 executors |
|---|---:|---:|---:|---:|---:|
| **10 GB** | 584 s | 396 s | 206 s | 201 s | **158 s** |
| **20 GB** | 988 s | 608 s | 344 s | **271 s** | 274 s |
| **30 GB** | 1,360 s | 906 s | 536 s | 439 s | **293 s** |
| **40 GB** | 1,555 s | 1,019 s | 648 s | 492 s | **402 s** |
| **50 GB** | 2,629 s | 1,108 s | 782 s | 638 s | **445 s** |
| **60 GB** | 3,433 s | 1,307 s | 933 s | 651 s | **618 s** |
| **70 GB** | 3,530 s | 1,678 s | 1,021 s | 942 s | **682 s** |
| **80 GB** | 4,426 s | 1,845 s | 1,228 s | 1,059 s | **869 s** |
| **90 GB** | 4,976 s | 2,136 s | 1,302 s | 962 s | **802 s** |
| **100 GB** | 5,533 s | 2,109 s | 1,396 s | 1,112 s | **957 s** |

From 20 GB upwards, going from 2 to 4 executors gives the biggest cut, and the gains flatten out by 8–10 executors.
The one case where 8 executors beat 10 (20 GB: 271 s vs 274 s) is within run-to-run noise on the shared cluster.

---

### ③ Tuning resources: cores vs memory

> **Takeaway:** cores are what speeds Spark up. Past 4 GB per executor, extra memory bought nothing.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/perf_resources_dark.png">
  <img src="docs/images/perf_resources_light.png" alt="Word Count runtime against total cores, and Word Count and Grep runtime against memory per executor">
</picture>

- **Cores:** going from 4 to 36 total cores cut Word Count on 100 GB from **152 to 21 minutes (7.2×)**. Most of the
  gain came early, between 4 and 12 cores.
- **Memory:** with 6 executors × 4 cores, both jobs were fastest at **4 GB per executor**. 1 GB was too tight, and
  6–10 GB gave no further improvement. Word Count was even slower at 8 GB, which the report puts down to longer
  garbage-collection pauses on bigger heaps. Once each partition fits in memory, the job is bound by CPU and I/O,
  not RAM.

---

### ④ Shuffle partitions: the setting that matters for K-Means

> **Takeaway:** `spark.sql.shuffle.partitions` barely affects a single aggregation, but it is a real tuning knob for
> iterative ML.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/perf_partitions_dark.png">
  <img src="docs/images/perf_partitions_light.png" alt="Runtime and shuffle volume against shuffle partitions for Statistical Analysis and K-Means">
</picture>

All runs used 4 executors × 4 cores, varying only the shuffle partition count.

- **Statistical Analysis** shuffles only ~67 MB at every setting, so the partition count makes little difference.
  Most settings land between 880 and 1,000 s, and the best was **128 partitions (884 s)**. One run at 400
  partitions finished in 627 s, but two repeats took about 1,120 s (the faint dots). It was an anomaly, and a reminder
  of how much noise a shared cluster adds; the line plots the mean.
- **K-Means** shuffles more as partitions increase (1.0 → 1.8 GB), yet the extra parallelism still won up to
  **800 partitions (421 s, 1.5× faster than 16)**. At 1,200 partitions, task-scheduling overhead took over and the
  runtime jumped back to 678 s.

---

### Recommended configuration

For workloads of this size on this cluster (report §5.6):

| Setting | Recommendation | Why |
|---|---|---|
| Executors × cores | **6–8 × 4** | The biggest gains came from 2 → 6 executors; beyond 8 the returns diminish on the shared server |
| Memory per executor | **4 GB** | No improvement beyond 4–6 GB, and requesting less leaves room for other users' jobs |
| `shuffle.partitions` for aggregation | **128–200** | The shuffle volume is fixed (~67 MB), so the default or a moderate value is enough |
| `shuffle.partitions` for iterative ML | **400–800** | Balances parallelism against the growing shuffle volume |

The [`spark-submit` commands](#3-submit-the-jobs) below use 6 × 4 executors, with 128 partitions for Statistical
Analysis and 800 for K-Means.

---

## Repository layout

```
.
├── product/
│   ├── BigData_WikiWordCount.py         # (1) cluster job, 100 GB Wikipedia
│   ├── BigData_WikiDistributedGrep.py   # (2) cluster job, 100 GB Wikipedia
│   ├── BigData_StatAnalysis.py          # (3) cluster job, 100 GB NOAA
│   ├── BigData_KMeans.py                # (4) cluster job, 100 GB NOAA
│   ├── BigData_KMeans_Map.py            # local: interactive plotly world map of clusters
│   ├── BigData_KMeans_Scatter.py        # local: matplotlib scatter of clusters
│   ├── Performance_testing/             # benchmark trackers (.xlsx) + analysis notebooks
│   └── Testing/
│       ├── src/kMeans.py                # ClimateClusterer class (shared with cluster job)
│       ├── src/statAnalysis.py          # stats functions (2023 subset version)
│       ├── tests/                       # unittest / pytest suites using local Spark
│       ├── localKMeans.py, mapVisualisation.py   # small-sample local prototypes
│       └── wikiWordCount.ipynb, wordcount.ipynb, wikiDistributedGrep.py  # early prototypes
├── documents/                           # final report, interim report, professional issues, retrospective
├── docs/
│   ├── images/                          # performance charts (light + dark), world map, scatter
│   ├── charts/                          # build_charts.py: rebuilds the charts from the trackers
│   ├── map/                             # interactive Plotly map (index.html) + build_map.py
│   ├── data/                            # clustered K-Means output from the cluster run (CSV)
│   ├── diary.md                         # development diary (Oct 2025 – Apr 2026)
│   └── original_submission_README.md    # README as submitted
├── requirements.txt
└── pytest.ini
```

---

## How it was set up and run on the cluster

These are the steps used on the university cluster. To reproduce them you need access to a Hadoop/YARN
cluster that has Spark installed. The jobs read the Wikipedia XML with
`com.databricks:spark-xml_2.11:0.12.0` (Scala 2.11, i.e. Spark 2.4).

### 1. Connect and fetch the code
```bash
ssh <username>@<cluster-gateway>
git clone <this-repo-url>
cd <repo>/product
```

### 2. Load the datasets into HDFS
```bash
# Wikipedia: download the English pages-articles dump, decompress it and upload it
wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2
bunzip2 enwiki-latest-pages-articles.xml.bz2
hdfs dfs -mkdir -p /user/<username>/datasets/wikipedia
hdfs dfs -put enwiki-latest-pages-articles.xml /user/<username>/datasets/wikipedia/wiki_100gb.xml
#   (10–90 GB subsets were cut from this file for the dataset-scaling experiments)

# NOAA GHCN-Daily: yearly CSVs 1900–2025
for y in $(seq 1900 2025); do
  wget https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/$y.csv.gz && gunzip $y.csv.gz
done
hdfs dfs -mkdir -p /user/<username>/datasets/weather
hdfs dfs -put *.csv /user/<username>/datasets/weather/
```
> The HDFS paths in the scripts point to the original cluster account (`hdfs:///user/zlac302/...`).
> Change them to your own user directory before running.

### 3. Submit the jobs
`--num-executors` / `--executor-cores` / `--executor-memory` below are the best-performing values from the experiments.

```bash
# (1) Word Count
time spark-submit --packages com.databricks:spark-xml_2.11:0.12.0 \
  --num-executors 6 --executor-cores 4 --executor-memory 6G --driver-memory 4G \
  BigData_WikiWordCount.py

# (2) Distributed Grep
time spark-submit --packages com.databricks:spark-xml_2.11:0.12.0 \
  --num-executors 6 --executor-cores 4 --executor-memory 4G --driver-memory 4G \
  BigData_WikiDistributedGrep.py

# (3) Statistical Analysis
time spark-submit \
  --num-executors 6 --executor-cores 4 --executor-memory 4G --driver-memory 4G \
  --conf spark.sql.shuffle.partitions=128 \
  BigData_StatAnalysis.py

# (4) K-Means
time spark-submit \
  --num-executors 6 --executor-cores 4 --executor-memory 4G --driver-memory 4G \
  --conf spark.sql.shuffle.partitions=800 \
  BigData_KMeans.py
```
Each run was checked in the Spark UI / YARN history server (application IDs are recorded in the notebooks).

### 4. Pull the results back for visualisation
```bash
hdfs dfs -getmerge /user/<username>/outputs/kmeans_temperature_zones product/data/temperature_zones_1900_2025.csv
wget -P product/data https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt
python product/BigData_KMeans_Map.py       # -> product/data/bigdata_temperature_map.html
python product/BigData_KMeans_Scatter.py   # -> product/data/bigdata_temperature_scatter.png
```

---

## Running locally

The full 100 GB jobs need a cluster. The core logic can still be explored on a laptop:

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt    # needs Java 8/11/17 on PATH for PySpark

# Unit tests (local[1] Spark session, small mock DataFrames)
pytest

# Small-sample K-Means prototype: put one NOAA year (e.g. 1950.csv) in product/data/ first
python product/Testing/localKMeans.py
```

The tests check the cleaning rules (TMAX only, tenths of °C to °C, nulls dropped), the per-station feature
values, the K-Means output schema, the summary statistics, and the bucket counts of the frequency distribution.

> **Windows tip:** if PySpark fails with *"Python worker exited unexpectedly"*, set
> `PYSPARK_PYTHON` to the full path of your Python interpreter, or use WSL.

---

## Key learnings

- Adding more executors does not keep speeding things up: on a shared physical host the hardware becomes the limit, and the experiments show where the gains level off.
- Partitioning has to suit the workload. A single-pass aggregation hardly reacts to the shuffle partition count, while an iterative algorithm like K-Means is sensitive to it.
- Approximate algorithms (`percentile_approx`) and explicit schemas are essential at 100 GB scale.
- Separating the cluster computation from local visualisation made iteration much faster.
- Feature choice matters as much as the algorithm (dropping precipitation from the K-Means features).

## Tech stack

PySpark (DataFrame API, Spark SQL, MLlib) · HDFS · YARN · spark-xml · pandas · matplotlib · plotly · Jupyter · pytest / unittest · Git (originally hosted on the university's GitLab)
