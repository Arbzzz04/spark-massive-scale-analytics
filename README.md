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

🎥 **Demo video:** <https://www.youtube.com/watch?v=Q9mJMntAv3c>
📄 **Full report:** [`documents/ArbaazJaffer.final.pdf`](documents/ArbaazJaffer.final.pdf)

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

The project ran on the university's Big Data cluster. Hadoop **HDFS** stored the data, **YARN** handed out
the compute, and **Apache Spark** (through PySpark) ran the jobs. According to the CIM help desk, the
"cluster" was **ten virtual-machine executors on a single physical server**. That detail explains a lot of the
scaling behaviour in the [results](#results--performance-analysis).

### 1. Cluster & workflow

```mermaid
flowchart TB
    DEV["💻 <b>Local development</b><br/>write code · unit-test with PySpark local[1]"]
    GIT[("GitLab<br/>issues · branches · merge requests")]

    subgraph CLUSTER["🏛️ University Big Data cluster"]
        GW["<b>Gateway node</b><br/>SSH + tmux · git clone<br/>time spark-submit"]
        RM["<b>YARN ResourceManager</b><br/>grants executors · cores · memory"]
        subgraph APP["Spark application"]
            DRV["<b>Driver</b><br/>runs the PySpark script<br/>Catalyst optimiser · DAG scheduler"]
            EX["<b>Executors</b> · 2–10 VMs<br/>all on one physical server<br/>1 task per partition per core"]
        end
        subgraph STORE["HDFS"]
            NN["<b>NameNode</b><br/>file → block locations"]
            DN[("<b>DataNodes</b><br/>128 MB blocks · 3× replicated<br/>Wikipedia XML · NOAA CSVs · outputs")]
        end
        UI["<b>Spark UI</b><br/>Jobs · Executors · DAG"]
    end

    VIZ["📊 <b>Local visualisation</b><br/>pandas · matplotlib · plotly"]

    DEV -->|git push| GIT
    GIT -->|git clone| GW
    GW -->|submit job| RM
    RM -->|launch| DRV
    RM -->|launch| EX
    DRV -->|schedule tasks| EX
    DRV -.- UI
    DRV -.->|looks up block locations| NN
    DN -->|read partitions| EX
    EX -->|write result CSVs| DN
    DN -->|hdfs dfs -get| VIZ
```

| Component | Role in this project |
|---|---|
| **Gateway + tmux** | Reached over SSH. The code was `git clone`d here, and 100 GB jobs were launched inside `tmux` so that a dropped SSH connection couldn't kill them. Each job was wrapped in `time` to record its runtime. |
| **YARN ResourceManager** | Grants the `--num-executors`, `--executor-cores` and `--executor-memory` requested on the `spark-submit` line. These were the independent variables in the experiments. |
| **Spark driver** | Runs the PySpark script and builds the logical plan. Catalyst optimises the plan, and the DAG scheduler splits it into stages and tasks. |
| **Executors** | Run the tasks in parallel, roughly one HDFS block (≈128 MB) per task. All 10 share the same server's CPU, RAM, disk and network, which is why speed-ups flatten after about 6–8 executors. |
| **HDFS** | Holds the 100 GB Wikipedia dump, its 10–90 GB subsets, ~100 GB of NOAA weather CSVs and every job's output. The NameNode tracks where blocks live, so Spark can schedule tasks next to their data. |
| **Spark UI** | Used to watch stage progress, executor memory and task spread, and the DAG, to check that the regex transformations were pipelined into a single stage. |

### 2. Lifecycle of one job

```mermaid
sequenceDiagram
    autonumber
    actor Me as Me (SSH + tmux)
    participant Y as YARN
    participant D as Spark Driver
    participant E as Executors
    participant H as HDFS

    Me->>Y: time spark-submit --num-executors 6 --executor-cores 4 ...
    Y->>D: start driver, allocate executor containers
    D->>D: run script: transformations are only recorded (lazy)
    Note over D: Action reached (show / write)<br/>Catalyst optimises the whole plan<br/>DAG is cut into stages at each shuffle
    D->>E: schedule one task per input partition
    E->>H: read 128 MB blocks
    E->>E: narrow stage: parse, filter, clean (no data movement)
    E->>E: Exchange: shuffle rows by key (groupBy / aggregation)
    alt show()
        E->>D: top results returned to driver
    else write()
        E->>H: write output CSV
    end
    D-->>Me: job finished, runtime printed by time
```

### 3. How Spark actually executed it: stages and shuffles

Every program is written as a chain of **lazy DataFrame transformations** followed by one **action**. Nothing runs
until the action is reached. Catalyst then optimises the whole chain, merges consecutive *narrow* operations
(filter, `withColumn`, `regexp_replace`) into one stage, and starts a new stage only where a *wide* operation needs
a **shuffle**. Here is the actual DAG from the Spark UI for the Statistical Analysis summary job:

```mermaid
flowchart TB
    subgraph S0["<b>Stage 0</b> · 838 tasks"]
        direction LR
        A["scan NOAA CSVs<br/>explicit schema"] --> B["filter<br/>ELEMENT = TMAX"] --> C["value ÷ 10<br/>→ °C"] --> D["drop nulls<br/>keep −100…60 °C"] --> P["partial aggregate<br/>per partition"]
    end
    subgraph S1["<b>Stage 1</b> · 1 task"]
        direction LR
        F["final aggregate<br/>count · min · max · approx. median"] --> O[("summary CSV<br/>→ HDFS")]
    end
    S0 ==>|"Exchange (shuffle)"| S1
```

Most of the work happens in the parallel narrow stage, and only small partial results cross the network. How much
each program shuffles is what separates their performance:

| Program | Wide operations | Shuffle cost | Scaling (2 or 1 → 10 executors) |
|---|---|---|---|
| Distributed Grep | none: `explode` + `rlike` filter only | none | best: **10.6×** |
| Word Count | `groupBy(word)` + `orderBy(count)` | large: every distinct word | **9.3×** |
| Statistical Analysis | global aggregate + `groupBy(bucket)` | tiny (~67 MB) | **3.4×** |
| K-Means | `groupBy(STATION)` + a shuffle on every K-Means iteration | 1.0–1.8 GB, grows with partitions | **3.0×** |

### 4. Code design

The interim programs (Word Count and Grep) are straightforward procedural scripts. For the final phase I switched
to **test-driven, object-oriented code**. The K-Means logic lives in one `ClimateClusterer` class, and its constructor
accepts an optional `spark_session`. The same code therefore runs unchanged in three places:

```mermaid
flowchart LR
    T["tests/kMeansTest.py<br/>injects local[1] session<br/>mock stations"] --> CC
    L["localKMeans.py · mapVisualisation.py<br/>100k-row local sample"] --> CC
    B["BigData_KMeans.py<br/>session from spark-submit<br/>full 100 GB on cluster"] --> CC
    CC["ClimateClusterer<br/>Testing/src/kMeans.py<br/>noaa_schema · clean_data()<br/>extract_features() · apply_kmeans()"]
```

The statistical analysis functions (`get_summary_stats`, `get_frequency_distribution`) are pure functions
tested with **pytest** and PySpark's `assertDataFrameEqual`.

---

## How each program works

### Text workloads: Wikipedia XML (Problems 1 & 2)

```mermaid
flowchart TD
    W0[("HDFS · wiki_100gb.xml")] --> W1["spark-xml reader<br/>rowTag = page → one row per article"]
    W1 --> W2["keep real articles<br/>ns = 0 · not a redirect · text not null"]
    W2 --> W3["shared cleaning · built-in Spark SQL functions, no UDFs<br/>lower() · strip links, templates, refs, HTML, URLs<br/>keep a–z only · drop stray single letters"]
    W3 --> W4["split + explode<br/>one row per word"]
    W4 --> C1["drop numbers and<br/>markup junk words"]
    C1 --> C2["groupBy word · count<br/>wide → shuffle"]
    C2 --> C3["orderBy count desc<br/>show top 20"]
    W4 --> G1["filter word rlike 'queen'<br/>narrow → no shuffle"]
    G1 --> G2["output matched_string<br/>+ article title"]

    subgraph WC["1 · Word Count"]
        C1
        C2
        C3
    end
    subgraph GR["2 · Distributed Grep"]
        G1
        G2
    end
```

### 1. Word Count: [`product/BigData_WikiWordCount.py`](product/BigData_WikiWordCount.py)
1. Read the XML dump with `spark-xml`, using `rowTag="page"` so that each `<page>` becomes one row.
2. Keep only real articles (`ns == 0`, not redirects) and pull out `revision.text`.
3. Clean the text with a chain of `regexp_replace` calls: lowercase, strip `[[links]]`, `{{templates}}`, `<ref>` and HTML tags and URLs, keep only `a–z`, and drop single letters other than *a* and *i*.
4. `split` + `explode` into words, then remove numeric tokens and markup junk words (`infobox`, `wikitable`, `colspan`, …).
5. `groupBy("word").count().orderBy(desc)`.

### 2. Distributed Grep: [`product/BigData_WikiDistributedGrep.py`](product/BigData_WikiDistributedGrep.py)
Uses the same load-and-clean pipeline as Word Count, then explodes each article into words and keeps those matching a
regex (`"queen"` in the benchmarks). It returns `(matched_string, location=article title)` pairs.

### Numerical workloads: NOAA GHCN-Daily (Problems 3 & 4)

```mermaid
flowchart TD
    N0[("HDFS · NOAA GHCN-Daily CSVs<br/>1900–2025 · ~100 GB")] --> N1["read with explicit StructType schema<br/>no inference pass · column pruning"]
    N1 --> N2["clean: ELEMENT = TMAX · ÷10 → °C<br/>drop nulls · keep −100…60 °C"]

    subgraph SA["3 · Statistical Analysis"]
        S1["count · min · max<br/>percentile_approx median"]
        S2["Bucketizer: 10 equal-width bins<br/>groupBy bucket · count"]
        S3["join readable range labels"]
        S2 --> S3
    end

    subgraph KM["4 · K-Means"]
        K1["groupBy STATION<br/>avg · max · min · range"]
        K2["VectorAssembler<br/>avg_temp + temp_range"]
        K3["StandardScaler"]
        K4["KMeans k = 5 · seed 42<br/>iterates to convergence"]
        K1 --> K2 --> K3 --> K4
    end

    N2 --> S1
    N2 --> S2
    N2 --> K1
    S1 --> O1[("summary + distribution CSVs")]
    S3 --> O1
    K4 --> O2[("clustered stations CSV")]
    O2 -- "hdfs dfs -get" --> V1["join ghcnd-stations.txt lat/long<br/>rank zones coldest → hottest"]
    V1 --> V2["plotly world map<br/>matplotlib scatter"]
```

### 3. Statistical Analysis: [`product/BigData_StatAnalysis.py`](product/BigData_StatAnalysis.py)
1. Read the NOAA CSVs with an **explicit schema**, which avoids a schema-inference pass over 100 GB.
2. Filter to `TMAX` records, convert tenths of °C to °C, and remove nulls and implausible values (outside −100 to 60 °C).
3. Compute count, min, max and an **approximate median** (`percentile_approx`), because an exact median would need a full sort.
4. Build a 10-bucket frequency distribution with MLlib's `Bucketizer` and write both outputs to HDFS as CSV.

### 4. K-Means Clustering: [`product/BigData_KMeans.py`](product/BigData_KMeans.py) + [`product/Testing/src/kMeans.py`](product/Testing/src/kMeans.py)
The logic sits in a reusable, unit-tested `ClimateClusterer` class:
1. `clean_data`: keeps TMAX records and converts them to °C.
2. `extract_features`: for each station, computes `avg_temp`, `max_temp`, `min_temp` and `temp_range`.
3. `apply_kmeans`: `VectorAssembler(avg_temp, temp_range)` → `StandardScaler` → `KMeans(k=5, seed=42)`.
4. Writes the clustered stations to HDFS. [`BigData_KMeans_Map.py`](product/BigData_KMeans_Map.py) then joins them with NOAA station coordinates and draws an interactive world map, and [`BigData_KMeans_Scatter.py`](product/BigData_KMeans_Scatter.py) draws a scatter plot of the clusters.

> **Design decision:** the first version also clustered on precipitation. That produced clusters with no
> geographic pattern, so precipitation was dropped. Clustering on temperature alone gives zones that line up
> with real climate bands on the map.

---

## Results & performance analysis

Every job was timed with `time spark-submit …` on the cluster. The numbers below come from the notebooks in
[`product/Performance_testing/`](product/Performance_testing), which are saved with their outputs, so you can
read them on GitHub without running anything.

### Headline numbers

| Program | Experiment | Slowest → fastest | Speed-up |
|---|---|---|---|
| Word Count (100 GB) | 1 → 10 executors (1 core each) | 29,515 s → 3,158 s | **9.3×** |
| Word Count (100 GB) | 4 → 36 total cores | 9,136 s → 1,276 s | **7.2×** |
| Distributed Grep (100 GB) | 1 → 10 executors (1 core each) | 17,384 s → 1,646 s | **10.6×** |
| Statistical Analysis (100 GB) | 2 → 10 executors (4 cores each) | 1,207 s → 357 s | **3.4×** (87 → 293 MB/s) |
| K-Means (100 GB) | 2 → 10 executors (4 cores each) | 759 s → 256 s | **3.0×** (138 → 409 MB/s) |

### Word Count: runtime vs executors, and vs dataset size (10–100 GB)
<p>
  <img src="docs/images/wordcount_2.png" width="49%" alt="Word count runtime vs executors">
  <img src="docs/images/wordcount_3.png" width="49%" alt="Word count runtime by dataset size">
</p>

### Distributed Grep: runtime vs executors, and vs dataset size
<p>
  <img src="docs/images/distributedgrep_1.png" width="49%" alt="Grep runtime vs executors">
  <img src="docs/images/distributedgrep_2.png" width="49%" alt="Grep runtime vs dataset size">
</p>

### Statistical Analysis & K-Means: speed-up compared with ideal linear scaling
<p>
  <img src="docs/images/statanalysis_2.png" width="49%" alt="Stat analysis speedup">
  <img src="docs/images/kmeans_2.png" width="49%" alt="K-Means speedup">
</p>

### K-Means: runtime and shuffle volume vs shuffle partitions
<p>
  <img src="docs/images/kmeans_3.png" width="49%" alt="K-Means runtime vs partitions">
  <img src="docs/images/kmeans_4.png" width="49%" alt="K-Means shuffle volume vs partitions">
</p>

### What the experiments showed
- **The text workloads parallelise very well.** Word Count and Grep get close to a 10× speed-up from 10 executors, because tokenising and filtering are almost entirely independent per partition.
- **Gains shrink after about 6–8 executors** (Amdahl's law). All 10 "nodes" are VMs on **one physical server**, so they share CPU, memory bandwidth, disk and network.
- **Runtime grows roughly linearly with data size** (Grep, 10 → 100 GB), so throughput stays steady as the input grows.
- **Iterative ML scales worse than a single-pass aggregation.** Each K-Means iteration triggers a shuffle to reassign points to centroids, which keeps its speed-up further below the ideal line than Statistical Analysis.
- **Shuffle partitions matter more for K-Means.** Shuffle volume grows with the partition count (1.0 → 1.8 GB), while Statistical Analysis stays at about 67 MB. The fastest K-Means run used 800 partitions (421 s), and the fastest Statistical Analysis run used 128 (884 s). These are the values used in the recommended commands below.

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
│   ├── Performance_testing/             # pre-rendered Jupyter notebooks with results + charts
│   └── Testing/
│       ├── src/kMeans.py                # ClimateClusterer class (shared with cluster job)
│       ├── src/statAnalysis.py          # stats functions (2023 subset version)
│       ├── tests/                       # unittest / pytest suites using local Spark
│       ├── localKMeans.py, mapVisualisation.py   # small-sample local prototypes
│       └── wikiWordCount.ipynb, wordcount.ipynb, wikiDistributedGrep.py  # early prototypes
├── documents/                           # final report, interim report, professional issues, retrospective
├── docs/
│   ├── images/                          # charts exported from the notebooks
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
