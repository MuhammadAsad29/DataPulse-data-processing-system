# DataPulse

### A desktop workbench for exploring datasets and learning parallel processing

DataPulse is a Java Swing application for importing CSV data, exploring numerical columns, and seeing how statistical work can be split across threads or separate worker processes. It combines an interactive data-analysis interface with practical demonstrations of concurrency, benchmarking, and distributed-memory processing.

![Java](https://img.shields.io/badge/Java-8%2B-ED8B00?logo=openjdk&logoColor=white)
![UI](<https://img.shields.io/badge/UI-Java%20Swing-4B6EAF>)
![Dependencies](<https://img.shields.io/badge/Dependencies-JDK%20only-2E8B57>)

---

## What you can do

- **Import and inspect CSV files** — preview rows, identify numerical and text columns, and see valid versus empty or invalid values.
- **Explore descriptive statistics** — calculate count, sum, mean, median, minimum, maximum, range, variance, and standard deviation.
- **Compare two numerical columns** — calculate population/sample covariance and Pearson correlation using rows where both values are valid.
- **Visualize distributions** — view a histogram for the selected numerical column.
- **Benchmark shared-memory processing** — compare sequential work with multiple Java threads, inspect execution time and speedup, and check that parallel results match.
- **Monitor workers** — inspect worker partitions, thread states, completion status, and priority settings.
- **Try distributed processing locally** — send data partitions from the desktop app to two independent Java worker processes over TCP sockets.
- **Learn concurrency concepts** — explore synchronization and race conditions, independent work over multiple arrays, and an OpenMP-inspired Java thread example.

## Requirements

- Java Development Kit (JDK) 8 or later
- A desktop environment capable of displaying Java Swing windows
- No third-party libraries or build tools required

## Quick start

Run these commands from the project root in **Windows PowerShell**:

```powershell
javac -d out src\*.java
java -cp out Main
```

On macOS or Linux, use:

```bash
javac -d out src/*.java
java -cp out Main
```

The application opens with no dataset loaded. In the **Import** tab, choose a CSV file to begin.

## Run the two-worker demo

The distributed demo requires both workers to be running before you start it in the application. Open **three terminals** in the project root.

**Terminal 1 — worker on port 5001**

```powershell
java -cp out DistributedWorker 5001
```

**Terminal 2 — worker on port 5002**

```powershell
java -cp out DistributedWorker 5002
```

**Terminal 3 — launch DataPulse**

```powershell
java -cp out Main
```

Import a dataset with at least two valid numerical values, open the **Distributed** tab, and run the processing action. Stop each worker with **Ctrl+C** when finished.

> **Networking note:** Workers bind to `127.0.0.1`; this is a local demonstration and does not accept connections from other machines. If either default port is already in use, select two different available ports in the application and start the workers on those ports.

## CSV format and data handling

- Files should have a header row followed by comma-separated records and should be UTF-8 encoded.
- Empty lines are skipped. Empty or non-numeric cells are excluded from numerical calculations and reported by the application.
- Paired-column calculations include only rows where **both** selected values are valid numbers.
- The built-in reader is intentionally lightweight: it does **not** support commas inside quoted fields or multiline quoted fields. Use a standard CSV parser or preprocess the file if your data needs those CSV features.
- `sampledataset.csv` is included as a small example. Larger local datasets and the project report are intentionally excluded from the suggested GitHub upload.

## How it works

```text
CSV file
   |
   v
DatasetLoader --> Dataset --> Swing analysis and visualizations
                                |
              +-----------------+-----------------+
              |                                   |
      ParallelProcessor                   DistributedWorker
      Java threads / shared data          Separate JVMs / localhost TCP
              |                                   |
      Statistics.Partial <------ merge partial results
```

The parallel and distributed paths calculate mergeable partial statistics using Welford updates and Chan-style merging, then compare their results with the sequential calculation. Median is calculated separately from the full selected data because it cannot be derived from those partial summaries.

Benchmark timings depend on the machine, JVM, and dataset size. The application uses a warm-up and reports the best of three runs; for small inputs, parallel execution may be slower because thread setup has a cost.

## Project structure

```text
.
├── src/
│   ├── Main.java                 # Swing user interface and application actions
│   ├── Dataset.java              # In-memory dataset representation
│   ├── DatasetLoader.java        # CSV reading and numerical value extraction
│   ├── Statistics.java           # Statistical calculations and mergeable partials
│   ├── ParallelProcessor.java    # Shared-memory workers and thread demonstrations
│   └── DistributedWorker.java    # Socket-based worker process
├── sampledataset.csv             # Small example dataset
├── .gitignore
└── README.md
```

## Push this project to GitHub

Create an **empty public repository** named `DataPulse-Data-Processing-System` under [github.com/MuhammadAsad29](https://github.com/MuhammadAsad29), then run these commands from the project root in PowerShell:

```powershell
git init
git add README.md .gitignore src sampledataset.csv
git commit -m "Add DataPulse data processing system"
git branch -M main
git remote add origin https://github.com/MuhammadAsad29/DataPulse-Data-Processing-System.git
git push -u origin main
```

If this local project already has an `origin` remote, update it instead of adding it again:

```powershell
git remote set-url origin https://github.com/MuhammadAsad29/DataPulse-Data-Processing-System.git
git push -u origin main
```

GitHub will prompt you to authenticate if needed. The commands above intentionally stage the source, README, ignore rules, and small sample only—not the larger CSV files, compiled output, or project report.
