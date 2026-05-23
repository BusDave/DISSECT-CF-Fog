# Szakdolgozat-hozzájárulás: GNN-alapú workflow-ütemező pipeline


A jelen README a `simulator/README.md` (az upstream DISSECT-CF-Fog
dokumentáció) **kiegészítője**, nem helyettesítője — a Maven-build, Eclipse-
és IntelliJ-konfiguráció szabványos lépéseit ott találod.

---

## 0. Telepítendő csomagok és OS-támogatás

A teljes pipeline futtatásához két környezet kell: egy Java-oldal a
szimulátor és az ONNX-inferencia futtatásához, és egy Python-oldal a
workflow-generáláshoz, a GNN-tréninghez és az eredmények elemzéséhez.

### 0.1 Java-oldal

| Csomag | Verzió | OS-támogatás | Megjegyzés |
|---|---|---|---|
| **JDK** | 11+ | Win / Mac / Linux | Bármely OpenJDK 11+ disztribúció (Temurin, Zulu, Oracle) |
| **Maven** | 3.9+ | Win / Mac / Linux | IDE-k (IntelliJ, Eclipse) saját Maven-támogatást biztosítanak |
| **onnxruntime (Java)** | 1.19.2 | Win / Mac / Linux | Maven dependency a `pom.xml`-ben, automatikusan letöltődik. CPU-only, nincs GPU-kód |

A Maven-build a `pom.xml`-en keresztül automatikusan letölti az
`onnxruntime` Java-bindinget (Maven Central). Külön natív könyvtár-
telepítés nem szükséges; az ONNX Runtime saját, OS-specifikus
natív könyvtárakat csomagol a JAR-ba.

### 0.2 Python-oldal

A Python-csomagok telepítése egy lépésben, a `simulator/` mappából:

```bash
pip install -r requirements.txt
```

> Az upstream `src/main/resources/script/requirement.txt` (régebbi, csak az
> upstream szkriptekhez) is megmaradt visszafelé kompatibilitás miatt — a
> szakdolgozati pipeline-hoz a top-level `requirements.txt` használandó.

A telepített csomagok és OS-támogatásuk:

| Csomag | Mire való | OS-támogatás | Megjegyzés |
|---|---|---|---|
| **Python** 3.8+ | Futtatókörnyezet | Win / Mac / Linux | A Python is OS-független |
| **numpy**, **pandas** | Numerikus + tábla-műveletek | Win / Mac / Linux | Pure-Python wheel-ek |
| **torch** ≥ 2.0 | PyTorch — modell és tréning | Win / Mac / Linux | CPU-verzió mindenhol működik; GPU-verzió platform-specifikus, de **nem szükséges** (a CPU-tréning is 5 perc alatt fut) |
| **onnx** + **onnxruntime** | ONNX-export + parity-validáció | Win / Mac / Linux | A pip-csomagok mindhárom OS-re elérhetők |
| **matplotlib** | Grafikongenerálás | Win / Mac / Linux | Pure-Python plotting |
| **wfcommons** ≥ 2.0 | WfCommons workflow-receptek | Win / Mac / Linux | Pure-Python csomag |
| **notebook** (Jupyter) | A `train_gnn.ipynb` notebook futtatása | Win / Mac / Linux | Alternatíva: `jupyterlab` |
| **folium** | Térkép-vizualizálás (upstream) | Win / Mac / Linux | Pure-Python |
| **pydotplus** | DAG-PDF vizualizálás (upstream `DAG.py`) | Win / Mac / Linux | **⚠️ Külön Graphviz binár kell**: `brew install graphviz` (Mac), `apt install graphviz` (Linux), vagy [graphviz.org/download](https://graphviz.org/download/) (Win) |

### 0.3 Jupyter Notebook indítása

A `train_gnn.ipynb` notebookot a `notebooks/` mappából indítjuk:

```bash
cd notebooks/
jupyter notebook              # vagy: jupyter lab
```

A böngészőben megnyílik a Jupyter-felület, ahol a `train_gnn.ipynb`-re
kattintva betöltődik a tréning-pipeline. A cellákat sorban kell
futtatni (Shift+Enter), vagy egyben a "Run All" gombbal.

### 0.4 Telepítés-validáció

Egy gyors ellenőrző parancs, hogy minden a helyén van-e:

```bash
python3 -c "import torch, onnxruntime, wfcommons, numpy, pandas; \
            print(f'torch={torch.__version__}, ort={onnxruntime.__version__}, wfcommons OK')"
```

Ha hibaüzenet nélkül lefut, a Python-környezet kész. A Java-oldali
ellenőrzés egyszerűen az IDE-ben (vagy `mvn compile`-lal) történő
fordítás sikeressége.

---

## 1. A pipeline 5 szakaszban

```
┌──────────────────────────┐    ┌──────────────────────────┐    ┌──────────────────────────┐
│  1. Korpusz-generálás    │───▶│  2. Tréning-CSV-k        │───▶│  3. GNN tréning           │
│     (Python)             │    │     (Java)               │    │     (Python notebook)    │
└──────────────────────────┘    └──────────────────────────┘    └──────────────────────────┘
                                                                                │
                                                                                ▼
                              ┌──────────────────────────┐    ┌──────────────────────────┐
                              │  5. Eredmények elemzése  │◀───│  4. Benchmark futtatása  │
                              │     (Python)             │    │     (Java)               │
                              └──────────────────────────┘    └──────────────────────────┘
```

### Quick start — előre tanított modellel

Az `src/main/resources/models/gnn_scheduler.onnx` (+ `.onnx.data`) már
tartalmazza a betanított modellt, így a 3-as tréning-lépés átugorható.
A benchmark-runner azonban 20 generált workflow-t használ (`blast_*`,
`montage_*`, `cycles_*` stb.) — ezek `.gitignore`-olva vannak, mert
regenerálhatók, ezért **a workflow-korpuszt egyszer le kell futtatni**:

```bash
# A simulator/ mappából
pip install -r requirements.txt                                            # 1×
python3 src/main/resources/script/generate_training_corpus.py              # ~2 perc
mvn -q exec:java -Dexec.mainClass=hu.u_szeged.inf.fog.simulator.demo.SchedulerBenchmarkRunner
```

A benchmark kiírja a `sim_res/benchmark_results.csv`-t (~30 másodperc,
150 szimuláció: 5 ütemező × 10 workflow × 3 seed).

### Full reproduction — teljes pipeline

A parancsok mind a `simulator/` mappából futtatandók. Előfeltétel:
- Python-csomagok telepítve (`pip install -r requirements.txt`)
- Maven-build lefutott (`mvn -q -DskipTests install`)

```bash
# 1. Generáljuk a 200-elemű workflow-korpuszt (~2 perc)
python3 src/main/resources/script/generate_training_corpus.py
# → src/main/resources/demo/WORKFLOW_examples/*.xml

# 2. Generáljuk a tréning-CSV-ket (~5 perc)
mvn -q exec:java -Dexec.mainClass=hu.u_szeged.inf.fog.simulator.demo.BatchTrainingRunner
# → sim_res/training/*.csv + *.meta.json

# 3. Tanítsuk be a GNN-modellt (~5 perc CPU-n)
cd notebooks && jupyter notebook train_gnn.ipynb     # Run All; cd .. utána
# → src/main/resources/models/gnn_scheduler.onnx + .onnx.data

# 4. Futtassuk a teljes benchmark-ot (~30 másodperc)
mvn -q exec:java -Dexec.mainClass=hu.u_szeged.inf.fog.simulator.demo.SchedulerBenchmarkRunner
# → sim_res/benchmark_results.csv

# 5. Elemzés + ábragenerálás
python3 notebooks/analyze_benchmark.py
# → sim_res/benchmark_{summary,makespan,energy,slr,speedup}.{csv,png,pdf}

# 6. (Opcionális) 25-csomópontos skálázhatósági teszt — ugyanaz az ONNX-modell
mvn -q exec:java -Dexec.mainClass=hu.u_szeged.inf.fog.simulator.demo.GnnScalabilityDemo
# → sim_res/scalability_25node_results.csv + logok
```

> A `mvn exec:java` plugin **nincs explicit deklarálva** a `pom.xml`-ben, de a
> Maven le tudja húzni futáskor. Ha mégis hibázik, IDE-ben (IntelliJ / Eclipse)
> érdemes Run Configuration-t csinálni a megfelelő főosztályra, vagy
> alternatívaként: `mvn -q dependency:build-classpath` → a kapott classpath-szel
> `java -cp ... hu.u_szeged.inf.fog.simulator.demo.<Class>` indítás.

---

## 2. Fájl-térkép — minden hozzáadott fájl és mappa

### 2.1 Java-oldal — ütemezők

| Fájl | Mire való |
|---|---|
| `src/main/java/.../workflow/scheduler/HeftScheduler.java` | Klasszikus HEFT + DVR-HEFT (3-változatos rang); olvas: workflow XML; ad: ütemezési döntések |
| `src/main/java/.../workflow/scheduler/HeftDsScheduler.java` | Energia-tudatos HEFT-DS — kritikus út + sub-deadline + slack-elosztás; örökli HEFT-et |
| `src/main/java/.../workflow/scheduler/AdaptiveHeftScheduler.java` | DAG-szélesség alapú regime switching (EFT-mohó ↔ round-robin); örökli HEFT-DS-t |
| `src/main/java/.../workflow/scheduler/MaxMinScheduler.java` | Klasszikus Max-Min baseline ütemező |
| `src/main/java/.../workflow/scheduler/GnnScheduler.java` | **(új)** GNN-alapú tanult policy; olvas: ONNX-modell; ad: csomópont-választás vagy fallback |

### 2.2 Java-oldal — tréning-támogatás

| Fájl | Mire való |
|---|---|
| `src/main/java/.../workflow/scheduler/training/HeftSchedulerWithLogging.java` | HEFT + CSV-logger wrapper minden döntéshez |
| `src/main/java/.../workflow/scheduler/training/HeftDsSchedulerWithLogging.java` | HEFT-DS + CSV-logger wrapper |
| `src/main/java/.../workflow/scheduler/training/AdaptiveHeftSchedulerWithLogging.java` | Adaptive HEFT + CSV-logger wrapper (diagnosztikai, tréningbe nem kerül) |
| `src/main/java/.../workflow/scheduler/training/MaxMinSchedulerWithLogging.java` | Max-Min + CSV-logger wrapper (diagnosztikai) |
| `src/main/java/.../workflow/scheduler/training/TrainingLogger.java` | Egy futás döntéseit CSV-be ír + JSON-mellékletet (makespan, energy, workflow XML útja) |
| `src/main/java/.../workflow/scheduler/training/SchedulerFeatureExtractor.java` | 50-dim állapotvektor-építő (task + global + per-node feature-ök) |
| `src/main/java/.../workflow/scheduler/training/GnnGraphBuilder.java` | **(új)** A két-gráfos GNN-bemenet builder-je (workflow + cluster gráfok); Java-mása a Python `gnn_data_pipeline.py`-nak |

### 2.3 Java-oldal — demók és futtatók

| Fájl | Mire való |
|---|---|
| `src/main/java/.../demo/TrainingSetup.java` | Közös 20-csomópontos klaszter-konfiguráció (3 cloud + 12 fog + 5 edge); minden runner ezt használja |
| `src/main/java/.../demo/BatchTrainingRunner.java` | Iterálja a `WORKFLOW_examples/*.xml`-eket, mind a 4 WithLogging-ütemezővel futtatja; olvas: workflow XML-ek; ad: `sim_res/training/*.csv` + `*.meta.json` |
| `src/main/java/.../demo/SchedulerBenchmarkRunner.java` | 10-elemű kiválasztott korpuszon mind az 5 ütemezőt (HEFT, HEFT-DS, Max-Min, Adaptive, GNN) futtatja 3 seeddel; olvas: workflow XML-ek + ONNX-modell; ad: `sim_res/benchmark_results.csv` |
| `src/main/java/.../demo/GnnScalabilityDemo.java` | **(új)** A GNN-policy futtatása 24-csomópontos klaszteren (5 plusz csomóponttal), ugyanazzal az ONNX-modellel; cluster-méret függetlenség validáció |

### 2.4 Python-oldal — workflow-generálás

| Fájl | Mire való |
|---|---|
| `src/main/resources/script/workflow_grammar.py` | **(új)** Kompozíciós gráf-grammatika (Pipeline, FanOut, ForkJoin, Stencil, ... minták); chaotic DAG-okat épít |
| `src/main/resources/script/random_workflow_generator.py` | **(új)** A grammar felett egy random DAG-építő wrapper; paraméterek: `--nodes`, `--max-depth`, `--chaos`, `--seed` |
| `src/main/resources/script/generate_workflows.py` | **(új)** A `wfcommons` könyvtár wrapper-e: paraméterek: `--type {blast,bwa,cycles,...}`, `--tasks N` |
| `src/main/resources/script/wfformat_to_dissect_converter.py` | **(új)** A WfCommons JSON-formátumot a DISSECT-CF XML-jére konvertálja |
| `src/main/resources/script/generate_training_corpus.py` | **(új)** A két generátort együtt futtatja: 9 WfCommons-recept × 15 méret + 65 chaotic = 200 workflow |
| `src/main/resources/script/requirement.txt` | A jelen szakdolgozat által bővített Python-függőség lista (eredetileg upstream; a thesis-csomagokkal kiegészítve) |

> A `DAG.py`, `map.py`, `clusterMap.py` szkriptek az upstream DISSECT-CF-Fog részei,
> nem a szakdolgozat hozzájárulásai — a kísérleti vizualizációhoz az upstream
> kódbázis biztosítja őket, így itt nem listázzuk.

### 2.5 Python-oldal — GNN-tréning és elemzés

| Fájl | Mire való |
|---|---|
| `notebooks/gnn_data_pipeline.py` | **(új)** CSV-betöltő + gráf-rekonstruktor; olvas: `sim_res/training/*.csv` + `*.meta.json` + `WORKFLOW_examples/*.xml`; ad: `GraphSample` objektumok a tanításhoz |
| `notebooks/gnn_model.py` | **(új)** Tiszta PyTorch GraphSAGE-modell (`GraphSageBlock`, `GnnScheduler`, `GnnSchedulerInference`); ONNX-export-barát |
| `notebooks/train_gnn.ipynb` | **(új)** A tréning notebook: betölti a CSV-ket, gráfminta-listává alakítja, 200 epoch tréning, ONNX-export, parity-validáció |
| `notebooks/analyze_benchmark.py` | A benchmark eredmények aggregálása + grafikongenerálás (makespan, energia, SLR sávdiagram, speedup scatter) |

### 2.6 Erőforrások — modell és workflow-k

| Fájl/mappa | Mire való |
|---|---|
| `src/main/resources/models/gnn_scheduler.onnx` | **(új)** A betanított GNN-modell architektúrája ONNX-formátumban (~14 kB) |
| `src/main/resources/models/gnn_scheduler.onnx.data` | **(új)** A modell-súlyok külön bináris fájlban (~167 kB) |
| `src/main/resources/demo/WORKFLOW_examples/CyberShake_100.xml` | Az upstream demo CyberShake workflow |
| `src/main/resources/demo/WORKFLOW_examples/IoT_CyberShake_100.xml` | IoT-CyberShake variáns (workflow + szenzor-stream) |
| `src/main/resources/demo/WORKFLOW_examples/IoT_workflow.xml` | Klasszikus IoT demo workflow |
| `src/main/resources/demo/WORKFLOW_examples/*.xml` (gitignored) | A 200-elemű korpusz generált fájljai; reprodukálhatók a `generate_training_corpus.py`-vel |

### 2.7 Eredmények és benchmark output

| Fájl/mappa | Mire való |
|---|---|
| `sim_res/training/` | A BatchTrainingRunner kimenete; üres mappa a git-ben, futtatás után megtelik `*.csv` és `*.meta.json` fájlokkal (regenerálható) |
| `sim_res/benchmark_results.csv` | A SchedulerBenchmarkRunner soronkénti raw kimenete (workflow × scheduler × seed) |
| `sim_res/benchmark_summary.csv` | Az `analyze_benchmark.py` aggregált összefoglalója (átlag ± szórás workflow-nként) |
| `sim_res/benchmark_makespan.png` | Sávdiagram: makespan ütemezőnként |
| `sim_res/benchmark_energy.png` | Sávdiagram: energiafogyasztás ütemezőnként |
| `sim_res/benchmark_slr.png` és `.pdf` | Normalizált SLR sávdiagram (HEFT = 1.0 baseline) |
| `sim_res/benchmark_speedup.png` | GNN vs HEFT speedup scatter |

---

## 3. Adat-folyamat — mi olvas mit és hova ír

```
WfCommons recipes ──► generate_training_corpus.py ──► WORKFLOW_examples/*.xml
                                                              │
                                                              ▼
                       BatchTrainingRunner.java   ◄─── 20-csomópontos klaszter
                                  │                        (TrainingSetup.java)
                                  ▼
                       sim_res/training/*.csv
                       sim_res/training/*.meta.json
                                  │
                                  ▼
                       gnn_data_pipeline.py  ──► GraphSample-ek
                                                       │
                                                       ▼
                              train_gnn.ipynb ──► gnn_scheduler.onnx
                                                       │
                                                       ▼
                       SchedulerBenchmarkRunner.java
                                  │
                                  ▼
                       sim_res/benchmark_results.csv
                                  │
                                  ▼
                       analyze_benchmark.py ──► benchmark_{summary,*.png,*.pdf}
```

---

## 4. Komponens-szerepkörök röviden

- **Tréning-adatkészítés**: a HEFT és HEFT-DS ütemezők döntéseit logoljuk a 200
  workflow-n → ~210\,000 ütemezési döntés CSV-formátumban.
- **GNN-tréning**: imitációs tanulás reward-súlyozással (1/makespan) +
  class-súlyozás (kiegyensúlyozott osztály-eloszlás).
- **ONNX-export**: a betanított PyTorch-modell ONNX-be exportálva,
  dinamikus axes-szel (változó méretű DAG és klaszter).
- **Java-integráció**: az `onnxruntime` Java-bindingjén keresztül futtatjuk,
  a `GnnScheduler` az `AdaptiveHeftScheduler`-ből származik, így bizonytalan
  döntéseknél (softmax < 0.3) természetes fallback van.
- **Benchmark + elemzés**: a `SchedulerBenchmarkRunner` 5 ütemezőt × 10 workflow
  × 3 seed = 150 szimulációt futtat, az `analyze_benchmark.py` aggregál és
  grafikonokat épít.
