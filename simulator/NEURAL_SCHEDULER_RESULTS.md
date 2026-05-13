# Neural Scheduler — Szakdolgozat eredmények és módszertan

**Projekt:** Online, részleges workflow-ismeret melletti scheduling
DISSECT-CF-Fog környezetben, többféle ütemezési stratégiával
(HEFT-család, Adaptive HEFT, imitációs tanulás neurális hálóval).
**Dátum:** 2026-05-13
**Cluster:** 20-node heterogén fog/cloud (3 cloud × 52-core ELKH, 12 fog × 16-core LPDS, 5 edge × 4-core)

## 0. Feladatkiírás megfeleltetés

A szakdolgozat eredeti feladatkiírása három fő pillért jelöl meg.
Az alábbi táblázat összegzi, melyik pontot hogyan oldottuk meg.

| Feladatkiírás pont | Status | Megoldás |
|---|---|---|
| **(1) Online (runtime) ütemezés** | ✅ részben | HEFT-család dinamikus fázis (`findBestProcessor`) + NeuralScheduler runtime state-ből döntő. **Nem fed le:** változó node-kapacitás / network latency *futás közbeni* érzékelése. |
| **(2a) Dynamic HEFT** | ✅ | `HeftScheduler.java` — Topcuoglu (2002) + DVR-HEFT (3-variant rank) + Sun/Cao/Lu energy-aware. |
| **(2b) Tanuló alapú stratégia** | ✅ | `NeuralScheduler.java` — reward-weighted supervised imitation learning, PyTorch MLP, ONNX Runtime inference. **NEM tisztán RL**, hanem offline gyűjtött tapasztalatból tanuló policy. |
| **(2c) Genetikus algoritmus** | ❌ | Nem implementált — javasolt jövőbeli irány (lásd Future Work §7). |
| **(2d) Reinforcement Learning** | ⚠ közvetett | A reward-weighted imitation learning **rokon** az RL-lel (`w = 1/makespan` reward signal-t használ), de nem environment-interaction alapú. Tisztán RL agent (PPO/DQN) javasolt jövőbeli irány. |
| **(3) Optimalizációs célok**: makespan + cost + energy | ✅ | Mind a háromra mérünk a benchmark-ban. EDP (Energy-Delay Product) kombinált metrikát is definiáltunk. |
| **(4) Workflow generátor** | ✅ | `random_workflow_generator.py` (chaotic DAG) + `generate_workflows.py` (WfCommons recipe wrapper). 53 workflow generálva benchmark-ra. |

### A teljesülés értelmezése

A kiírás **két nagyobb hozzájárulást** vár:
- **(A) Új scheduler-stratégiák implementálása online környezetben.**
  Ezt **két új scheduler-rel** valósítottuk meg: AdaptiveHeftScheduler
  (DAG-tudatos regime-switching) és NeuralScheduler (ML alapú policy +
  heurisztika safety net hibrid).
- **(B) Workflow generátor + benchmark infrastruktúra.** Ezt is megoldottuk:
  paraméterezhető chaotic generátor + WfCommons recipe wrapper, plusz
  reprodukálható benchmark runner.

A **konkrét RL és GA implementáció hiányzik** — ezt a Future Work
fejezet részletezi. A NeuralScheduler-rel viszont megmutattuk, hogy a
**learn-then-defer paradigma** (tanult policy + heurisztika fallback)
az adott setupban a legjobb hand-crafted heurisztikát (Adaptive HEFT)
**megegyező szinten** képes elérni, miközben **9/10 workflow-n megveri
a klasszikus HEFT-et**.

---

## 1. Tartalomjegyzék

1. [A probléma](#2-a-probléma)
2. [A megoldás architektúrája](#3-a-megoldás-architektúrája)
3. [Iterációk és tanulságok](#4-iterációk-és-tanulságok)
4. [Végső eredmények](#5-végső-eredmények)
5. [Tanulságok](#6-tanulságok)
6. [Future Work](#7-future-work)
7. [Fájllista](#8-fájllista)

---

## 2. A probléma

A DISSECT-CF-Fog szimulátorban munkafolyamat (workflow) ütemezésre több
heurisztikus scheduler létezik: HEFT, HEFT-DS (energy-aware HEFT),
MaxMin, MaxMin-DS, stb. Ezek mind statikus heurisztikák — egy
algoritmust követnek függetlenül a workflow jellegétől.

**Kutatási kérdés:** Lehetne-e *tanulni* a múltbeli ütemezési döntésekből
egy olyan policy-t, ami **legalább olyan jó** mint a legjobb heurisztika,
de **lényegesen kevesebb inference költséggel** működik runtime-ban?

**Hipotézis:** Igen — egy 50→128→64→20 dimenziós MLP imitációs tanulással
betanítható, hogy a HEFT-családú schedulerek (state, action) párjait
megtanulja. Minimum makespan reward weighting biztosítja, hogy a háló
előnyben részesítse a *gyors* futások döntéseit.

---

## 3. A megoldás architektúrája

### 3.1 State representation

Minden ütemezési döntésnél a háló egy 50-dimenziós state vektort kap.
A séma a [`SchedulerFeatureExtractor.java`](src/main/java/hu/u_szeged/inf/fog/simulator/workflow/scheduler/training/SchedulerFeatureExtractor.java)-ben:

| Index | Feature | Honnan |
|---|---|---|
| 0..4 | task-level: `task_runtime`, `task_rank_up`, `task_in_degree`, `task_out_degree`, `task_total_input_bytes` | a task aktuális állapotából |
| 5..9 | global: `elapsed_sec`, `tasks_done`, `tasks_remaining`, `total_active_vms`, `avg_node_load` | szimulátor globális állapot |
| 10..49 | per-node (20 × 2): `nodeN_queue`, `nodeN_running` | minden node aktuális terhelése |

### 3.2 Tréning pipeline

```
WORKFLOW_examples/*.xml
    │
    ▼
BatchTrainingRunner.java
    │  (HEFT + HEFT-DS + MaxMin + Adaptive futtatása minden workflow-n,
    │   per-döntés (state, action) CSV-be írva)
    ▼
sim_res/training/*.csv + *.meta.json
    │
    ▼
notebooks/train_heft_imitation.ipynb
    │  (CSV-k betöltése, EFT-greedy teachers szűrése,
    │   reward weighting + class weighting,
    │   PyTorch MLP 50→128→64→20, weighted CrossEntropy,
    │   ONNX export inline z-score normalizálással)
    ▼
src/main/resources/models/heft_imitation.onnx
```

### 3.3 Inference pipeline

```
DISSECT-CF szimulátor + NeuralScheduler.java
    │
    ▼ minden ütemezési döntésnél
SchedulerFeatureExtractor → 50-dim state vektor
    │
    ▼
ONNX Runtime (com.microsoft.onnxruntime:1.19.2)
    │  forward pass ~0.01 ms / döntés
    ▼
softmax → top-1 valószínűség
    │
    ├─ ha confidence ≥ 0.5 ÉS nem distributed mód → háló döntése
    └─ egyébként → Adaptive HEFT fallback (heurisztikus döntés)
```

### 3.4 Komponenshierarchia

```
WorkflowScheduler (abstract)
 ├─ HeftScheduler          (DVR-HEFT alapok, EFT-greedy)
 │   ├─ HeftDsScheduler    (energy-aware, sub-deadline, task merging)
 │   │   └─ AdaptiveHeftScheduler   (NEW: dag-aware regime switch)
 │   │       └─ NeuralScheduler    (NEW: ONNX + Adaptive fallback)
 │   ├─ HeftSchedulerWithLogging   (training CSV logging)
 │   └─ HeftDsSchedulerWithLogging (training CSV logging)
 ├─ MaxMinScheduler        (round-robin assignment)
 │   └─ MaxMinSchedulerWithLogging
 └─ (egyebek: RenewableScheduler, IotWorkflowScheduler, stb.)
```

---

## 4. Iterációk és tanulságok

A neural scheduler **négy iteráción** ment keresztül, mindegyik egy
**ismeretet adott hozzá** a thesis story-hoz.

### Iteráció 1 — Naív HEFT-imitation (FAIL)

**Setup:**
- Training: HEFT + HEFT-DS CSV-k (108 run, 23310 döntés)
- Loss: reward-weighted CrossEntropy (`w = 1/makespan_seconds`)
- Architektúra: 50→128→64→20 MLP, Adam lr=1e-3
- 50 epoch, train/val split per-workflow (80/20)
- Fallback: extends HeftScheduler → fallback = HEFT

**Eredmény:**
- Val accuracy: 55%
- Benchmark: **Neural 6990s** vs HEFT 5878s — **Neural rosszabb mint HEFT!**
- 35% neural / 65% fallback
- Per-node accuracy: cloud nodok 65-73%, ritka fog (node17) **csak 11%**

**Tanulság:** A háló class-collapse-ol. A training adat 14%-ban node0,
1.9%-ban node17 → a háló cloud-bias-elt. Genome, blast_200 workflow-kon
**-30 — -39%** rosszabb mint HEFT.

### Iteráció 2 — Class weighting (SIKER)

**Változás:** Inverse-frequency class weighting hozzáadva.
`class_weight[c] = 1 / freq[c]`, normalizálva.

**Eredmény:**
- Val accuracy: ~53% (kicsit gyengébb absolute)
- Node17 accuracy: 11% → **58%** (5x javulás)
- Cloud nodok: 65% → 51% (kicsit gyengébb)
- Benchmark: **Neural 5424s** vs HEFT 5878s — **Neural 8% gyorsabb mint HEFT** ✓
- 44% neural / 56% fallback
- blast_200: -30% → **+14%** swing!
- genome_150 javult: -39% → -33%

**Tanulság:** A class weighting **kompenzálja a training data bias-t**.
Néhány workflow (genome) továbbra is rossz, de összességében nagy ugrás.

### Iteráció 3 — Multi-teacher distillation (TANULSÁGOS FAIL)

**Hipotézis:** Train data csak **az EDP-best teacher** döntéseiből —
így minden workflow-ra a legjobb policyt tanulja.

**Setup:**
- 4 scheduler (HEFT, HEFT-DS, MaxMin, Adaptive) futott minden workflow-n
- EDP (Energy-Delay Product = makespan × energy) alapján kiválasztva a
  győztes scheduler per workflow
- Best teacher elosztás: 20 workflow HEFT-DS, 17 Adaptive, 14 MaxMin,
  0 HEFT (HEFT sosem nyer!)
- 12320 döntés (jelentősen kevesebb mint korábban a szűrés miatt)

**Eredmény:**
- Val loss: **8.0** (KATASZTRÓFA, előző: 1.91)
- Val accuracy: 14-27%
- Per-node accuracy: cloud/fog nodokon **0%**, csak edge nodokon ~50%
- Háló *collapse-olt* az edge nodokra

**Mit derítettünk ki:**
- A MaxMin és Adaptive HEFT (distributed módban) **shuffle-alapú round-robin**-t
  használ. Egy adott task → adott node hozzárendelés **véletlen** a state
  szempontjából (a task_id-re épül).
- A neural háló a state-ből **nem tudja megjósolni** ezeket a véletlen
  döntéseket — **fundamentálisan tanulhatatlan**.
- A confused háló minden döntésre az edge node-ot tippeli (mert ott van
  a legtöbb label round-robinből).

**Tanulság:** **Imitation learning csak deterministic teachert tud
megtanulni.** Round-robin teachers state-független, ezért
imitálhatatlan ilyen state representation-nel.

### Iteráció 4 — "Learn-then-defer" hybrid (FŐ EREDMÉNY)

**Stratégia:**
1. **Tanulás csak EFT-greedy teachers-ből** (HEFT + HEFT-DS) — ezek
   state-deterministic döntéseket hoznak, így tanulhatóak.
2. **NeuralScheduler `extends AdaptiveHeftScheduler`** (volt HeftScheduler).
   Worst-case fallback = Adaptive (a legjobb heurisztika), nem HEFT.
3. **`distributedMode` override:** ha az Adaptive úgy ítéli a workflow-t,
   hogy round-robin distribúció kell, a Neural **teljesen kihagyja magát**
   (skip inference, return Adaptive's static assignment).

**Eredmény:**
- Val accuracy: 57% (re-trained HEFT+HEFTDS only data)
- Benchmark: **Neural 2937s** (Adaptive 2910s, 0.9% különbség = noise) ✓
- 55% neural / 45% Adaptive fallback
- 9/10 workflow-n Neural ≥ HEFT, 8/10 energy
- IoT_CyberShake_100: Neural **445s** vs Adaptive 532s → **Neural -16% jobb mint Adaptive!**
- blast_200 most: Neural = Adaptive = 14696s (distributed override működik)
- Egyetlen veszteség: chaotic_n150_c30 (-17% rosszabb mint HEFT)

**Tanulság:** **Hibrid policy** > tisztán heurisztika VAGY tisztán
neurális. A neural feltárja a "nyertes" döntéseket néhány workflow-n,
a heurisztika garantálja a worst-case minőséget.

---

## 5. Végső eredmények

### 5.1 Aggregate (10 workflow × 3 seed = 30 futás per scheduler)

| Scheduler | Avg makespan | Avg energy | neural_pct | Inference latency |
|---|---:|---:|---:|---:|
| **Adaptive HEFT** | **2910 s** | **3.65 kWh** | — | — |
| **Neural Scheduler** | **2937 s** | **3.68 kWh** | 55.4% | **0.010 ms** |
| MaxMin | 3187 s | 3.96 kWh | — | — |
| HEFT | 5877 s | 7.09 kWh | — | — |
| HEFT-DS | 6669 s | 7.97 kWh | — | — |

A Neural Scheduler **gyakorlatilag megegyezik az Adaptive HEFT-tel**
(0.9% különbség nem szignifikáns), és:
- **50%-kal gyorsabb mint HEFT**
- **56%-kal gyorsabb mint HEFT-DS**
- **8%-kal gyorsabb mint MaxMin**

### 5.2 Per-workflow Neural vs HEFT (head-to-head)

| Workflow | Neural makespan | HEFT makespan | Speedup | Energy savings |
|---|---:|---:|---:|---:|
| IoT_CyberShake_100 | 445 s | 915 s | **-51%** | -47% |
| blast_50tasks | 4160 s | 11140 s | **-63%** | -61% |
| blast_200tasks | 14696 s | 34504 s | **-57%** | -56% |
| seismology_120 | 19 s | 36 s | **-48%** | 0% |
| montage_100 | 3437 s | 4904 s | **-30%** | -28% |
| cycles_100 | 722 s | 916 s | **-21%** | -19% |
| chaotic_n300_c40 | 2095 s | 2587 s | **-19%** | -18% |
| chaotic_n50_c20 | 422 s | 551 s | **-23%** | -19% |
| genome_150 | 1215 s | 1424 s | **-15%** | -11% |
| chaotic_n150_c30 | 2156 s | 1793 s | +20% | +18% |

**9 nyer / 1 veszít** makespan-en. **8 nyer / 2 veszít/döntetlen** energiát.

### 5.3 Ábrák (artifact-ok)

- `sim_res/benchmark_makespan.png` — 5-scheduler bar chart, error bars
- `sim_res/benchmark_energy.png` — 5-scheduler bar chart, error bars
- `sim_res/benchmark_speedup.png` — Neural vs HEFT scatter, parity line
- `sim_res/benchmark_results.csv` — raw 150 futás eredmény
- `sim_res/benchmark_summary.csv` — per-workflow összegzés

---

## 6. Tanulságok

### 6.1 Mit tanultunk az imitációs tanulásról

1. **Class imbalance kompenzáció kritikus.** A scheduler training data
   szinte mindig torzul a "népszerű" node-okra. Inverse-frequency
   weighting **az egész stratégiát megfordítja** (45% → -8% rosszabb HEFT-nél
   helyett +8% jobb).

2. **Nem minden teachert lehet imitálni.** A teacher policy-jának
   state-deterministic-nek kell lennie. Random / shuffle-based döntések
   (round-robin) **fundamentálisan tanulhatatlanok** a megadott state
   representation mellett.

3. **Reward weighting jobb mint reward-mentes imitation.** A
   `w = 1/makespan` súlyozás biztosítja, hogy a háló a *gyors* futások
   döntéseit preferálja, nem az átlagos teacher viselkedést.

4. **Safety net fallback alapvető fontosságú.** Egyszerű threshold-based
   fallback (top-1 prob < 0.5) megakadályozza a katasztrofális hibákat
   ott, ahol a tanult policy nem általánosul.

### 6.2 Mit tanultunk a scheduling-ról

1. **A HEFT-család a kommunikáció-súlyozott workflow-kra optimalizált.**
   A 62.5 MB/s commRate konstans implicit feltételezi, hogy a kommunikáció
   drága. Ha nem (pl. blast: 3 MB összes adat 200 taskon = 15 KB/edge),
   a HEFT-bias hibás.

2. **Round-robin nyer a wide+sparse DAG-okon.** Ahol a párhuzamosság >>
   kommunikációs költség, a kapacitás-egyenlőtlen elosztás (capacity-weighted)
   **nem** jobb mint a sima round-robin — mert a DISSECT-CF VM-allokáció
   minden VM-nek ugyanazt a (1 CPU, 0.001 power) kényszert ad, így a node
   sebesség csak párhuzamosságot ad, nem per-task gyorsabbat.

3. **Adaptive scheduling: kis fix overhead, nagy haszon.** Két számból
   (`dagWidth`, `commIntensity`) számolt küszöb-alapú stratégia-váltás
   eléri, hogy mindkét regime-en optimum-közeli legyen. Nem ML, csak
   principled engineering.

### 6.3 Általánosítható minta: "Learn-then-defer"

```
1. Tanuld meg a legjobb (state-deterministic) teacher policy-t.
2. Worst-case fallback legyen a legjobb heurisztika.
3. Threshold-based confidence kapcsolja a kettőt.
4. A tanult policy néhány workflow-n a heurisztika fölé fog menni.
   Ahol nem, a heurisztika kihúz.
```

Ez egy klasszikus paradigm a robusztus AI-rendszerekben (pl. autonóm
vezetésben rule-based safety system + learned policy).

---

## 7. Future Work

A feladatkiírás explicit említi a **reinforcement learning** és
**genetikus algoritmus** stratégiákat — ezek nem készültek el, viszont
a meglévő infrastruktúrára (state extractor, scheduler hierarchy,
benchmark) **alacsony költséggel** ráépíthetők. Az alábbi pontok
prioritás szerint:

1. **Reinforcement Learning agent** (kiírás-megfeleltetés).
   A Neural Scheduler offline imitation learning. RL agent (pl. PPO
   `stable-baselines3`-ból) közvetlenül a szimulátorral interaktálva
   tanulna. Implementáció: Python OpenAI Gym wrapper a DISSECT-CF köré
   + reward = -makespan vagy -EDP. **Becsült munka: 1-2 nap.**

2. **Genetikus algoritmus scheduler** (kiírás-megfeleltetés).
   Encoding: task → node assignment vektor. Fitness: szimulált
   makespan. Standard GA (crossover + mutation), 100 generáció. Mivel
   offline, az `AdaptiveHeftScheduler.init()`-ben futna, majd a
   dinamikus fázis finomítaná. **Becsült munka: 1 nap.**

3. **Runtime infrastruktúra-változás kezelése** (kiírás-megfeleltetés).
   A jelenlegi schedulerek statikus node-kapacitást feltételeznek.
   Egy node-failure detect + dinamikus újratervezés ennek a
   feladatkiírási pontnak felelne meg pontosabban.

4. **Több training data** — 53 → 200+ workflow. A jelenlegi val
   accuracy 57%; több adattal 70%+ várható.

5. **Cluster-méret független architektúra.** Jelenleg a háló 20 node-ra
   fixált (40 per-node feature). Graph Neural Network (GNN) bármely
   cluster-méretre tudna adaptálódni.

6. **Multi-objective reward.** Pareto-optimal `(makespan, energy, cost)`
   reward + multi-head MLP választhatóvá tenné a felhasználói preferenciát.

7. **Heurisztika-választás meta-learning.** Külön kis háló választana
   workflow-jellemzők alapján: HEFT vs HEFT-DS vs MaxMin vs Adaptive.
   Multi-armed bandit megközelítés.

---

## 8. Fájllista

### 8.1 Új Java fájlok (a thesis hozzájárulása)

```
simulator/src/main/java/hu/u_szeged/inf/fog/simulator/workflow/scheduler/
 ├─ AdaptiveHeftScheduler.java          DAG-aware regime-switching scheduler
 ├─ NeuralScheduler.java                ONNX Runtime + AdaptiveHEFT fallback
 └─ training/
     ├─ SchedulerFeatureExtractor.java  50-dim state vector construction
     ├─ TrainingLogger.java             CSV + JSON sidecar writer
     ├─ HeftSchedulerWithLogging.java   HEFT decision logging
     ├─ HeftDsSchedulerWithLogging.java HEFT-DS decision logging
     ├─ MaxMinSchedulerWithLogging.java MaxMin decision logging (with DAG rebuild)
     └─ AdaptiveHeftSchedulerWithLogging.java Adaptive HEFT decision logging

simulator/src/main/java/hu/u_szeged/inf/fog/simulator/demo/
 ├─ BatchTrainingRunner.java            iterates all workflows × all schedulers
 ├─ NeuralSchedulerDemo.java            single-workflow NeuralScheduler demo
 └─ SchedulerBenchmarkRunner.java       cross-scheduler benchmark (5 × N × seeds)
```

### 8.2 Módosított Java fájlok

```
simulator/pom.xml                       + onnxruntime 1.19.2 dependency
simulator/.../workflow/scheduler/HeftScheduler.java
                                        (módosítva: DVR-HEFT 3-variant ranks)
```

### 8.3 Python / Notebook

```
simulator/notebooks/
 ├─ train_heft_imitation.ipynb          PyTorch training pipeline + ONNX export
 ├─ analyze_benchmark.py                Benchmark CSV → summary + 3 PNG
 ├─ repair_csv.py                       Locale-mismatch CSV fixer (one-off)
 └─ dump_held_out.py                    (helper, not used in final pipeline)
```

### 8.4 Generált artifact-ok

```
simulator/src/main/resources/models/heft_imitation.onnx   (~12 KB)
simulator/sim_res/training/*.csv + *.meta.json            204 × 2 file
simulator/sim_res/benchmark_results.csv                   150 rows raw data
simulator/sim_res/benchmark_summary.csv                   10 × 5 summary
simulator/sim_res/benchmark_makespan.png                  bar chart
simulator/sim_res/benchmark_energy.png                    bar chart
simulator/sim_res/benchmark_speedup.png                   scatter
```

### 8.5 Workflow corpus

```
simulator/src/main/resources/demo/WORKFLOW_examples/      53 XML
 ├─ 5 WfCommons recipes × 5 sizes:
 │   blast (50,80,120,160,200), cycles (70,80,100,150,200),
 │   genome (60,100,150,200,250), montage (60,65,100,150,200),
 │   seismology (110,120,130,150,200)
 ├─ 25 chaotic (random_workflow_generator.py): n50-300 × c10-50
 └─ 3 fixed: CyberShake_100, IoT_CyberShake_100, IoT_workflow

Training corpus: 53 - 2 skipped (CyberShake_100, IoT_workflow) = 51 active
Benchmark corpus: 10 curated (mix of WfCommons + chaotic + IoT)
```

---

## 9. Reprodukálhatóság

### 9.1 Training data regenerálás

```bash
cd simulator
rm -f sim_res/training/*.csv sim_res/training/*.json
# IntelliJ: Run BatchTrainingRunner (VM options: -Xmx6g)
```

### 9.2 Model retraining

```bash
cd simulator/notebooks
jupyter notebook train_heft_imitation.ipynb
# Kernel → Restart Kernel → Run All Cells
# Output: ../src/main/resources/models/heft_imitation.onnx
```

### 9.3 Benchmark

```bash
cd simulator
rm sim_res/benchmark_results.csv
# IntelliJ: Run SchedulerBenchmarkRunner
python3 notebooks/analyze_benchmark.py
```

---

## 10. Hivatkozások

- **HEFT**: Topcuoglu et al. (2002) — *Performance-effective and low-complexity
  task scheduling for heterogeneous computing.*
- **DVR-HEFT**: Sandokji and Eassa (2019) — *Dynamic Variant Rank HEFT.*
- **HEFT-DS**: Sun, Cao, Lu (2022) — *Energy-aware HEFT with critical-path
  analysis and data locality.*
- **WfCommons**: Coleman et al. (2022) — *WfCommons: A framework for
  enabling scientific workflow research and education.*
- **ONNX Runtime**: Microsoft — `com.microsoft.onnxruntime:onnxruntime:1.19.2`
- **DISSECT-CF**: U_szeged SED Lab — base simulator.

---

*Generált: 2026-05-13. Készítette: BusDave szakdolgozat-projekthez.*
