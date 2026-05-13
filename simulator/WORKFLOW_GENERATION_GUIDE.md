# Workflow Generation Guide

## Gyors attekintes

Ez az utmutato megmutatja, hogyan generalj szintetikus workflow-kat a DISSECT-CF-Fog szimulatorhoz. Ket megkozelites erheto el:

| Megkozelites | Szkript | Fuggoseg | Mikor hasznald |
|---|---|---|---|
| **WfCommons** | `generate_workflows.py` + `wfformat_to_dissect_converter.py` | `pip install wfcommons` | Realisztikus tudomanyos workflow-k (blast, montage, stb.) |
| **Workflow Grammar** | `workflow_grammar.py` | nincs (csak Python 3) | Egyedi/szintetikus mintazatok (pipeline, fan-out, fork-join, stencil, stb.) |

## 1. Workflow Grammar (WfCommons nelkul)

Formalis grafnyelvtan alapjan general workflow-kat, kozvetlenul DISSECT-CF-Fog XML-t allit elo.

> Reszletes dokumentacio: [WORKFLOW_GRAMMAR_README.md](WORKFLOW_GRAMMAR_README.md)

### Elerheto mintazatok

| Mintazat | Struktura | Parancs |
|----------|-----------|---------|
| `pipeline` | SOURCE -> COMPUTE x N -> SINK | `--pattern pipeline --depth 5` |
| `fan-out` | SOURCE -> ROUTE -> agak -> AGG -> SINK | `--pattern fan-out --fan-out 4 --depth 2` |
| `filter-map` | SOURCE -> FILTER -> ROUTE -> agak -> AGG -> SINK | `--pattern filter-map --fan-out 3 --depth 2` |
| `fork-join` | SOURCE x N -> agak -> AGG -> SINK | `--pattern fork-join --sources 8 --depth 2` |
| `stencil` | SOURCE -> racs(width x depth) -> AGG -> SINK | `--pattern stencil --width 5 --depth 4` |
| `chained-fan-out` | SOURCE -> (ROUTE -> agak -> AGG) x stages -> SINK | `--pattern chained-fan-out --stages 3 --fan-out 3` |
| `broadcast` | SOURCE -> teljes adat x N -> AGG -> SINK | `--pattern broadcast --fan-out 4 --depth 2` |

### Peldak

```bash
# Egyszeru pipeline
python3 workflow_grammar.py --pattern pipeline --depth 5 --output out.xml

# Fan-out mintazat
python3 workflow_grammar.py --pattern fan-out --fan-out 4 --depth 2 --output out.xml

# Filter-map mintazat
python3 workflow_grammar.py --pattern filter-map --fan-out 3 --depth 2 --output out.xml

# Fork-join (CyberShake-szeru, tobb forras -> kozos aggregator)
python3 workflow_grammar.py --pattern fork-join --sources 8 --depth 2 --output out.xml

# Stencil (Montage-szeru racsminta)
python3 workflow_grammar.py --pattern stencil --width 5 --depth 4 --output out.xml

# Chained-fan-out (LIGO-szeru iterativ map-reduce)
python3 workflow_grammar.py --pattern chained-fan-out --fan-out 3 --stages 3 --depth 2 --output out.xml

# Broadcast (teljes adat masolat minden agra)
python3 workflow_grammar.py --pattern broadcast --fan-out 4 --depth 2 --output out.xml

# Reprodukalhato kimenet
python3 workflow_grammar.py --pattern fan-out --fan-out 8 --depth 3 --seed 42 --output out.xml
```

### Python API

```python
from workflow_grammar import *

grammar = WorkflowGrammar("custom-workflow")
grammar.chain(
    PipelinePattern([NodeType.SOURCE, NodeType.COMPUTE]),
    FanOutPattern(fan_out=4, branch=PipelinePattern([NodeType.COMPUTE, NodeType.FILTER])),
    PipelinePattern([NodeType.AGGREGATOR, NodeType.SINK])
)
grammar.build_xml("output.xml")
```

## 2. WfCommons (realisztikus workflow-k)

A WfCommons konyvtarral valos tudomanyos workflow-kat generalhatsz, amiket a konverter DISSECT-CF-Fog XML-re alakit.

> Reszletes dokumentacio: [WFCOMMONS_CONVERTER_README.md](WFCOMMONS_CONVERTER_README.md)

### Elerheto workflow tipusok

| # | Tipus | Struktura | Min. task | Leiras |
|---|-------|-----------|-----------|--------|
| 1 | `blast` | Fan-out/Fan-in | 45 | Protein/DNA szekvencia osszehasonlitas |
| 2 | `bwa` | MapReduce-like | 30 | DNS szekvencia mapping |
| 3 | `cycles` | Mixed | 20 | Mezogazdasagi novenynovekedesi szimulacio |
| 4 | `epigenomics` | Pipeline | 20 | DNS metilaacios analizis |
| 5 | `genome` | Complex DAG | 50 | 1000 Genomes projekt analizis |
| 6 | `montage` | Pipeline | 30 | Asztronomiaia kepmozaik generalas |
| 7 | `seismology` | Fan-out/Fan-in | 103 | Foldrenges analizis (CyberShake) |
| 8 | `soykb` | Pipeline | 25 | Szojabab genomikai adatfeldolgozas |
| 9 | `srasearch` | Pipeline | 20 | NCBI szekvencia kereses |

### Peldak

```bash
# Workflow generalas (csak JSON)
python3 generate_workflows.py --type blast --tasks 100

# Generalas + XML konverzio (ajanlott)
python3 generate_workflows.py --type blast --tasks 100 --convert

# Elerheto tipusok listazasa
python3 generate_workflows.py --list

# Kulonallo konverzio
python3 wfformat_to_dissect_converter.py my_workflow.json output.xml
```

### Parameterek

#### generate_workflows.py

| Parameter | Rovid | Kotelezo | Default | Leiras |
|-----------|-------|----------|---------|--------|
| `--type` | `-t` | igen | - | Workflow tipus (lasd tabla fent) |
| `--tasks` | `-n` | igen | - | Task-ok szama |
| `--convert` | `-c` | nem | off | Automatikus XML konverzio |
| `--output` | `-o` | nem | auto | Custom JSON fajlnev (csak basename, mappa rogzitett) |
| `--xml-output` | - | nem | auto | Custom XML fajlnev (csak `--convert`-tel) |
| `--list` | `-l` | nem | off | Tipusok listazasa |
| `--count` | - | nem | 1 | Hany fuggetlen instance keszuljon (`_1`, `_2`... suffix) |
| `--runtime-factor` | - | nem | 1.0 | Task runtime-ok skalazasa (pl. 1.5 = +50%) |
| `--input-size-factor` | - | nem | 1.0 | Input fajlmeretek skalazasa |
| `--output-size-factor` | - | nem | 1.0 | Output fajlmeretek skalazasa |
| `--workflow-name` | - | nem | recipe default | A JSON-ba beagyazott workflow nev (csak `--count=1` eseten) |

### Pelda: tanito halmaz HEFT-imitation-hoz

```bash
# 50 fuggetlen Blast instance, ugyanaz a recipe, kulonbozo seedek
python3 generate_workflows.py --type blast --tasks 100 --count 50 --convert
```

### Pelda: skalazasi sensitivity teszt

```bash
# Ugyanaz a workflow, harom kulonbozo intenzitassal
for rf in 0.5 1.0 2.0; do
    python3 generate_workflows.py --type genome --tasks 100 \
        --runtime-factor $rf --output genome_rf${rf}.json --convert
done
```

## 3. Kimeneti mappak

Mindharom XML-generator (`workflow_grammar.py`, `random_workflow_generator.py`, `wfformat_to_dissect_converter.py`) **automatikusan** a kozos `src/main/resources/demo/WORKFLOW_examples/` mappaba ir. A `--output` parameter csak a fajl basename-jet veszi figyelembe, a mappa rogzitett.

A WfCommons koztes JSON workflow-k kulonalloan a `src/main/resources/demo/WfCommons_JSON/` mappaba kerulnek (a `generate_workflows.py` automatikusan letrehozza, ha nem letezik).

| Generator | Kimenet | Cel mappa |
|-----------|---------|-----------|
| `workflow_grammar.py` | `.xml` | `WORKFLOW_examples/` |
| `random_workflow_generator.py` | `.xml` | `WORKFLOW_examples/` |
| `generate_workflows.py` | `.json` (koztes) | `WfCommons_JSON/` |
| `wfformat_to_dissect_converter.py` | `.xml` (vegleges) | `WORKFLOW_examples/` |

## 4. Hasznalat a szimulatorban

Mindket megkozelites ugyanazt az XML formatumot allitja elo, igy a Java oldal azonos:

```java
import hu.u_szeged.inf.fog.simulator.util.xml.WorkflowJobModel;

// Workflow betoltese
String workflowFile = ScenarioBase.resourcePath + "/WORKFLOW_examples/out.xml";
Pair<String, ArrayList<WorkflowJob>> jobs = WorkflowJobModel.loadWorkflowXml(workflowFile, "0");

// Hasznalat a szimulacioban
executor.submitJobs(new MaxMinScheduler(nodes, instance, null, jobs));
```

## 5. DAG vizualizacio

Barmelyik megkozelitessel generalt XML vizualizalhato:

```bash
python3 DAG.py out.xml .
```

## 6. Tippek

### Workflow meretek

- **Kis workflow** (20-50 task): Gyors teszt
- **Kozepes workflow** (100-200 task): Realisztikus szimulacio
- **Nagy workflow** (500+ task): Skalazhatosagi tesztek

### Melyik megkozelitest valasszam?

- **Szintetikus teszt** (sajat struktura, gyors, fuggoseg nelkul) -> `workflow_grammar.py` (7 mintazat)
- **Veletlen komplex workflow** (nagy, vegyes struktura) -> `random_workflow_generator.py`
- **Realisztikus workflow** (valos tudomanyos alkalmazasok mintaja) -> `generate_workflows.py`

### Batch generalas

```bash
# Workflow Grammar - kulonbozo mintazatok
for pattern in pipeline fan-out filter-map fork-join stencil chained-fan-out broadcast; do
    python3 workflow_grammar.py --pattern $pattern --output ${pattern}.xml --seed 42
done

# Workflow Grammar - kulonbozo fan-out meretek
for f in 2 4 8 16; do
    python3 workflow_grammar.py --pattern fan-out --fan-out $f --depth 3 --output fanout_${f}.xml --seed 42
done

# Random workflow generator - tobb veletlen workflow
python3 random_workflow_generator.py --nodes 200 --count 5 --seed 42 --output random.xml

# WfCommons
for type in blast montage epigenomics; do
    python3 generate_workflows.py --type $type --tasks 100 --convert
done
```

## 7. Hibaelharitas

### "Cannot create synthetic graph with N nodes" (WfCommons)

A megadott task szam kisebb mint a recipe minimuma. Noveld a `--tasks` erteket.

### A szimulacio nem fejezodik be (Workflow Grammar)

Ellenorizd az `amount` ertekeket az XML-ben. Az AGGREGATOR node `amount` ertekenek meg kell egyeznie a szulok szamaval (pl. `amount="4"` ha 4 ag van). A `workflow_grammar.py` ezt automatikusan kezeli.

### "ImportError: cannot import name 'XYZRecipe'" (WfCommons)

A WfCommons nincs telepitve vagy regi verzio. Megoldas: `pip install --upgrade wfcommons`

## 8. Kapcsolodo fajlok

| Fajl | Leiras |
|------|--------|
| `workflow_grammar.py` | Grammatika-alapu workflow generator (7 mintazat) |
| `random_workflow_generator.py` | Veletlen komplex workflow generator |
| `generate_workflows.py` | WfCommons workflow generator wrapper |
| `wfformat_to_dissect_converter.py` | WfCommons JSON -> XML konverter |
| `DAG.py` | Workflow DAG vizualizacio |
| `WorkflowJobModel.java` | Java XML parser |
| `WorkflowSimulation.java` | Pelda szimulacio workflow-kkal |

---

**Verzio:** 1.2
**Letrehozva:** 2025-12-15
**Frissitve:** 2026-03-31
**Eszkozok:** Python 3, WfCommons 1.3 (opcionalis)
