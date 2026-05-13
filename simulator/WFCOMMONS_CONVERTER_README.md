# WfCommons to DISSECT-CF-Fog Konverter

## Gyors attekintes

A `wfformat_to_dissect_converter.py` a [WfCommons](https://wfcommons.org) WfFormat JSON workflow-kat alakitja at DISSECT-CF-Fog XML formattumba, lehetove teve valos tudomanyos workflow-k hasznalatat fog/IoT szimulaciokban.

**Fajl helye:** `src/main/resources/script/wfformat_to_dissect_converter.py`

> **Megjegyzes:** Ha nem WfCommons-bol, hanem sajat mintazatokbol szeretnel workflow-t generalni, lasd a [Workflow Grammar Generator](WORKFLOW_GRAMMAR_README.md) dokumentaciot.

## Miert WfCommons?

A **WfCommons** konyvtar az alabbiakat biztositja:
- **WfChef**: Automatizalt workflow recipe generalas
- **52%-kal realisztikusabb** mint a regi WorkflowGenerator
- **Aktivan karbantartott** (2025)
- Szeles valasztek tudomanyos workflow tipusokbol (Blast, Montage, Epigenomics, CyberShake, stb.)

## Telepites

```bash
# WfCommons Python csomag
pip install wfcommons

# Graphviz (opcionalis, vizualizaciohoz)
brew install graphviz  # macOS
# vagy
apt-get install graphviz  # Linux
```

## Hasznalat

### 1. WfFormat workflow generalasa

```python
from wfcommons import WorkflowGenerator, BlastRecipe

# Recipe es generator letrehozasa
recipe = BlastRecipe.from_num_tasks(num_tasks=100)
generator = WorkflowGenerator(recipe)

# Workflow epites es mentes
workflow = generator.build_workflow()
workflow.write_json("my_blast_workflow.json")
```

Vagy a `generate_workflows.py` wrapper szkripttel:

```bash
python3 generate_workflows.py --type blast --tasks 100
```

> **Kimeneti mappak:**
> - JSON workflow-k -> `src/main/resources/demo/WfCommons_JSON/`
> - DISSECT-CF-Fog XML-ek -> `src/main/resources/demo/WORKFLOW_examples/`
>
> Mindketto auto-letrejon, es `--output` / `--xml-output` eseten is csak a basename-t hasznaljuk, a mappat eroszakkal feluljuk.

### 2. Konverzio DISSECT-CF-Fog XML-re

```bash
python3 wfformat_to_dissect_converter.py my_blast_workflow.json output_workflow.xml
```

Vagy egyben (generalas + konverzio):

```bash
python3 generate_workflows.py --type blast --tasks 100 --convert
```

A `from_num_tasks` skalazasi parameterei (runtime + I/O meret) es a tobbszoros instance-generalas CLI-rol elerheto:

```bash
# 50 fuggetlen Blast peldany (HEFT-imitation tanitashoz)
python3 generate_workflows.py --type blast --tasks 100 --count 50 --convert

# Lassabb runtime + nagyobb input fajlok stressz-tesztre
python3 generate_workflows.py --type genome --tasks 200 \
    --runtime-factor 1.5 --input-size-factor 2.0 --convert
```

#### Mit jelentenek a `*-factor` parameterek?

A `--runtime-factor`, `--input-size-factor` es `--output-size-factor` mindegyike **lineáris szorzótényező** a recipe statisztikai eloszlásából húzott értékekre. Nem változtatják meg a DAG strukturáját, sem a task-ok számát — csak a numerikus súlyokat skálázzák.

| Parameter | Mit szoroz meg | Pelda (factor=1.5) |
|-----------|----------------|---------------------|
| `--runtime-factor` | minden task `runtimeInSeconds` | 1200s -> 1800s |
| `--input-size-factor` | minden bemeneti fajl `sizeInBytes` | 50 MB -> 75 MB |
| `--output-size-factor` | minden kimeneti fajl `sizeInBytes` | 50 MB -> 75 MB |

A skalazas **uniform**: a leglassabb task is es a leggyorsabb task is ugyanazzal a faktorral szorzodik. Ezert hasznalhato pl. CPU-intenziv vs. IO-intenziv szcenariok modellezesere.

#### Mitol lesznek a `--count N` peldanyok unique-ok?

A WfCommons recipe-ek alatt egy **statisztikai modell** lakik (valos workflow-traces-ekbol tanitva). Minden `build_workflow()` hivasnal:
- a DAG topologiaja (task-ok szama, edge-ek, fajlok szama) **azonos marad** (kisebb workflow-knal)
- a runtime ertekek **uj random mintat** kapnak az eloszlasbol minden task-hoz
- a fajlmeretek szinten uj mintat kapnak

Pelda 3 darab 50-task-os Blast instance osszehasonlitasa:

| Mezo | Instance 1 | Instance 2 | Instance 3 |
|------|-----------|-----------|-----------|
| Task-ok szama | 48 | 48 | 48 |
| Edge-ek szama | 135 | 135 | 135 |
| Atlag runtime | 1143s | 1283s | 1265s |
| Elso task runtime | 0.316s | 1.814s | 0.691s |
| Osszes adatmeret | 176 GB | 140 GB | 147 GB |

Tehat: **azonos graph, kulonbozo sulyok**. Nagyobb workflow-knal (~250+ task) a wfchef tobb microstructure variansbol valaszt, igy a topologia is varialodik.

#### Mit ad a `--workflow-name`?

Tisztan metadata — a JSON `name` mezojebe ker. Default: `"<Type>-synthetic-instance"`. Ezzel csak megkulonbozteted a kimeneti fajlokat / log-okat. Sem a strukturat, sem a futasi idot nem befolyasolja. `--count > 1` eseten figyelmen kivul hagyjuk (az egyes instance-ek kulon-kulon kapnak nevet).

### 3. Hasznalat a szimulatorban

```java
import hu.u_szeged.inf.fog.simulator.util.xml.WorkflowJobModel;

String workflowFile = ScenarioBase.resourcePath + "/WORKFLOW_examples/output_workflow.xml";
Pair<String, ArrayList<WorkflowJob>> jobs = WorkflowJobModel.loadWorkflowXml(workflowFile, "0");
executor.submitJobs(new MaxMinScheduler(nodes, instance, null, jobs));
```

## Elerheto workflow tipusok

| Recipe | Leiras | Min. task | Struktura |
|--------|--------|-----------|-----------|
| **BlastRecipe** | Protein/DNA szekvencia osszehasonlitas | 45 | Fan-out/Fan-in |
| **BwaRecipe** | Burrows-Wheeler Aligner (DNS mapping) | 30 | MapReduce-like |
| **CyclesRecipe** | Mezogazdasagi novenynovekedesi modell | 20 | Mixed |
| **EpigenomicsRecipe** | DNS metilaaciios analizis | 20 | Pipeline |
| **GenomeRecipe** | 1000 Genomes Project analizis | 50 | Complex DAG |
| **MontageRecipe** | Asztronomiaia kepek mozaikja | 30 | Pipeline |
| **SeismologyRecipe** | Foldrenges (CyberShake) analizis | 30 | Fan-out/Fan-in |
| **SoykbRecipe** | Szojabab genomikai feldolgozas | 25 | Pipeline |
| **SrasearchRecipe** | NCBI SRA szekvencia kereses | 20 | Pipeline |

## Konverter reszletei

### WfFormat -> DISSECT-CF-Fog lekepezes

| WfFormat | DISSECT-CF-Fog | Megjegyzes |
|----------|----------------|------------|
| `workflow.name` | `<adag name="...">` | Workflow neve |
| `tasks[].id` | `<job id="...">` | Task azonosito |
| `tasks[].runtimeInSeconds` | `runtime="..."` | Futasi ido |
| `tasks[].parents[]` | `<uses link="input" amount="N">` | N = a szulok szama (in-degree) — a szimulator visszaszamlalokent hasznalja |
| `tasks[].children[]` | `<uses link="output" id="...">` | Adatfuggosegek |
| `files[].sizeInBytes` | `size="..."` | Adatmeret |

### Szintetikus adatok

Ha a WfFormat workflow nem tartalmaz futtatasi adatokat, a konverter szintetikus ertekeket general:
- **Runtime**: 10-60 masodperc kozott veletlenszeruen
- **Adatmeret**: Task-par alapu realisztikus meretek, vagy 1KB - 1MB kozotti fallback
- Determinisztikus (ugyanaz a seed -> ugyanaz az eredmeny)

### Adatmeret szamitas prioritasa

1. `specification.files` meretek a JSON-bol (valos WfCommons adatok)
2. Task-nev-par alapu meretek (`TASK_PAIR_SIZES` tabla)
3. Fallback: 1KB - 1MB kozotti veletlenszeru meret

## Tesztelt workflow-k

```
blast_100tasks_workflow_converted.xml       (98 task, fan-out/fan-in)
epigenomics_43tasks_workflow_converted.xml  (43 task, pipeline)
cycles_100tasks_workflow_converted.xml      (100 task, mixed)
```

## Kapcsolodo fajlok

| Fajl | Leiras |
|------|--------|
| `wfformat_to_dissect_converter.py` | Ez a konverter szkript |
| `generate_workflows.py` | WfCommons workflow generator wrapper |
| `workflow_grammar.py` | Grammatika-alapu workflow generator (WfCommons nelkul) |
| `DAG.py` | Workflow DAG vizualizacio |
| `WorkflowJobModel.java` | Java XML parser a szimulatorban |

## Hibaelharitas

**"Cannot create synthetic graph with N nodes"**
- A megadott task szam kisebb mint a recipe minimuma
- Megoldas: noveld a `num_tasks` parametert

**Hianyzo futtatasi adatok**
- Egyes WfFormat workflow-k nem tartalmaznak runtime/file size adatokat
- A konverter automatikusan szintetikus ertekeket general

**XML validacios hiba**
- Ellenorizd, hogy a generalt XML jol formalt-e
- Minden szukseges attributumnak jelen kell lennie (id, runtime, link, type)

## Referenciak

- WfCommons: https://wfcommons.org
- WfCommons GitHub: https://github.com/wfcommons
- Dokumentacio: https://docs.wfcommons.org

---

**Verzio:** 1.1
**Letrehozva:** 2025-12-15
**Frissitve:** 2025-02-24
