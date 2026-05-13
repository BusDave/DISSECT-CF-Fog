# Workflow Grammar Generator

## Gyors attekintes

A `workflow_grammar.py` formalis grafnyelvtan alapjan general workflow DAG-okat a DISSECT-CF-Fog szimulatorhoz. Kozvetlenul XML-t allit elo -- WfCommons es JSON kozbulso lepes nelkul.

**Fajl helye:** `src/main/resources/script/workflow_grammar.py`

## Telepites

Nincs szukseg kulso fuggosegre -- kizarolag Python 3 standard konyvtarakat hasznal (`argparse`, `random`, `xml.etree.ElementTree`, `enum`, `abc`).

## Hasznalat

### CLI

```bash
# Pipeline: SOURCE -> COMPUTE x N -> SINK
python3 workflow_grammar.py --pattern pipeline --depth 5 --output out.xml

# Fan-out: SOURCE -> ROUTE -> (COMPUTE x depth) x fan_out -> AGGREGATOR -> SINK
python3 workflow_grammar.py --pattern fan-out --fan-out 4 --depth 2 --output out.xml

# Filter-map: SOURCE -> FILTER -> ROUTE -> (COMPUTE -> FILTER) x fan_out -> AGGREGATOR -> SINK
python3 workflow_grammar.py --pattern filter-map --fan-out 3 --depth 2 --output out.xml

# Fork-join: SOURCE x N -> (COMPUTE x depth) x N -> AGGREGATOR -> SINK
python3 workflow_grammar.py --pattern fork-join --sources 8 --depth 2 --output out.xml

# Stencil: SOURCE -> mesh(width x depth) -> AGGREGATOR -> SINK
python3 workflow_grammar.py --pattern stencil --width 5 --depth 4 --output out.xml

# Chained-fan-out: SOURCE -> (ROUTE -> branches -> AGG) x stages -> SINK
python3 workflow_grammar.py --pattern chained-fan-out --fan-out 3 --stages 3 --depth 2 --output out.xml

# Broadcast: SOURCE -> full-data-copy x fan_out -> AGGREGATOR -> SINK
python3 workflow_grammar.py --pattern broadcast --fan-out 4 --depth 2 --output out.xml

# Reprodukalhato kimenet (fix random seed)
python3 workflow_grammar.py --pattern fan-out --fan-out 4 --depth 2 --seed 42 --output out.xml

# Segitseg
python3 workflow_grammar.py --help
```

### CLI parameterek

| Parameter | Rovid | Kotelezo | Alapertek | Leiras |
|-----------|-------|----------|-----------|--------|
| `--pattern` | `-p` | igen | - | Mintazat: `pipeline`, `fan-out`, `filter-map`, `fork-join`, `stencil`, `chained-fan-out`, `broadcast` |
| `--fan-out` | `-f` | nem | 4 | Parhuzamos agak szama |
| `--depth` | `-d` | nem | 2 | COMPUTE lepesek szama agankent / stencil szintek |
| `--sources` | - | nem | 4 | Fuggetlen forras node-ok szama (fork-join) |
| `--width` | - | nem | 4 | Racs szelessege (stencil) |
| `--stages` | - | nem | 2 | Fan-out fazisok szama (chained-fan-out) |
| `--output` | `-o` | nem | `<pattern>-...xml` | Kimeneti XML fajl neve (mindig a `WORKFLOW_examples/` mappaba kerul; csak a basename szamit) |
| `--name` | `-n` | nem | automatikus | Workflow neve az XML-ben |
| `--seed` | - | nem | - | Random seed reprodukalhatosaghoz |

### Python API

A szkript programozottan is hasznalhato, nem csak CLI-bol:

```python
from workflow_grammar import *

grammar = WorkflowGrammar("my-workflow")
grammar.chain(
    PipelinePattern([NodeType.SOURCE, NodeType.COMPUTE]),
    FanOutPattern(fan_out=4, branch=PipelinePattern([NodeType.COMPUTE, NodeType.FILTER])),
    PipelinePattern([NodeType.AGGREGATOR, NodeType.SINK])
)
grammar.build_xml("output.xml")
```

### Hasznalat a szimulatorban

```java
import hu.u_szeged.inf.fog.simulator.util.xml.WorkflowJobModel;

String workflowFile = ScenarioBase.resourcePath + "/WORKFLOW_examples/out.xml";
Pair<String, ArrayList<WorkflowJob>> jobs = WorkflowJobModel.loadWorkflowXml(workflowFile, "0");
executor.submitJobs(new MaxMinScheduler(nodes, instance, null, jobs));
```

## Node tipusok

| Tipus | Runtime | Output size | Szerepe |
|-------|---------|-------------|---------|
| `SOURCE` | random 1-5s | log-uniform 1 KB - 100 MB | Adatot termel, a DAG kezdopontja |
| `COMPUTE` | random 10-60s | input x uniform(0.5, 1.2), atlag ~0.85 | Szamitasigenyes feldolgozas |
| `FILTER` | random 1-10s | input x uniform(0.2, 0.7), atlag ~0.45 | Szures, csokkenti az adatmennyiseget |
| `ROUTE` | random 0.5-2s | input / fan_out | Szetosztja az adatot az agak kozott |
| `AGGREGATOR` | random 5-30s | max(inputs) x uniform(0.3, 0.9), atlag ~0.6 | Osszegyujti a parhuzamos agak kimeneteit |
| `SINK` | random 0.5-2s | 0 | Vegpont, nem kuld tovabb semmit |

> **Edge size cap:** minden `output_size` ertek `1 GiB`-re van vagva (`_MAX_EDGE_SIZE`), hogy ne csorduljon at a Java `long`-on a mely fan-in lancokon keresztul.

A node tipusok **kizarolag a generalasnal** szamitanak -- a runtime es size ertekek kiszamitasahoz. A Java szimulator mar csak a generalt `runtime`, `size` es `amount` ertekeket latja az XML-bol.

## Parameterek hatasa mintazatonkent

Minden mintazat eltero parameterekre reagal. Az alabbi tablazat osszefoglalja, hogy melyik flag melyik mintazatnal aktiv es hogyan befolyasolja a workflow meretet/alakjat.

| Mintazat | Releváns flagek | COMPUTE-ok szama | Teljes node szam | Gyokerek |
|----------|-----------------|------------------|------------------|----------|
| `pipeline` | `--depth` | `depth` | `depth + 2` | 1 |
| `fan-out` | `--fan-out`, `--depth` | `fan_out × depth` | `fan_out × depth + 4` | 1 |
| `filter-map` | `--fan-out`, `--depth` | `fan_out × depth` (+ ugyanannyi FILTER) | `2 × fan_out × depth + 5` | 1 |
| `fork-join` | `--sources`, `--depth` | `sources × depth` | `sources × (1 + depth) + 2` | `sources` |
| `stencil` | `--width`, `--depth` | `width × depth` | `width × depth + 3` | 1 |
| `chained-fan-out` | `--fan-out`, `--stages`, `--depth` | `stages × fan_out × depth` | `stages × (fan_out × depth + 2) + 2` | 1 |
| `broadcast` | `--fan-out`, `--depth` | `fan_out × depth` | `fan_out × depth + 3` | 1 |

> A "4 fix" (SOURCE + ROUTE + AGG + SINK) a `fan-out` esetén; `fork-join`-nál nincs ROUTE de van `sources` darab SOURCE; `stencil`-nél nincs ROUTE de van GatherPattern.

### Mit jelentenek a kozos flagek?

#### `--fan-out N` — a parhuzamos agak szama (szelesseg)
- **Hol aktiv:** `fan-out`, `filter-map`, `broadcast`, `chained-fan-out`
- **Mit befolyasol:** hany parhuzamos ag indul a ROUTE-bol (vagy SOURCE-bol broadcast eseten)
- **Szemantika:** magasabb ertek = nagyobb parhuzamossag, tobb scheduling-dontes
- **HEFT-re hatas:** nagy fan-out -> sok parallel task egyszerre -> a node-valasztas kritikus

#### `--depth N` — a felhasznali kontextusban valtoz a jelentese
- **`pipeline`-ban:** hany COMPUTE van sorban (kritikus ut hossza)
- **`fan-out`/`broadcast`-ben:** egy agon belul hany COMPUTE
- **`filter-map`-ben:** hany `(COMPUTE -> FILTER)` par van egy agon
- **`fork-join`-ban:** hany COMPUTE van minden source-agan
- **`stencil`-ben:** a racs **szintek szama** (vertikalis dimenzio)
- **`chained-fan-out`-ban:** hany COMPUTE egy stage-en belul egy agon
- **Szemantika:** magasabb ertek = hosszabb kritikus ut, hosszabb makespan
- **HEFT-re hatas:** mely DAG-ban a critical path azonositasa kritikus

#### `--sources N` — fuggetlen forras node-ok szama (csak `fork-join`)
- **Mit befolyasol:** hany gyokere lesz a DAG-nak
- **Szemantika:** mindegyik SOURCE-bol indul egy fuggetlen ag, kozos AGGREGATOR-ba erkeznek
- **HEFT-re hatas:** a parhuzamossag inditasnal maximalis, az AGGREGATOR az utolso szuk keresztmetszet

#### `--width N` — racs szelessege (csak `stencil`)
- **Mit befolyasol:** a stencil mintazatban a vizszintes pozici k szama
- **Szemantika:** minden szint `width` darab node-ot tartalmaz, a node `(i, j)` az `(i-1, j-1)`, `(i-1, j)` es `(i-1, j+1)` szuloktol fugg
- **HEFT-re hatas:** szomszed-fuggosegek -> nem-trivialis adat-lokalitasi mintazat

#### `--stages N` — fan-out-merge fazisok szama (csak `chained-fan-out`)
- **Mit befolyasol:** hany egymast koveto fan-out -> aggregate ciklus van
- **Szemantika:** minden stage egy teljes FanOut + AGG, az egyik AGG-ja a kovetkezo ROUTE inputja
- **HEFT-re hatas:** ismetlodo szuk keresztmetszetek -> minden stage AGG-ja sorrendi pont

### Konkret peldak

```bash
# Csak depth:
python3 workflow_grammar.py --pattern pipeline --depth 10
#   = 12 node (SOURCE + 10×COMPUTE + SINK)

# fan-out + depth:
python3 workflow_grammar.py --pattern fan-out --fan-out 4 --depth 5
#   = 24 node (SOURCE + ROUTE + 4×5=20 COMPUTE + AGG + SINK)

# sources + depth (fork-join):
python3 workflow_grammar.py --pattern fork-join --sources 8 --depth 3
#   = 8×(1+3)+2 = 34 node, 8 gyokerrel

# width + depth (stencil):
python3 workflow_grammar.py --pattern stencil --width 5 --depth 4
#   = 5×4+3 = 23 node, racs-mintaval

# stages + fan-out + depth (chained-fan-out):
python3 workflow_grammar.py --pattern chained-fan-out --stages 3 --fan-out 4 --depth 2
#   = 3 stage × (4×2+2) + 2 = 32 node, 3 egymast koveto fan-out fazissal
```

### Praktikus iranyok

| Cel | Pattern | Parameterek | Eredmeny |
|------|---------|-------------|----------|
| Maximalis parhuzamossag | `fan-out` | `--fan-out 16 --depth 1` | 16 fuggetlen task egy szinten |
| Hosszu szekvencialis lanc | `pipeline` | `--depth 20` | 20 task egymas utan |
| Sok fuggetlen indulas | `fork-join` | `--sources 16 --depth 5` | 16 parhuzamos ag, AGG-ben talalkoznak |
| Aggregator szuk kereszt | `fork-join` | `--sources 32 --depth 1` | minden a vegen vararakozik az AGG-ra |
| Realisztikus tudomanyos szimulacio | `fan-out` vagy `chained-fan-out` | `--fan-out 4 --depth 5` vagy `--stages 3` | atlagos meret, mixed parhuzamossag |
| Stencil-szeru tudomanyos kod | `stencil` | `--width 6 --depth 5` | racs-mintazatu szomszed-fuggosegek |

## Eloregyartott mintazatok

### Pipeline

```
SOURCE -> COMPUTE -> COMPUTE -> ... -> COMPUTE -> SINK
```

Egyszeru linearis lanc. A `--depth` parameter hatarozza meg a COMPUTE lepesek szamat.

### Fan-out

```
SOURCE -> ROUTE -+-> COMPUTE -> COMPUTE -+-> AGGREGATOR -> SINK
                 +-> COMPUTE -> COMPUTE -+
                 +-> COMPUTE -> COMPUTE -+
                 +-> COMPUTE -> COMPUTE -+
```

A ROUTE szetelosztja az adatot `fan_out` parhuzamos agra, mindegyik ag `depth` darab COMPUTE lepest tartalmaz, vegul az AGGREGATOR osszegyujti az osszes ag kimenetet.

### Filter-map

```
SOURCE -> FILTER -> ROUTE -+-> COMPUTE -> FILTER -> COMPUTE -> FILTER -+-> AGGREGATOR -> SINK
                           +-> COMPUTE -> FILTER -> COMPUTE -> FILTER -+
                           +-> COMPUTE -> FILTER -> COMPUTE -> FILTER -+
```

Hasonlo a fan-out-hoz, de a SOURCE utan van egy FILTER lepes, es minden agon COMPUTE->FILTER parok ismetlodnek `depth`-szer.

### Fork-join (map-reduce)

```
SOURCE_1 -> COMPUTE -> COMPUTE -+
SOURCE_2 -> COMPUTE -> COMPUTE -+-> AGGREGATOR -> SINK
SOURCE_3 -> COMPUTE -> COMPUTE -+
SOURCE_4 -> COMPUTE -> COMPUTE -+
```

Tobb fuggetlen forras parhuzamosan dolgozik, vegul egyetlen AGGREGATOR gyujti ossze az eredmenyeket. A CyberShake-szeru workflow-k mintaja.

### Stencil (racs/mesh)

```
SOURCE -> A1   A2   A3   A4
          |\\  |\\  |\\  |
          | \\ | \\ | \\ |
          B1   B2   B3   B4
          |\\  |\\  |\\  |  -> AGGREGATOR -> SINK
          | \\ | \\ | \\ |
          C1   C2   C3   C4
```

Racsszeru struktura, ahol minden node a felette levo szomszedaitol fugg (j-1, j, j+1 pozicio). A Montage workflow-kra jellemzo minta. A `--width` a racs szelessege, a `--depth` a szintek szama.

### Chained-fan-out (iterativ map-reduce)

```
SOURCE -> ROUTE -+-> branch -+-> AGG -> ROUTE -+-> branch -+-> AGG -> SINK
                 +-> branch -+                 +-> branch -+
                 +-> branch -+                 +-> branch -+
```

Ismetlodo fan-out -> merge fazisok sorozata. Minden fazis egy teljes FanOut mintazat. A `--stages` a fazisok szama. LIGO-szeru workflow-k mintaja.

### Broadcast

```
SOURCE -> branch_1 (teljes adat) -+
       -> branch_2 (teljes adat) -+-> AGGREGATOR -> SINK
       -> branch_3 (teljes adat) -+
       -> branch_4 (teljes adat) -+
```

Hasonlo a fan-out-hoz, de **nincs ROUTE** -- minden ag a **teljes** adatot kapja (nem osztja el). Jellemzo peldak: replikacio, tobb csatornaju feldolgozas, validacios parhuzamossag.

## Egyedi mintazatok

### Sajat Pattern irasa

Uj mintazatot a `Pattern` absztrakt osztaly oroklese es a `build()` metodus implementalasa keszitheto:

```python
class DiamondPattern(Pattern):
    """A -> B,C parhuzamosan -> D (gyemant alak)"""

    def build(self, dag, entry_nodes):
        entries = entry_nodes if entry_nodes else [None]
        exits = []

        for entry in entries:
            input_size = entry.output_size if entry else 0

            a = dag.add_node(Node(NodeType.COMPUTE, input_size=input_size))
            if entry:
                dag.add_edge(entry, a)

            b = dag.add_node(Node(NodeType.COMPUTE, input_size=a.output_size))
            c = dag.add_node(Node(NodeType.FILTER, input_size=a.output_size))
            dag.add_edge(a, b)
            dag.add_edge(a, c)

            d = dag.add_node(Node(NodeType.AGGREGATOR,
                             input_size=b.output_size + c.output_size))
            dag.add_edge(b, d)
            dag.add_edge(c, d)

            exits.append(d)
        return exits
```

### Mintazatok kombinalaasa

A `chain()` lehetove teszi a mintazatok szekvencialis lancba fuzeset:

```python
grammar = WorkflowGrammar("complex")
grammar.chain(
    PipelinePattern([NodeType.SOURCE]),
    FanOutPattern(fan_out=3, branch=PipelinePattern([NodeType.COMPUTE, NodeType.FILTER])),
    FanOutPattern(fan_out=2, branch=PipelinePattern([NodeType.COMPUTE])),
    PipelinePattern([NodeType.SINK]),
)
```

Es egymasba agyazast is:

```python
# Fan-out-on belul ujabb fan-out
grammar.chain(
    PipelinePattern([NodeType.SOURCE]),
    FanOutPattern(fan_out=3, branch=
        FanOutPattern(fan_out=2, branch=
            PipelinePattern([NodeType.COMPUTE])
        )
    ),
    PipelinePattern([NodeType.SINK]),
)
```

## XML formatum

A generalt XML megegyezik a `wfformat_to_dissect_converter.py` kimenetevel:

```xml
<adag name="my-workflow" repeat="1">
  <job id="source_00000001" runtime="2.71">
    <uses link="input" type="compute" amount="0"/>
    <uses link="output" id="compute_00000002" type="data" size="52428800"/>
  </job>
  <job id="compute_00000002" runtime="34.21">
    <uses link="input" type="compute" amount="1"/>
    <uses link="output" id="filter_00000003" type="data" size="44564480"/>
  </job>
  <job id="aggregator_00000010" runtime="12.50">
    <uses link="input" type="compute" amount="4"/>
    <uses link="output" id="sink_00000011" type="data" size="20971520"/>
  </job>
</adag>
```

Fontos szabalyok:
- **`amount`** = a node szuloinek szama (in-degree). A szimulator visszaszamlalokent hasznalja: minden parent befejezese `amount--`, es csak `amount == 0`-nal utemezi a jobot.
- **Root node** (nincs szuloje): `amount="0"` -- azonnal futhat.
- **AGGREGATOR** (tobb szulovel): `amount="N"` -- megvarja az osszes ag befejezodeseset.

## Node ID sema

`{node_type_kisbetus}_{szamlalo:08d}` -- pl. `compute_00000003`, `route_00000007`

Globalis szamlalo, minden `build()` hivasnal nullarol indul.

## Architektura

```
WorkflowGrammar                -- orkesztrator, chain() lancba fuzi a mintazatokat
  +-- Pattern (ABC)            -- absztrakt alap, build(dag, entries) -> exits
  |     +-- PipelinePattern        -- linearis lanc (A -> B -> C)
  |     +-- FanOutPattern          -- ROUTE -> agak -> AGGREGATOR
  |     +-- ParallelPattern        -- fuggetlen agak egymas mellett
  |     +-- GatherPattern          -- tobb ag -> 1 AGGREGATOR (tiszta fan-in)
  |     +-- MultiSourcePattern     -- N fuggetlen SOURCE -> agak
  |     +-- ForkJoinPattern        -- N SOURCE -> agak -> 1 AGGREGATOR
  |     +-- BroadcastPattern       -- teljes adat N agra -> AGGREGATOR
  |     +-- StencilPattern         -- racs/mesh szomszed-fuggosegekkel
  |     +-- ChainedFanOutPattern   -- ismetelt fan-out -> merge fazisok
  +-- WorkflowDAG              -- csucsok + elek tarolasa, to_xml() generalas
        +-- Node               -- egy csucs: id, tipus, runtime, output_size
```

## Ellenorzes

```bash
# 1. Generalas
python3 workflow_grammar.py --pattern fan-out --fan-out 4 --depth 2 --output test.xml --seed 42

# 2. XML betoltese a szimulatorban (WorkflowSimulation.java)

# 3. DAG vizualizacio (opcionalis)
python3 DAG.py test.xml .
```

## Kapcsolodo fajlok

| Fajl | Leiras |
|------|--------|
| `workflow_grammar.py` | Ez a szkript |
| `wfformat_to_dissect_converter.py` | WfCommons JSON -> DISSECT-CF-Fog XML konverter |
| `generate_workflows.py` | WfCommons workflow generator wrapper |
| `DAG.py` | Workflow DAG vizualizacio |
| `WorkflowJobModel.java` | Java XML parser a szimulatorban |

## Osszehasonlitas: Grammar vs. WfCommons

| | Workflow Grammar | WfCommons |
|---|---|---|
| **Fuggoseg** | nincs (csak Python 3) | `pip install wfcommons` |
| **Kimenet** | kozvetlenul XML | JSON -> XML konverzio kell |
| **Mintazatok** | sajat, epitokokkakent | valos tudomanyos workflow-k masolata |
| **Rugalmassag** | tetszoleges DAG struktura | fix recipe-k (blast, montage, stb.) |
| **Hasznalat** | egyedi/szintetikus tesztekhez | realisztikus szimulaciohoz |

---

**Verzio:** 2.0
**Letrehozva:** 2025-02-24
**Frissitve:** 2026-03-31
**Eszkozok:** Python 3
