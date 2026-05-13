# Workflow rendszer - Reszletes jegyzet

## Tartalomjegyzek

1. [Mi az a workflow?](#1-mi-az-a-workflow)
2. [Workflow strukturak (mintazatok)](#2-workflow-strukturak-mintazatok)
3. [A DISSECT-CF-Fog XML formatuma](#3-a-dissect-cf-fog-xml-formatuma)
4. [WfCommons: realisztikus tudomanyos workflow-k](#4-wfcommons-realisztikus-tudomanyos-workflow-k)
5. [Workflow Grammar: szintetikus workflow generator](#5-workflow-grammar-szintetikus-workflow-generator)
6. [A konverzios folyamat (JSON -> XML)](#6-a-konverzios-folyamat-json---xml)
7. [Hogyan dolgozza fel a szimulator az XML-t?](#7-hogyan-dolgozza-fel-a-szimulator-az-xml-t)
8. [Meretek es idoegysegek](#8-meretek-es-idoegysegek)
9. [Osszefoglalas](#9-osszefoglalas)

---

## 1. Mi az a workflow?

Egy **workflow** (munkafolyamat) egymastol fuggo feladatok (task-ok/job-ok) halmaza, amelyek
egy iraanyitott, korforgasmentes grafot (DAG -- Directed Acyclic Graph) alkotnak. Minden job-nak
lehetnek szulei (akiktol adatot kap) es gyerekei (akiknek adatot kuld).

Pelda: egy kepfeldolgozo pipeline, ahol eloszor betoltjuk a kepet (SOURCE), utana szurjuk
(FILTER), feldolgozzuk (COMPUTE), vegul mentjuk (SINK). Minden lepes fugg az elozo
eredmenyetol.

A DISSECT-CF-Fog szimulatorban a workflow-kat XML fajlkent irjuk le, amiket a szimulator beolvas
es vegrehajt: a job-okat virtualis gepekre utemezi, az adatatetviteleket halozaton szimulaalja.

---

## 2. Workflow strukturak (mintazatok)

### 2.1 Pipeline (csovezetek)

```
A --> B --> C --> D --> E
```

A legegyszerubb struktura: a feladatok **sorban, egymasutan** hajtodnak vegre. Minden job
pontosan egy szulotol kap adatot es pontosan egy gyereknek kuld tovabb.

**Peldak a valosagban:**
- Adatfeldolgozo pipeline: beolvasas -> szures -> transzformacio -> mentes
- Epigenomics workflow: DNS minta -> szekvenaalas -> szures -> osszeillesztes -> analizis

**Jellemzoi:**
- Nincs parhuzamossag
- Egyszeru fuggosegi lanc
- A teljes futasi ido = az osszes job runtime-janak osszege + adatatetviteli idok

---

### 2.2 Fan-out / Fan-in (szetterules es osszegyujtes)

```
         +--> B1 --> C1 --+
         |                |
A -------+--> B2 --> C2 --+-------> D
         |                |
         +--> B3 --> C3 --+
```

**Fan-out:** egy job outputjat **tobb parhuzamos** agra osztjuk szedt. Az A job vegez,
es az eredmenyet elosztja B1, B2, B3 kozott.

**Fan-in:** a parhuzamos agak eredmenyeit egyetlen job **osszegyujti**. A D job megvarja
mindharom ag befejezeeset, majd egyutt dolgozza fel az osszes eredmenyt.

**Peldak a valosagban:**
- BLAST workflow: egy DNS szekvenciat szet darabolunk (split_fasta), parhuzamosan keresunk
  adatbazisban (blastall x N), vegul egyesitjuk az eredmenyeket (cat_blast)
- MapReduce: Map fazis (fan-out) -> Reduce fazis (fan-in)

**Jellemzoi:**
- Parhuzamos vegrehaajtas a kozepso agakon
- A fan-in pontban szinkronizacio tortenik: az aggregator megvarja AZ OSSZES aget
- Az agak szama a "fan-out faktor" (pl. 4, 8, 16)

---

### 2.3 Diamond (gyemant)

```
         +--> B --+
A -------|        |-------> D
         +--> C --+
```

A fan-out/fan-in egy specialis esete, ahol pontosan 2 ag van. A B es C parhuzamosan
futnak, D pedig mindkettore var.

---

### 2.4 Fork-join / Map-Reduce

```
SOURCE_1 --> B1 --> C1 --+
SOURCE_2 --> B2 --> C2 --+-------> D (AGGREGATOR)
SOURCE_3 --> B3 --> C3 --+
```

Tobb **fuggetlen forras** parhuzamosan indit munkaat, es egyetlen aggregator
gyujti ossze az eredmenyeket. Ez a CyberShake workflow mintaja: tobb szeizmikus
forrasmodell parhuzamos szintetizalasa, vegul kozos osszesites.

**Jellemzoi:**
- Tobb belepo pont (multi-source)
- Parhuzamos, fuggetlen feldolgozas
- Egyetlen osszegyujto pont (shared sink)

---

### 2.5 Stencil / Mesh (racsminta)

```
A1   A2   A3   A4
|\\  |\\  |\\  |
| \\ | \\ | \\ |
B1   B2   B3   B4
|\\  |\\  |\\  |
| \\ | \\ | \\ |
C1   C2   C3   C4
```

Racsszeru struktura, ahol minden node az elozo szint **szomszedaitol** fugg
(j-1, j, j+1 pozicio). A Montage workflow-kra jellemzo: egymas melletti
egdarabok feldolgozasa fugg a szomszedos darabok allapotatol.

**Jellemzoi:**
- Strukturalt parhuzamossag (szintenként)
- Lokalis fuggosegek (nem minden elozo node-tol fugg, csak a szomszedoktol)
- Tipikusan nagy node-szam: width x depth

---

### 2.6 Chained fan-out (iterativ map-reduce)

```
ROUTE -> agak -> AGG -> ROUTE -> agak -> AGG
```

Ismetlodo fan-out es merge fazisok sorozata. A LIGO gravitacios-hullamdetektalo
workflow-kra jellemzo: az adatfeldolgozas tobb fazisban tortenik, minden fazisban
ujra szetosztjak es osszegyujtik az adatokat.

---

### 2.7 Broadcast (teljes adat szoras)

```
         --> B1 (teljes adat) --+
A -----> --> B2 (teljes adat) --+-------> D (AGGREGATOR)
         --> B3 (teljes adat) --+
```

Hasonlo a fan-out-hoz, de **nem osztja el** az adatot -- minden ag a
**teljes adatkeszletet** kapja. Tipikus pelda: replikacio, tobb csatornaju
validacio, kulonbozo algoritmusokkal torteno parhuzamos feldolgozas.

**Kulonbseg a fan-out-tol:**
- Fan-out: ROUTE elosztja az adatot (output_size / fan_out)
- Broadcast: minden ag a teljes adatot kapja (output_size valtozatlan)

---

### 2.8 Mixed / Complex DAG

```
A --> B ----+--> D --> F
            |         ^
            +--> E ---+
C ----------+---------+
```

A valos tudomanyos workflow-k altalaban nem tisztaan egy mintazat, hanem ezek
**kombinacioja**. A Genome workflow peldaul pipeline, fan-out es fan-in szakaszokat
is tartalmaz kulonbozo pontokon.

---

## 3. A DISSECT-CF-Fog XML formatuma

A szimulator XML fajlokbol olvassa be a workflow-kat. Az alabbiak a formatum reszletei.

### 3.1 Altalanos struktura

```xml
<?xml version="1.0" ?>
<adag name="workflow-neve" repeat="1">
  <job id="job_azonosito" runtime="34.21">
    <uses link="input" type="compute" amount="1"/>
    <uses link="output" id="gyerek_job_id" type="data" size="52428800"/>
  </job>
  <!-- tovabbi job-ok -->
</adag>
```

### 3.2 Gyokerelem: `<adag>`

| Attributum | Leiras |
|---|---|
| `name` | A workflow neve (tetszoleges string) |
| `repeat` | Hanyszor kell lefuttatni a workflow-t (altalaban `"1"`) |

### 3.3 Job elem: `<job>`

Minden `<job>` egy feladatot (task-ot) jelol a DAG-ban.

| Attributum | Leiras | Pelda |
|---|---|---|
| `id` | Egyedi azonosito | `"compute_00000003"` |
| `runtime` | Futasi ido **masodpercben** | `"34.21"` |
| `long` | Foldrazi hosszusag (opcionalis, IoT) | `"1"` |
| `lat` | Foldrazi szelesseg (opcionalis, IoT) | `"2"` |

### 3.4 Uses elem: `<uses>`

Minden job-nak van legalabb egy `<uses>` eleme, ami a bemenetet vagy kimenetet irja le.

#### Input uses (bemenet)

```xml
<uses link="input" type="compute" amount="1"/>
```

| Attributum | Leiras |
|---|---|
| `link="input"` | Ez egy bemeneti elem |
| `type="compute"` | A job szamitast igenyel (COMPUTE tipusu bemenet) |
| `amount` | **Visszaszamlalo**: hany szulotol kell meg adatot kapnia. `"0"` = gyoker node (nincs szuloje, azonnal futhat). `"3"` = 3 szulo kell befejezzen mielott ez a job indulhat. |

**Fontos az `amount` megertese:**
- A szimulatorban ez egy **visszaszamlalo**
- Minden szulo befejezodik -> `amount--`
- Amikor `amount == 0` -> a job utemezodhet es futhat
- Tehat ha egy AGGREGATOR-nak 4 aga van, akkor `amount="4"` kell legyen
- Ha hibasan `amount="1"` van irva, a job mar az elso ag befejezese utan elindul
  es a tobbi ag adatait nem kapja meg -> a szimulacio "lefagy"

#### Output uses (kimenet)

```xml
<uses link="output" id="compute_00000005" type="data" size="52428800"/>
```

| Attributum | Leiras |
|---|---|
| `link="output"` | Ez egy kimeneti elem |
| `id` | A cel job azonositoja (akinek kuldi az adatot) |
| `type="data"` | Adat tipusu kimenet (a masik lehetoseg: `"actuate"`, IoT aktuatorokhoz) |
| `size` | Az atetvitt adatmennyiseg **byte-okban** |

Egy job-nak tobb output `<uses>` eleme is lehet -- mindegyik egy masik gyerekhez vezeto el.

### 3.5 Teljes pelda

```xml
<?xml version="1.0" ?>
<adag name="pelda-workflow" repeat="1">
  <!-- Root job: nincs szuloje, tehat amount=0 -->
  <job id="source_00000001" runtime="0.00">
    <uses link="input" type="compute" amount="0"/>
    <uses link="output" id="compute_00000002" type="data" size="10485760"/>
    <uses link="output" id="compute_00000003" type="data" size="10485760"/>
  </job>

  <!-- Compute job: 1 szuloje van (source), tehat amount=1 -->
  <job id="compute_00000002" runtime="34.21">
    <uses link="input" type="compute" amount="1"/>
    <uses link="output" id="aggregator_00000004" type="data" size="10485760"/>
  </job>

  <job id="compute_00000003" runtime="28.55">
    <uses link="input" type="compute" amount="1"/>
    <uses link="output" id="aggregator_00000004" type="data" size="10485760"/>
  </job>

  <!-- Aggregator job: 2 szuloje van (compute_2 es compute_3), tehat amount=2 -->
  <job id="aggregator_00000004" runtime="12.50">
    <uses link="input" type="compute" amount="2"/>
    <uses link="output" id="sink_00000005" type="data" size="20971520"/>
  </job>

  <!-- Sink job: 1 szuloje van, tehat amount=1. Nincs kimenete. -->
  <job id="sink_00000005" runtime="0.00">
    <uses link="input" type="compute" amount="1"/>
  </job>
</adag>
```

---

## 4. WfCommons: realisztikus tudomanyos workflow-k

### 4.1 Mi az a WfCommons?

A [WfCommons](https://wfcommons.org) egy Python konyvtar, amely valos tudomanyos alkalmazasok
workflow mintait tanulmanyozta, es ezek alapjan tud **szintetikus, de realisztikus** workflow-kat
generalni. A generalas un. "recipe"-k (receptek) alapjan tortenik.

### 4.2 Hogyan mukodik?

1. A WfCommons csapat valos szamitasi feladatokat tanulmanyozott (pl. BLAST DNS kereses,
   Montage asztrokepmozaik)
2. Ezekbol statisztikai modelleket keszitettek: milyen task tipusok vannak, hogyan fuggenek
   egymatol, mennyi ideig futnak, mekkora fajlokat generaalnak
3. A `Recipe` osztaly ezeket a modelleket tartalmazza
4. A `WorkflowGenerator` a recipe alapjan uj, szintetikus workflow-kat general, amelyek
   statisztikailag hasonlitanak a valos futasokra

### 4.3 Elerheto receptek

#### BLAST (BlastRecipe)

**Mit csinal a valosagban:** DNS vagy protein szekvenciakat hasonlit ossze adatbazisokkal.
Egy bemeneti szekvenciat szeetdarabol, parhuzamosan keres, majd osszegyujti az eredmenyeket.

**Workflow struktura:** Fan-out / Fan-in
```
split_fasta --> blastall (x N parhuzamosan) --> cat_blast
```

- `split_fasta`: szetdarabolja a bemeneti DNS fajlt
- `blastall`: minden darabot kulon-kulon osszehasonlit az adatbazissal (ez a leghosszabb)
- `cat_blast`: osszefuzi az eredmenyeket

**Jellemzo meretek:** runtime: 2-200 masodperc, fajlmeretek: par KB - par tiz KB

---

#### Epigenomics (EpigenomicsRecipe)

**Mit csinal a valosagban:** DNS metilaacios mintazatokat elemez. Az epigenetika vizsgalja,
hogyan valtozik a genexpresszio az alap DNS szekvencia modositasa nelkul.

**Workflow struktura:** Pipeline
```
fastqSplit --> filterContams --> sol2sanger --> fastqc --> mapMerge --> ...
```

- `fastqSplit`: nyers szekvenaalas-adatokat darabolja
- `filterContams`: szennyezodest szur ki
- `sol2sanger`: formatumot konvertal
- `mapMerge`: genommra illeszti a szekvenciaaakat

**Jellemzo meretek:** runtime: 5-150 masodperc, fajlmeretek: 100 byte - 50 KB

---

#### Montage (MontageRecipe)

**Mit csinal a valosagban:** Asztronomiaia felveetelekbol nagy felbontasu egmozaikot keszit.
Tobb teleszkop kepe kell osszeillessze egyetlen osszefuggo kepbe.

**Workflow struktura:** Pipeline (tobb fazisu)
```
mProjectPP --> mDiffFit --> mConcatFit --> mBgModel --> mBackground --> mImgTbl --> mAdd --> mShrink
```

**Jellemzo meretek:** runtime: 1-300 masodperc, fajlmeretek: valtozo

---

#### Cycles (CyclesRecipe)

**Mit csinal a valosagban:** Mezogazdasagi novenynovekedest es talajaaallapotot szimulal.
Idojaraasi adatok, talajosszetetel es novenyfiziologia alapjan josol termeest.

**Workflow struktura:** Mixed (pipeline + elagazasok)

---

#### Genome (GenomeRecipe)

**Mit csinal a valosagban:** Az 1000 Genomes Projekt reszeekent emberi genetikai variaciokat
elemez nagy mintaszamu populaaciokon.

**Workflow struktura:** Complex DAG (pipeline, fan-out, fan-in kevereke)

---

#### Seismology / CyberShake (SeismologyRecipe)

**Mit csinal a valosagban:** Foldrenges-veszelyeztetettseegi terkeepeket general. Szeizmogrammokat
szintetizal, majd csucseerteekeket szamol beloluk.

**Workflow struktura:** Fan-out / Fan-in
```
ExtractSGT --> SeismogramSynthesis (x N) --> PeakValCalcOkaya (x N) --> ZipSeis / ZipPSA
```

---

#### BWA (BwaRecipe)

**Mit csinal a valosagban:** DNS szekvenciakat illeszt referenciagenomra a
Burrows-Wheeler Aligner algoritmussal.

**Workflow struktura:** MapReduce-like

---

#### SoyKB (SoykbRecipe)

**Mit csinal a valosagban:** Szojabab genomikai adatokat dolgoz fel a Soybean Knowledge
Base projektben.

**Workflow struktura:** Pipeline

---

#### SRASearch (SrasearchRecipe)

**Mit csinal a valosagban:** Az NCBI Sequence Read Archive adatbazisban keres
szekvenciaakat.

**Workflow struktura:** Pipeline

---

### 4.4 A WfCommons JSON kimenet formaaatuma (WfFormat)

A WfCommons a workflow-kat JSON formatumban irja le. Fo mezeoi:

```json
{
  "name": "Blast-synthetic-instance",
  "workflow": {
    "specification": {
      "tasks": [
        {
          "id": "split_fasta_00000001",
          "name": "split_fasta",
          "parents": [],
          "children": ["blastall_00000002", "blastall_00000003"],
          "inputFiles": ["file_001"],
          "outputFiles": ["file_002", "file_003"]
        }
      ],
      "files": [
        {
          "id": "file_002",
          "sizeInBytes": 9408
        }
      ]
    },
    "execution": {
      "tasks": [
        {
          "id": "split_fasta_00000001",
          "runtimeInSeconds": 2.54
        }
      ]
    }
  }
}
```

| JSON mezo | Leiras |
|---|---|
| `tasks[].id` | Task egyedi azonositoja |
| `tasks[].name` | Task tipusa (pl. "blastall", "split_fasta") |
| `tasks[].parents` | Szulo task-ok ID-i |
| `tasks[].children` | Gyerek task-ok ID-i |
| `tasks[].inputFiles` | Bemeneti fajlok ID-i |
| `tasks[].outputFiles` | Kimeneti fajlok ID-i |
| `files[].sizeInBytes` | Fajlmeret **byte-okban** |
| `execution.tasks[].runtimeInSeconds` | Futasi ido **masodpercben** |

---

## 5. Workflow Grammar: szintetikus workflow generator

### 5.1 Mi ez?

A `workflow_grammar.py` egy altalunk irt script, amely **formalis grafnyelvtan** alapjan
general workflow DAG-okat. Nem hasznal WfCommons-t, nincs JSON kozbulso lepes -- kozvetlenul
a DISSECT-CF-Fog XML formatumot allitja elo.

### 5.2 Node tipusok

A grammar 6 absztrakt node tipust definiaal. Ezek **nem leteznek** a Java szimulatorban --
kizarolag a generator hasznalja oket arra, hogy realisztikus `runtime` es `size` ertekeket
rendeljen a job-okhoz.

#### SOURCE (forras)

- **Szerepe:** A workflow beleepo pontja. Adatot termel a "semmibol".
- **Runtime:** 0 masodperc (nem szamol, csak adatot ad)
- **Output size:** Random 1 MB -- 100 MB
- **Pelda valos analoogia:** Fajl beolvasas, szenzor adatgyujtes, adatbazis lekerdezes

#### COMPUTE (szamitas)

- **Szerepe:** Szamitasigenyes feldolgozo lepes.
- **Runtime:** Random 10--60 masodperc
- **Output size:** = input size (nem valtoztatja az adatmeretet, csak feldolgozza)
- **Pelda valos analooia:** Szekvenaiaillesztes (blastall), kepfeldolgozas, szimulaacioos lepes

#### FILTER (szuro)

- **Szerepe:** Kiszeuri az adatok egy reszet -- csak a relevansat engedi tovabb.
- **Runtime:** Random 1--10 masodperc (gyors muvelet)
- **Output size:** input size * 0.5 (az adat felet kiszuri)
- **Pelda valos analoogia:** Szennyezodesszeures (filterContams), adattisztitas

#### ROUTE (eloszto)

- **Szerepe:** Szetosztja az adatot tobb parhuzamos agra. Maga nem szamol.
- **Runtime:** 0 masodperc
- **Output size:** input size / fan_out (egyenlo elosztaas)
- **Pelda valos analoogia:** split_fasta a BLAST-ban, Map fazis elosztasa

#### AGGREGATOR (osszegyujto)

- **Szerepe:** Osszegyujti a parhuzamos agak eredmenyeit egyetlen kimenetbe.
- **Runtime:** Random 5--30 masodperc
- **Output size:** sum(input sizes) -- az osszes ag kimeneteenek osszege
- **Pelda valos analoogia:** cat_blast, Reduce fazis, eredmenyek egyesitese
- **Fontos:** az XML-ben `amount` = az agak szama (pl. 4 ag -> `amount="4"`)

#### SINK (nyelo)

- **Szerepe:** A workflow veegpontja. Megkapja a vegso eredmenyt, de nem kuld tovabb semmit.
- **Runtime:** 0 masodperc
- **Output size:** 0 (nincs kimenete)
- **Pelda valos analoogia:** Eredmeny mentese, vegso riport generalas

### 5.3 Pattern-ek (mintazatok)

A grammar epitokocka-szeru mintazatokbol (Pattern) epit workflow-kat.

#### PipelinePattern

Lineaaris lanc: minden lepes a kovetkezo bemenete.

```python
PipelinePattern([NodeType.SOURCE, NodeType.COMPUTE, NodeType.COMPUTE, NodeType.SINK])
# Eredmeny: SOURCE --> COMPUTE --> COMPUTE --> SINK
```

Ha tobb entry node-ot kap (pl. egy ROUTE-bol jovo tobb ag), akkor minden entry-hez
kulon pipeline peldanyt hoz letre.

#### FanOutPattern

Eloszto-osszegyujto minta: letrehoz egy ROUTE node-ot, N parhuzamos aget fut,
es AGGREGATOR-ral gyujti ossze.

```python
FanOutPattern(fan_out=3, branch=PipelinePattern([NodeType.COMPUTE, NodeType.FILTER]))
# Eredmeny: ROUTE --> 3x(COMPUTE --> FILTER) --> AGGREGATOR
```

#### ParallelPattern

Tobb fuggetlen ag egymas mellett, kozos ROUTE/AGGREGATOR nelkul.

```python
ParallelPattern([
    PipelinePattern([NodeType.COMPUTE]),
    PipelinePattern([NodeType.FILTER]),
])
# Eredmeny: ket fuggetlen ag egymas mellett
```

#### GatherPattern

Tiszta fan-in: tobb bemeneti node egyetlen AGGREGATOR-ba konvergal.

```python
# Hasznalatban: egy ParallelPattern utan lancoljuk
grammar.chain(
    ParallelPattern([branch_a, branch_b, branch_c]),
    GatherPattern(),  # az osszes branch exit -> 1 AGGREGATOR
)
```

#### MultiSourcePattern

Tobb fuggetlen SOURCE node, mindegyik egy aget taplal.

```python
MultiSourcePattern(count=4, branch=PipelinePattern([NodeType.COMPUTE, NodeType.FILTER]))
# Eredmeny: 4x(SOURCE --> COMPUTE --> FILTER)
```

#### ForkJoinPattern

Multi-source + agak + aggregator (map-reduce minta).

```python
ForkJoinPattern(source_count=4, branch=PipelinePattern([NodeType.COMPUTE]))
# Eredmeny: 4x(SOURCE --> COMPUTE) --> AGGREGATOR
```

#### BroadcastPattern

Teljes adatmasolat N agra (nem osztja el, mint a FanOut), vegul AGGREGATOR.

```python
BroadcastPattern(fan_out=3, branch=PipelinePattern([NodeType.COMPUTE, NodeType.FILTER]))
# Eredmeny: 3x(COMPUTE --> FILTER) [mindegyik teljes adatot kap] --> AGGREGATOR
```

#### StencilPattern

Racsszeru struktura szomszed-fuggosegekkel.

```python
StencilPattern(width=4, depth=3)
# Eredmeny: 4x3-as racs, minden node fugg a felette levo szomszedaitol
```

#### ChainedFanOutPattern

Ismetelt fan-out -> merge fazisok sorozata.

```python
ChainedFanOutPattern(stages=2, fan_out=3, branch=PipelinePattern([NodeType.COMPUTE]))
# Eredmeny: (ROUTE --> 3x COMPUTE --> AGG) --> (ROUTE --> 3x COMPUTE --> AGG)
```

### 5.4 Lancolas (chain)

A `WorkflowGrammar.chain()` szekvencialisan fuzi ossze a mintazatokat:
az elozo mintazat kimenete a kovetkezo bemenete.

```python
grammar = WorkflowGrammar("my-workflow")
grammar.chain(
    PipelinePattern([NodeType.SOURCE]),       # 1. SOURCE letrehozasa
    FanOutPattern(fan_out=4, branch=...),     # 2. SOURCE kimeneteehez csatlakozik
    PipelinePattern([NodeType.SINK]),          # 3. AGGREGATOR kimenetehez csatlakozik
)
```

---

## 6. A konverzios folyamat (JSON -> XML)

A `wfformat_to_dissect_converter.py` a WfCommons JSON-t alakitja DISSECT-CF-Fog XML-re.

### 6.1 Mi lesz mibol?

| WfCommons JSON | DISSECT-CF-Fog XML | Atalaakitas |
|---|---|---|
| `tasks[].id` | `<job id="...">` | Kozvetlen masolat |
| `tasks[].children[]` | `<uses link="output" id="...">` | Gyerek lista -> output elemek |
| `execution.tasks[].runtimeInSeconds` | `runtime="..."` | Masodpercben, 2 tizedes |
| `files[].sizeInBytes` | `size="..."` | Byte-okban, egesz szamkent |
| `tasks[].parents` letezes | `amount="0"` vagy `"1"` | Van szulo? -> 1, Nincs? -> 0 |

### 6.2 Runtime (futasi ido)

**Forraaas:** `execution.tasks[].runtimeInSeconds` -- ez masodpercben van.

**Atalaakitas:** Kozvetlenul atmaasoljuk, 2 tizedes pontossaggal: `runtime="34.21"`

**Ha nincs futasi adat:** A konverter szintetikus runtime-ot general:
`random.uniform(10.0, 60.0)` -- tehat 10 es 60 masodperc kozott.
A random seed a task ID hash-e, igy determinisztikus (ugyanaz a task -> ugyanaz az ertek).

**Hogyan hasznalja a szimulator?** A `WorkflowExecutor.java`-ban a runtime-ot
masodpercbol szamitasi muvelette (NOI - Number of Instructions) alakitja:
```java
noi = 1000 * workflowJob.runtime * cpuCount * processingPower;
```
A `1000`-es szorzo azert kell, mert a szimulator belso orajele **milliszekundumokban**
mukodik, a runtime viszont masodpercben van megadva.

### 6.3 Adatmeret (size)

**Forraas:** A fajl meretek byte-okban vannak a JSON-ban (`sizeInBytes`).

**Ataalaakitas:** Kozvetlenul byte-okban irjuk az XML-be: `size="9408"` (ez ~9 KB).

**Adatmeret szamitaaas prioritasa:**

1. **Valos fajlmeretek** (`specification.files`): Ha a JSON tartalmaz fajlmereteket,
   azokat hasznalja. Kiszuri a 50 MB feletti fajlokat (ezek altalaban megosztott
   adatbazisok, pl. a BLAST "nt" adatbazisa ~4.5 GB, ami nem valos adatatetvitel).

2. **Task-par alapu meretek** (`TASK_PAIR_SIZES` tablaazat): Ha nincs fajlmeret,
   a konverter ismert task-tippus parokra ad realisztikus tartomanyt:
   - `split_fasta -> blastall`: 6 -- 2015 byte (DNS szegmensek)
   - `blastall -> cat_blast`: 5 -- 17952 byte (eredmenyfajlok)
   - `filterContams -> mapMerge`: 1000 -- 50000 byte

3. **Fallback**: Ha semmi mas nem erheto el, `random.randint(1024, 1048576)` -- tehat
   1 KB es 1 MB kozott.

### 6.4 Amount (szulo szamlalo)

**Szabaly:** Ha a task-nak nincs szuloje (`parents` ures) -> `amount="0"`.
Ha van legalabb egy szuloje -> `amount="1"`.

**Fontos:** A WfCommons konverternel ez egyszeru, mert a WfCommons workflow-kban a
szulok mindegyike kulon-kulon kuldi az adatot (tehat minden gyereknek annyi output `<uses>`
eleme van ahany szuloje, es az `amount` szamot kezzel allitja a konverter). A `sendFileToChildren`
metodus a szimulatorban job-onkent keresi meg a gyereket, tehat az `amount` akkor is jol
mukodik, ha mindegyik szulo kulon-kulon dekrementaalja.

---

## 7. Hogyan dolgozza fel a szimulator az XML-t?

### 7.1 Betoltes (WorkflowJobModel.loadWorkflowXml)

A Java JAXB konyvtarral olvassa be az XML-t. Minden `<job>` elembol letrehoz egy
`WorkflowJob` objektumot:
- `id`: egyedi azonosito (prefixelve: `repeatIndex_jobId_name`)
- `runtime`: futasi ido (masodperc, double)
- `inputs`: bemeneti `Uses` objektumok listaja
- `outputs`: kimeneti `Uses` objektumok listaja

### 7.2 Utemezees (MaxMinScheduler)

1. **Inicializaalas:** Minden job-ot hozzaarendel egy computing node-hoz (round-robin).
   A root job-okat (ahol `amount == 0`) rogton berakja a futtatasi sorba.

2. **Schedule:** Amikor `inputs.get(0).amount == 0`, a job bekerul a futtatasi sorba.
   A MaxMinScheduler a legnagyobb runtime-u job-okat futtatja eloszor.

### 7.3 Vegrehajtaas (WorkflowExecutor)

1. **Job futtataaas:**
   - Ha `runtime == 0`: a futasi muvelet a kapott byte-ok alapjan szamolodik
     (`bytesReceived * processingRatio`)
   - Ha `runtime > 0`: `noi = 1000 * runtime * cpuCount * processingPower`
   - A `newComputeTask(noi, ...)` inditja a szamitast a virtuaalis gepen

2. **Adat kuldees a gyerekeknek** (`sendFileToChildren`):
   - Vegigmegy a job output `Uses` elemein
   - Minden `DATA` tipusu outputra:
     - Megkeresi a cel job-ot az ID alapjan
     - Letrehoz egy `StorageObject`-et a megadott `size`-zal
     - Ha ugyanazon a computing node-on vannak: azonnali atetvitel, `amount--`
     - Ha kulonbozo node-on: halozati atetvitel indul (`requestContentDelivery`),
       befejezeskor `amount--`
   - Amikor a cel job `amount` erteke eleri a 0-t -> `schedule()` hivodik -> a job
     bekerul a sorba

3. **Befejezees:**
   - `isAllSchedulerJobCompleted()`: minden job `COMPLETED` statuszuan?
   - Ha igen: rogziti a befejezesi idot, leallitja az energiamerest

### 7.4 A teljes eletciklus peldaval

```
Workflow: SOURCE -> COMPUTE_A -> COMPUTE_B -> SINK

1. Betoltes: 4 WorkflowJob objektum jon letre
2. Inicializaalas: SOURCE amount=0 -> bekeruel a sorba
3. SOURCE lefut (runtime=0, noi a bytesReceived-bol)
4. SOURCE kuldi az adatot COMPUTE_A-nak (size=52MB)
   -> COMPUTE_A amount: 1 -> 0 -> bekeruel a sorba
5. COMPUTE_A lefut (runtime=34s, noi = 1000 * 34 * CPU * power)
6. COMPUTE_A kuldi az adatot COMPUTE_B-nek (size=52MB)
   -> COMPUTE_B amount: 1 -> 0 -> bekeruel a sorba
7. COMPUTE_B lefut (runtime=28s)
8. COMPUTE_B kuldi az adatot SINK-nek (size=52MB)
   -> SINK amount: 1 -> 0 -> bekeruel a sorba
9. SINK lefut (runtime=0)
10. Minden job COMPLETED -> szimulaacioo befejezodott
```

---

## 8. Meretek es idoegysegek

### 8.1 Osszefoglaloo

| Ertek | Egyseg az XML-ben | Egyseg a szimulatorban | Megjegyzes |
|---|---|---|---|
| `runtime` | **masodperc** (double) | Milliszekundum (belsole) | `noi = 1000 * runtime * ...` |
| `size` | **byte** (long) | Byte | Kozvetlenul hasznalja a halozati szimulaaciio |
| `amount` | Darab (int) | Darab | Szuloek szama, visszaszamlalo |
| `activate` | Milliszekundum (long) | Milliszekundum | Szenzor aktivalas keslelteteese (IoT) |

### 8.2 Tiipikus ertektartomanyok

#### WfCommons (realisztikus):
- Runtime: 0.34 -- 154.45 masodperc (receptfuggo)
- Size: 216 byte -- 20 GB (receptfuggo, de a konverter kiszuri az 50MB+ megosztott fajlokat)
- Egy atlagos BLAST transfer: 5 -- 18 KB

#### Workflow Grammar (szintetikus):
- Runtime: 0 -- 60 masodperc (tipusfuggo)
- Size: 1 MB -- 100 MB (SOURCE output), utaana tipusfuggo ataalaakitaasok
- Nagyobb meretek mint a WfCommons, mert ez altalaanosabb szintetikus tesztekhez keszult

### 8.3 Mitol fugg mennyi ideig fut a szimulaacioo?

A szimulaacioos ido a kovetkezoktooel fugg:
1. **Job runtime-ok osszege** (de a parhuzamos agak egyideju futnak)
2. **Adatatetvitel seebessege** (a `size` es a halozati bandszelesseeg fuggvenyeben)
3. **Node-ok szama es kapacitaaasa** (CPU-k, feldolgozo teljesitmeny)
4. **Halozati kesleltetes** (node-ok kozti tavolsag, latency)

---

## 9. Osszefoglalas

### Keet megkozelites, azonos cel

| | WfCommons + Konverter | Workflow Grammar |
|---|---|---|
| **Fajlok** | `generate_workflows.py` + `wfformat_to_dissect_converter.py` | `workflow_grammar.py` |
| **Fuggoseg** | `pip install wfcommons` | nincs (csak Python 3) |
| **Bemenet** | Recept tipusa + task szam | Mintazat + parameterek |
| **Kozbulso lepes** | JSON -> XML konverzio | nincs, kozvetlenul XML |
| **Mintazatok** | Valos tudomanyos workflow-k masolata | 9 epitokocka (pipeline, fan-out, fork-join, stencil, broadcast, stb.) |
| **Runtime forraas** | WfCommons statisztikai modell vagy szintetikus | Tipus alapu random |
| **Size forraas** | Valos fajlmeretek vagy task-par alapu | Tipus alapu szaabaly |
| **Kimenet** | Teljesen azonos DISSECT-CF-Fog XML formatum | Teljesen azonos DISSECT-CF-Fog XML formatum |
| **Mikor hasznald** | Realisztikus tudomanyos szimulaciookhoz | Egyedi strukturaak teszteleseehez |

### A generalt XML-t mindkeet eseetben ugyanugy hasznalod:

```java
String workflowFile = ScenarioBase.resourcePath + "/WORKFLOW_examples/out.xml";
Pair<String, ArrayList<WorkflowJob>> jobs = WorkflowJobModel.loadWorkflowXml(workflowFile, "0");
executor.submitJobs(new MaxMinScheduler(nodes, instance, null, jobs));
```

---

**Verzio:** 2.0
**Letrehozva:** 2025-02-24
**Frissitve:** 2026-03-31
