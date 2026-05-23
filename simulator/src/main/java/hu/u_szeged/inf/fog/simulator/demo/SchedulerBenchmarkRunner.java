package hu.u_szeged.inf.fog.simulator.demo;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.AlterableResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;
import hu.mta.sztaki.lpds.cloud.simulator.util.SeedSyncer;
import hu.u_szeged.inf.fog.simulator.node.ComputingAppliance;
import hu.u_szeged.inf.fog.simulator.node.WorkflowComputingAppliance;
import hu.u_szeged.inf.fog.simulator.provider.Instance;
import hu.u_szeged.inf.fog.simulator.util.EnergyDataCollector;
import hu.u_szeged.inf.fog.simulator.util.SimLogger;
import hu.u_szeged.inf.fog.simulator.util.xml.WorkflowJobModel;
import hu.u_szeged.inf.fog.simulator.workflow.WorkflowExecutor;
import hu.u_szeged.inf.fog.simulator.workflow.WorkflowJob;
import hu.u_szeged.inf.fog.simulator.workflow.scheduler.AdaptiveHeftScheduler;
import hu.u_szeged.inf.fog.simulator.workflow.scheduler.GnnScheduler;
import hu.u_szeged.inf.fog.simulator.workflow.scheduler.HeftDsScheduler;
import hu.u_szeged.inf.fog.simulator.workflow.scheduler.HeftScheduler;
import hu.u_szeged.inf.fog.simulator.workflow.scheduler.MaxMinScheduler;
import hu.u_szeged.inf.fog.simulator.workflow.scheduler.WorkflowScheduler;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Cross-scheduler benchmark. Runs HEFT, HEFT-DS, Max-Min, Adaptive HEFT and
 * the GNN scheduler on the same set of workflows under multiple seeds and
 * writes a single results CSV that the analysis script reads.
 *
 * <p>The workflow list is a hand-picked subset chosen to span:
 * <ul>
 *   <li>Different WfCommons recipes (blast, cycles, genome, montage, seismology)</li>
 *   <li>Random "chaotic" DAGs of different sizes and cross-edge density</li>
 *   <li>The canonical IoT_CyberShake demo workflow</li>
 *   <li>Sizes from 50 to 300 tasks</li>
 * </ul>
 *
 * <p>Output: {@code sim_res/benchmark_results.csv} with columns:
 * <pre>
 *   workflow, scheduler, seed, makespan_sec, energy_kwh, total_tasks,
 *   gnn_decisions, fallback_decisions, avg_inference_ms
 * </pre>
 */
public class SchedulerBenchmarkRunner {

    // Balanced suite: mixes "wide & low-comm" (Adaptive's round-robin sweet spot)
    // with "narrower & comm-heavy" workflows where the GNN gets to actually
    // run and contribute. The bwa/epigenomics/soykb/srasearch ones tend to have
    // dagWidth < 20 OR commIntensity >= 0.05, so they don't fall into
    // Adaptive's distributedMode and exercise the learned policy.
    private static final List<String> WORKFLOWS = Arrays.asList(
            // Wide & low-comm (HEFT-DS struggles here, Adaptive round-robin shines)
            "blast_50tasks_workflow_converted.xml",
            "blast_200tasks_workflow_converted.xml",
            // Diamond / mixed (HEFT-DS sometimes shines)
            "montage_100tasks_workflow_converted.xml",
            "montage_200tasks_workflow_converted.xml",
            // Deep / sequential (HEFT-DS friendly)
            "cycles_100tasks_workflow_converted.xml",
            "cycles_200tasks_workflow_converted.xml",
            "cycles_400tasks_workflow_converted.xml",
            "epigenomics_120tasks_workflow_converted.xml",
            "epigenomics_200tasks_workflow_converted.xml",
            "epigenomics_400tasks_workflow_converted.xml",
            // Comm-intensive
            "bwa_200tasks_workflow_converted.xml",
            "bwa_400tasks_workflow_converted.xml",
            "soykb_120tasks_workflow_converted.xml",
            "soykb_320tasks_workflow_converted.xml",
            "srasearch_120tasks_workflow_converted.xml",
            "genome_150tasks_workflow_converted.xml",
            "genome_320tasks_workflow_converted.xml",
            "seismology_120tasks_workflow_converted.xml",
            // Chaotic + IoT demos
            "chaotic_n300_c40.xml",
            "IoT_CyberShake_100.xml");

    private static final int[] SEEDS = {42, 123, 7};

    private enum SchedulerKind { HEFT, HEFTDS, MAXMIN, ADAPTIVE, GNN }

    public static void main(String[] args) throws Exception {
        SimLogger.setLogging(1, false);

        File outFile = new File("sim_res/benchmark_results.csv");
        outFile.getParentFile().mkdirs();
        try (BufferedWriter w = new BufferedWriter(new FileWriter(outFile))) {
            w.write("workflow,scheduler,seed,makespan_sec,energy_kwh,total_tasks,"
                    + "gnn_decisions,fallback_decisions,avg_inference_ms\n");

            int totalRuns = WORKFLOWS.size() * SchedulerKind.values().length * SEEDS.length;
            int runIdx = 0;
            long startMillis = System.currentTimeMillis();

            for (String wfName : WORKFLOWS) {
                String wfPath = ScenarioBase.resourcePath + "WORKFLOW_examples/" + wfName;
                if (!new File(wfPath).exists()) {
                    System.err.println("Missing workflow, skipping: " + wfPath);
                    continue;
                }
                for (SchedulerKind kind : SchedulerKind.values()) {
                    for (int seed : SEEDS) {
                        runIdx++;
                        System.out.printf("%n[%d/%d] %s  %s  seed=%d%n",
                                runIdx, totalRuns, wfName, kind, seed);
                        try {
                            BenchmarkResult r = runOnce(wfPath, kind, seed);
                            // CSV always uses '.' as decimal separator regardless of JVM locale
                            w.write(String.format(Locale.US,
                                    "%s,%s,%d,%.3f,%.5f,%d,%d,%d,%.4f%n",
                                    wfName, kind.name().toLowerCase(), seed,
                                    r.makespanSec, r.energyKwh, r.totalTasks,
                                    r.gnnDecisions, r.fallbackDecisions,
                                    r.avgInferenceMs));
                            w.flush();
                            System.out.printf(Locale.US,
                                    "  makespan=%.1fs  energy=%.4fkWh  gnn=%d/%d%n",
                                    r.makespanSec, r.energyKwh, r.gnnDecisions,
                                    r.gnnDecisions + r.fallbackDecisions);
                        } catch (Exception e) {
                            System.err.println("  FAILED: " + e);
                            e.printStackTrace();
                        }
                    }
                }
            }
            long secs = (System.currentTimeMillis() - startMillis) / 1000;
            System.out.printf("%nBenchmark done. %d runs in %ds. Results: %s%n",
                    runIdx, secs, outFile.getAbsolutePath());
        }
    }

    private static BenchmarkResult runOnce(String workflowFile, SchedulerKind kind, int seed)
            throws Exception {
        resetSimulatorState();
        SeedSyncer.modifySeed(seed);

        ArrayList<WorkflowComputingAppliance> nodes = TrainingSetup.buildNodes();
        List<ArrayList<WorkflowComputingAppliance>> clusters = TrainingSetup.cluster(nodes);
        TrainingSetup.attachEnergyCollectors(nodes);

        WorkflowExecutor executor = WorkflowExecutor.getIstance();
        VirtualAppliance va = new VirtualAppliance("va", 100, 0, false, 1_073_741_824L);
        AlterableResourceConstraints arc = new AlterableResourceConstraints(1, 0.001, 1_073_741_824L);
        Instance instance = new Instance("instance", va, arc, 0.102 / 60 / 60 / 1000, 1);

        ArrayList<Object> schedulers = new ArrayList<>();
        int totalTasks = 0;
        for (int i = 0; i < clusters.size(); i++) {
            Pair<String, ArrayList<WorkflowJob>> jobs =
                    WorkflowJobModel.loadWorkflowXml(workflowFile, Integer.toString(i));
            totalTasks = Math.max(totalTasks, jobs.getRight().size());
            Object sch;
            switch (kind) {
                case HEFT:
                    sch = new HeftScheduler(clusters.get(i), instance, null, jobs);
                    break;
                case HEFTDS:
                    sch = new HeftDsScheduler(clusters.get(i), instance, null, jobs);
                    break;
                case MAXMIN:
                    sch = new MaxMinScheduler(clusters.get(i), instance, null, jobs);
                    break;
                case ADAPTIVE:
                    sch = new AdaptiveHeftScheduler(clusters.get(i), instance, null, jobs);
                    break;
                case GNN:
                    sch = new GnnScheduler(clusters.get(i), instance, null, jobs);
                    break;
                default:
                    throw new IllegalStateException(kind.name());
            }
            executor.submitJobs((WorkflowScheduler) sch);
            schedulers.add(sch);
        }

        Timed.simulateUntilLastEvent();

        double totalEnergyKwh = 0;
        for (EnergyDataCollector c : EnergyDataCollector.energyCollectors) {
            totalEnergyKwh += c.energyConsumption / 3_600_000_000.0;
        }
        double perCluster = schedulers.isEmpty() ? 0 : totalEnergyKwh / schedulers.size();

        double maxMakespan = 0;
        int gnnDecisions = 0;
        int fallbackDecisions = 0;
        long inferenceTimeNs = 0;
        for (Object o : schedulers) {
            WorkflowScheduler ws = (WorkflowScheduler) o;
            double ms = (ws.stopTime - ws.startTime) / 1000.0;
            maxMakespan = Math.max(maxMakespan, ms);
            if (o instanceof GnnScheduler) {
                GnnScheduler gs = (GnnScheduler) o;
                gnnDecisions += gs.gnnDecisions;
                fallbackDecisions += gs.fallbackDecisions;
                inferenceTimeNs += gs.inferenceTimeNs;
                gs.close();
            }
        }
        int totalDecisions = gnnDecisions + fallbackDecisions;
        double avgInferenceMs = totalDecisions == 0
                ? 0
                : (inferenceTimeNs / 1_000_000.0) / totalDecisions;

        BenchmarkResult r = new BenchmarkResult();
        r.makespanSec = maxMakespan;
        r.energyKwh = perCluster;
        r.totalTasks = totalTasks;
        r.gnnDecisions = gnnDecisions;
        r.fallbackDecisions = fallbackDecisions;
        r.avgInferenceMs = avgInferenceMs;
        return r;
    }

    /**
     * Best-effort reset of all known static simulator state between runs.
     * Same as {@code BatchTrainingRunner.resetSimulatorState}.
     */
    private static void resetSimulatorState() {
        Timed.resetTimed();
        WorkflowScheduler.schedulers.clear();
        EnergyDataCollector.energyCollectors.clear();
        EnergyDataCollector.readings.clear();
        ComputingAppliance.allComputingAppliances.clear();
        try {
            Field exec = WorkflowExecutor.class.getDeclaredField("executor");
            exec.setAccessible(true);
            exec.set(null, null);

            Field schedulers = WorkflowExecutor.class.getDeclaredField("workflowSchedulers");
            schedulers.setAccessible(true);
            schedulers.set(null, new ArrayList<WorkflowScheduler>());
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Cannot reset WorkflowExecutor", e);
        }
    }

    private static final class BenchmarkResult {
        double makespanSec;
        double energyKwh;
        int totalTasks;
        int gnnDecisions;
        int fallbackDecisions;
        double avgInferenceMs;
    }
}
