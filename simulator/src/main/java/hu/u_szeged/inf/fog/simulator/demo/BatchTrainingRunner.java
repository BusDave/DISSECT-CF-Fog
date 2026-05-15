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
import hu.u_szeged.inf.fog.simulator.workflow.scheduler.WorkflowScheduler;
import hu.u_szeged.inf.fog.simulator.workflow.scheduler.training.AdaptiveHeftSchedulerWithLogging;
import hu.u_szeged.inf.fog.simulator.workflow.scheduler.training.HeftDsSchedulerWithLogging;
import hu.u_szeged.inf.fog.simulator.workflow.scheduler.training.HeftSchedulerWithLogging;
import hu.u_szeged.inf.fog.simulator.workflow.scheduler.training.MaxMinSchedulerWithLogging;
import java.io.File;
import java.io.FilenameFilter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Iterates every {@code *.xml} workflow in {@code WORKFLOW_examples/} and
 * runs HEFT + HEFT-DS on each, dumping training CSVs to
 * {@code sim_res/training/}.
 *
 * <p>Between runs all known static state is reset:
 * <ul>
 *   <li>{@link Timed#resetTimed()} — clears the event queue and time counter</li>
 *   <li>{@link WorkflowScheduler#schedulers} — list of registered schedulers</li>
 *   <li>{@link EnergyDataCollector#energyCollectors} + readings</li>
 *   <li>{@link ComputingAppliance#allComputingAppliances}</li>
 *   <li>{@code WorkflowExecutor.executor} singleton (reflection) +
 *       {@code workflowSchedulers}</li>
 * </ul>
 *
 * <p>If state leakage manifests (sims hang, memory bloats, makespans drift),
 * fall back to running each workflow in its own JVM.
 */
public class BatchTrainingRunner {

    /** Skip workflows in this list — typically Pegasus-DAX format that the
     *  JAXB parser cannot handle (namespaced XML). */
    private static final List<String> SKIP = Arrays.asList(
            "CyberShake_100.xml", // original Pegasus DAX, has xmlns
            "IoT_workflow.xml"    // HEFT init crashes (IndexOutOfBoundsException at HeftScheduler:134)
    );

    public static void main(String[] args) throws Exception {
        SeedSyncer.modifySeed(42);                  // deterministic batch
        SimLogger.setLogging(1, false);             // less verbose for batch

        File wfDir = new File(ScenarioBase.resourcePath + "WORKFLOW_examples");
        File[] xmls = wfDir.listFiles(new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return name.endsWith(".xml") && !SKIP.contains(name);
            }
        });
        if (xmls == null || xmls.length == 0) {
            System.err.println("No workflow XMLs found in " + wfDir);
            return;
        }
        Arrays.sort(xmls);

        SchedulerKind[] kinds = SchedulerKind.values();
        System.out.printf("Found %d workflows, running %d schedulers on each (%d total runs).%n",
                xmls.length, kinds.length, xmls.length * kinds.length);

        int run = 0;
        int total = xmls.length * kinds.length;
        long t0 = System.currentTimeMillis();

        for (File xml : xmls) {
            String wfPath = xml.getAbsolutePath();
            String wfName = xml.getName();

            for (SchedulerKind kind : kinds) {
                try {
                    run++;
                    System.out.printf("%n[%d/%d] %s   %s%n", run, total, wfName, kind);
                    resetSimulatorState();
                    runOnce(wfPath, kind);
                } catch (Exception e) {
                    System.err.println(kind + " failed on " + wfName + ": " + e);
                    e.printStackTrace();
                }
            }
        }

        long secs = (System.currentTimeMillis() - t0) / 1000;
        System.out.printf("%nBatch done. %d runs in %ds.%n", run, secs);
    }

    private enum SchedulerKind { HEFT, HEFTDS, MAXMIN, ADAPTIVE }

    /**
     * Strip the directory and extension so the XML filename becomes a unique
     * identifier per workflow. The WfCommons recipes overload the XML's
     * internal {@code name} attribute (every blast file says
     * "Blast-synthetic-instance0", etc), which collapses 5 size variants into
     * a single appName and breaks the notebook's per-workflow EDP grouping.
     * We override the appName to the filename to keep training data per-file
     * distinguishable.
     */
    private static String stableAppName(String workflowFile) {
        String name = new File(workflowFile).getName();
        int dot = name.lastIndexOf('.');
        return dot > 0 ? name.substring(0, dot) : name;
    }

    private static void runOnce(String workflowFile, SchedulerKind kind) throws Exception {
        ArrayList<WorkflowComputingAppliance> nodes = TrainingSetup.buildNodes();
        List<ArrayList<WorkflowComputingAppliance>> clusters = TrainingSetup.cluster(nodes);
        TrainingSetup.attachEnergyCollectors(nodes);

        WorkflowExecutor executor = WorkflowExecutor.getIstance();
        VirtualAppliance va = new VirtualAppliance("va", 100, 0, false, 1_073_741_824L);
        AlterableResourceConstraints arc = new AlterableResourceConstraints(1, 0.001, 1_073_741_824L);
        Instance instance = new Instance("instance", va, arc, 0.102 / 60 / 60 / 1000, 1);

        String workflowName = stableAppName(workflowFile);

        ArrayList<Object> loggers = new ArrayList<>();
        for (int i = 0; i < clusters.size(); i++) {
            Pair<String, ArrayList<WorkflowJob>> originalJobs =
                    WorkflowJobModel.loadWorkflowXml(workflowFile, Integer.toString(i));
            // Override the appName with <filename>_c<clusterIdx> so the CSV
            // stems written by TrainingLogger remain unique across workflows
            // even when several XMLs declare the same internal name.
            Pair<String, ArrayList<WorkflowJob>> jobs = org.apache.commons.lang3.tuple.ImmutablePair.of(
                    workflowName + "_c" + i, originalJobs.getRight());
            Object sch;
            switch (kind) {
                case HEFT:
                    sch = new HeftSchedulerWithLogging(clusters.get(i), instance, null, jobs, workflowFile);
                    break;
                case HEFTDS:
                    sch = new HeftDsSchedulerWithLogging(clusters.get(i), instance, null, jobs, workflowFile);
                    break;
                case MAXMIN:
                    sch = new MaxMinSchedulerWithLogging(clusters.get(i), instance, null, jobs, workflowFile);
                    break;
                case ADAPTIVE:
                    sch = new AdaptiveHeftSchedulerWithLogging(clusters.get(i), instance, null, jobs, workflowFile);
                    break;
                default:
                    throw new IllegalStateException(kind.name());
            }
            executor.submitJobs((WorkflowScheduler) sch);
            loggers.add(sch);
        }

        Timed.simulateUntilLastEvent();

        double totalEnergyKwh = 0;
        for (EnergyDataCollector c : EnergyDataCollector.energyCollectors) {
            totalEnergyKwh += c.energyConsumption / 3_600_000_000.0;
        }
        double perCluster = loggers.isEmpty() ? 0 : totalEnergyKwh / loggers.size();

        for (Object o : loggers) {
            // Order matters: AdaptiveHeftSchedulerWithLogging extends
            // AdaptiveHeftScheduler which extends HeftDsScheduler which extends
            // HeftScheduler. Without the explicit Adaptive check first, the
            // HEFT branch would catch the adaptive instance and call the wrong
            // finishLogging.
            if (o instanceof AdaptiveHeftSchedulerWithLogging) {
                AdaptiveHeftSchedulerWithLogging sch = (AdaptiveHeftSchedulerWithLogging) o;
                double makespanSec = (sch.stopTime - sch.startTime) / 1000.0;
                sch.finishLogging(makespanSec, perCluster);
                System.out.printf("  [ADAPTIVE] %d dec, makespan=%.1fs, kWh=%.4f%n",
                        sch.getLogger().getDecisionCount(), makespanSec, perCluster);
            } else if (o instanceof HeftDsSchedulerWithLogging) {
                HeftDsSchedulerWithLogging sch = (HeftDsSchedulerWithLogging) o;
                double makespanSec = (sch.stopTime - sch.startTime) / 1000.0;
                sch.finishLogging(makespanSec, perCluster);
                System.out.printf("  [HEFT-DS] %d dec, makespan=%.1fs, kWh=%.4f%n",
                        sch.getLogger().getDecisionCount(), makespanSec, perCluster);
            } else if (o instanceof HeftSchedulerWithLogging) {
                HeftSchedulerWithLogging sch = (HeftSchedulerWithLogging) o;
                double makespanSec = (sch.stopTime - sch.startTime) / 1000.0;
                sch.finishLogging(makespanSec, perCluster);
                System.out.printf("  [HEFT] %d dec, makespan=%.1fs, kWh=%.4f%n",
                        sch.getLogger().getDecisionCount(), makespanSec, perCluster);
            } else if (o instanceof MaxMinSchedulerWithLogging) {
                MaxMinSchedulerWithLogging sch = (MaxMinSchedulerWithLogging) o;
                double makespanSec = (sch.stopTime - sch.startTime) / 1000.0;
                sch.finishLogging(makespanSec, perCluster);
                System.out.printf("  [MAXMIN] %d dec, makespan=%.1fs, kWh=%.4f%n",
                        sch.getLogger().getDecisionCount(), makespanSec, perCluster);
            }
        }
    }

    /**
     * Best-effort reset of all known static simulator state between runs.
     * Anything missed here will accumulate in the JVM across iterations and
     * may eventually distort training data — re-run from a fresh JVM if you
     * notice metrics drifting.
     */
    private static void resetSimulatorState() {
        Timed.resetTimed();

        WorkflowScheduler.schedulers.clear();
        EnergyDataCollector.energyCollectors.clear();
        EnergyDataCollector.readings.clear();
        ComputingAppliance.allComputingAppliances.clear();

        // WorkflowExecutor is a singleton with private static state; clear it
        // via reflection so the next getIstance() call rebuilds from scratch.
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
}
