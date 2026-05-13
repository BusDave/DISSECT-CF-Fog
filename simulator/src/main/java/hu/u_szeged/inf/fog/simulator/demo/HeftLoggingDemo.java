package hu.u_szeged.inf.fog.simulator.demo;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.AlterableResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;
import hu.mta.sztaki.lpds.cloud.simulator.util.SeedSyncer;
import hu.u_szeged.inf.fog.simulator.node.WorkflowComputingAppliance;
import hu.u_szeged.inf.fog.simulator.provider.Instance;
import hu.u_szeged.inf.fog.simulator.util.EnergyDataCollector;
import hu.u_szeged.inf.fog.simulator.util.SimLogger;
import hu.u_szeged.inf.fog.simulator.util.xml.WorkflowJobModel;
import hu.u_szeged.inf.fog.simulator.workflow.WorkflowExecutor;
import hu.u_szeged.inf.fog.simulator.workflow.WorkflowJob;
import hu.u_szeged.inf.fog.simulator.workflow.scheduler.training.HeftSchedulerWithLogging;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Single-workflow training data collection for the HEFT scheduler.
 *
 * <p>Cluster and energy collection live in {@link TrainingSetup}; this class
 * only chooses the workflow and the scheduler. After the run completes, each
 * scheduler instance writes a CSV + sidecar JSON to {@code sim_res/training/}.
 *
 * <p>For HEFT-DS data, run {@link HeftDsLoggingDemo} instead — they share the
 * same node setup so the state distributions are comparable.
 */
public class HeftLoggingDemo {

    public static void main(String[] args) throws Exception {
        SeedSyncer.modifySeed(System.currentTimeMillis());
        SimLogger.setLogging(1, true);

        ArrayList<WorkflowComputingAppliance> nodes = TrainingSetup.buildNodes();
        List<ArrayList<WorkflowComputingAppliance>> clusters = TrainingSetup.cluster(nodes);
        TrainingSetup.attachEnergyCollectors(nodes);

        WorkflowExecutor executor = WorkflowExecutor.getIstance();
        String workflowFile = ScenarioBase.resourcePath + "WORKFLOW_examples/IoT_CyberShake_100.xml";

        VirtualAppliance va = new VirtualAppliance("va", 100, 0, false, 1_073_741_824L);
        AlterableResourceConstraints arc = new AlterableResourceConstraints(1, 0.001, 1_073_741_824L);
        Instance instance = new Instance("instance", va, arc, 0.102 / 60 / 60 / 1000, 1);

        ArrayList<HeftSchedulerWithLogging> loggers = new ArrayList<>();
        for (int i = 0; i < clusters.size(); i++) {
            Pair<String, ArrayList<WorkflowJob>> jobs =
                    WorkflowJobModel.loadWorkflowXml(workflowFile, Integer.toString(i));
            HeftSchedulerWithLogging sch =
                    new HeftSchedulerWithLogging(clusters.get(i), instance, null, jobs);
            executor.submitJobs(sch);
            loggers.add(sch);
        }

        Timed.simulateUntilLastEvent();
        ScenarioBase.logStreamProcessing();

        double totalEnergyKwh = 0;
        for (EnergyDataCollector c : EnergyDataCollector.energyCollectors) {
            totalEnergyKwh += c.energyConsumption / 3_600_000_000.0;
        }
        for (HeftSchedulerWithLogging sch : loggers) {
            double makespanSec = (sch.stopTime - sch.startTime) / 1000.0;
            sch.finishLogging(makespanSec, totalEnergyKwh / loggers.size());
            System.out.println("[HEFT] " + sch.getLogger().getDecisionCount()
                    + " decisions, makespan=" + makespanSec + "s, csv="
                    + sch.getLogger().getCsvPath());
        }
    }
}
