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
import hu.u_szeged.inf.fog.simulator.workflow.scheduler.NeuralScheduler;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Runs a single workflow with {@link NeuralScheduler} and reports makespan,
 * energy, and inference statistics. Uses {@link TrainingSetup} for the
 * 20-node cluster so the state distribution matches what the model was
 * trained on.
 */
public class NeuralSchedulerDemo {

    public static void main(String[] args) throws Exception {
        SeedSyncer.modifySeed(System.currentTimeMillis());
        SimLogger.setLogging(1, true);

        ArrayList<WorkflowComputingAppliance> nodes = TrainingSetup.buildNodes();
        List<ArrayList<WorkflowComputingAppliance>> clusters = TrainingSetup.cluster(nodes);
        TrainingSetup.attachEnergyCollectors(nodes);

        WorkflowExecutor executor = WorkflowExecutor.getIstance();
        String workflowFile = args.length > 0
                ? args[0]
                : ScenarioBase.resourcePath + "WORKFLOW_examples/IoT_CyberShake_100.xml";

        VirtualAppliance va = new VirtualAppliance("va", 100, 0, false, 1_073_741_824L);
        AlterableResourceConstraints arc = new AlterableResourceConstraints(1, 0.001, 1_073_741_824L);
        Instance instance = new Instance("instance", va, arc, 0.102 / 60 / 60 / 1000, 1);

        ArrayList<NeuralScheduler> schedulers = new ArrayList<>();
        for (int i = 0; i < clusters.size(); i++) {
            Pair<String, ArrayList<WorkflowJob>> jobs =
                    WorkflowJobModel.loadWorkflowXml(workflowFile, Integer.toString(i));
            NeuralScheduler sch =
                    new NeuralScheduler(clusters.get(i), instance, null, jobs);
            executor.submitJobs(sch);
            schedulers.add(sch);
        }

        Timed.simulateUntilLastEvent();
        ScenarioBase.logStreamProcessing();

        double totalEnergyKwh = 0;
        for (EnergyDataCollector c : EnergyDataCollector.energyCollectors) {
            totalEnergyKwh += c.energyConsumption / 3_600_000_000.0;
        }

        System.out.println();
        System.out.println("=== NeuralScheduler results ===");
        for (NeuralScheduler sch : schedulers) {
            double makespanSec = (sch.stopTime - sch.startTime) / 1000.0;
            int total = sch.neuralDecisions + sch.fallbackDecisions;
            double neuralPct = total == 0 ? 0 : 100.0 * sch.neuralDecisions / total;
            double avgInferenceMs = total == 0
                    ? 0
                    : (sch.inferenceTimeNs / 1_000_000.0) / total;
            System.out.printf("  cluster %s%n", sch.appName);
            System.out.printf("    makespan        : %.2f s%n", makespanSec);
            System.out.printf("    neural / total  : %d / %d  (%.1f%% neural)%n",
                    sch.neuralDecisions, total, neuralPct);
            System.out.printf("    fallback (HEFT) : %d%n", sch.fallbackDecisions);
            System.out.printf("    inference time  : %.2f ms total, %.3f ms/decision%n",
                    sch.inferenceTimeNs / 1_000_000.0, avgInferenceMs);
            sch.close();
        }
        System.out.printf("  total energy: %.4f kWh%n", totalEnergyKwh);
    }
}
