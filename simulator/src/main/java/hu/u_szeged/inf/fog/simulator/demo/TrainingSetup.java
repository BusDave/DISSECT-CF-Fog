package hu.u_szeged.inf.fog.simulator.demo;

import hu.u_szeged.inf.fog.simulator.iot.mobility.GeoLocation;
import hu.u_szeged.inf.fog.simulator.node.WorkflowComputingAppliance;
import hu.u_szeged.inf.fog.simulator.util.EnergyDataCollector;
import hu.u_szeged.inf.fog.simulator.workflow.aco.CentralisedAntOptimiser;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Shared 20-node cluster setup for the training-data demos. Pulled out so
 * {@link HeftLoggingDemo} and {@link HeftDsLoggingDemo} share the same
 * environment (otherwise each scheduler would learn from a slightly
 * different state distribution).
 *
 * <p>Composition: 4 cloud (ELKH 52-core) + 12 fog (LPDS 16-core) + 4 small
 * (4-core inline). Matches the production setup of {@link WorkflowSimulation}.
 */
public final class TrainingSetup {

    private TrainingSetup() {
    }

    public static ArrayList<WorkflowComputingAppliance> buildNodes() throws Exception {
        String cloudfile = ScenarioBase.resourcePath + "ELKH_original.xml";
        String fogfile = ScenarioBase.resourcePath + "XML_examples/LPDS_16.xml";

        WorkflowComputingAppliance node0  = new WorkflowComputingAppliance(cloudfile, "node0",  new GeoLocation(48.8566, 2.3522),   0);
        WorkflowComputingAppliance node1  = new WorkflowComputingAppliance(fogfile,   "node1",  new GeoLocation(51.5074, -0.1278),  0);
        WorkflowComputingAppliance node3  = new WorkflowComputingAppliance(fogfile,   "node3",  new GeoLocation(41.9028, 12.4964),  0);
        WorkflowComputingAppliance node5  = new WorkflowComputingAppliance(fogfile,   "node5",  new GeoLocation(43.7102, 7.2620),   0);
        WorkflowComputingAppliance node6  = new WorkflowComputingAppliance(fogfile,   "node6",  new GeoLocation(55.6761, 12.5683),  0);
        WorkflowComputingAppliance node8  = new WorkflowComputingAppliance(fogfile,   "node8",  new GeoLocation(48.2082, 16.3738),  0);
        WorkflowComputingAppliance node9  = new WorkflowComputingAppliance(fogfile,   "node9",  new GeoLocation(50.8503, 4.3517),   0);
        WorkflowComputingAppliance node10 = new WorkflowComputingAppliance(fogfile,   "node10", new GeoLocation(46.7762, 23.6213),  0);
        WorkflowComputingAppliance node11 = new WorkflowComputingAppliance(cloudfile, "node11", new GeoLocation(48.1351, 11.5820),  0);
        WorkflowComputingAppliance node12 = new WorkflowComputingAppliance(fogfile,   "node12", new GeoLocation(53.9076, 27.5754),  0);
        WorkflowComputingAppliance node13 = new WorkflowComputingAppliance(fogfile,   "node13", new GeoLocation(60.1695, 24.9354),  0);
        WorkflowComputingAppliance node14 = new WorkflowComputingAppliance(fogfile,   "node14", new GeoLocation(39.9334, 32.8597),  0);
        WorkflowComputingAppliance node16 = new WorkflowComputingAppliance(fogfile,   "node16", new GeoLocation(37.9838, 23.7275),  0);
        WorkflowComputingAppliance node17 = new WorkflowComputingAppliance(fogfile,   "node17", new GeoLocation(52.3702, 4.8952),   0);
        WorkflowComputingAppliance node19 = new WorkflowComputingAppliance(cloudfile, "node19", new GeoLocation(51.1657, 10.4515),  0);

        WorkflowComputingAppliance node18 = new WorkflowComputingAppliance(
                AgentTestUNC.createNode("node18", 4, 0.001, 4L * 1_073_741_824L, 32L * 1_073_741_824L,
                        1, 3.5, 7.5, 62_500, 15, new HashMap<>()),
                new GeoLocation(55.9533, -3.1883));
        WorkflowComputingAppliance node15 = new WorkflowComputingAppliance(
                AgentTestUNC.createNode("node15", 4, 0.001, 4L * 1_073_741_824L, 32L * 1_073_741_824L,
                        1, 3.5, 7.5, 62_500, 15, new HashMap<>()),
                new GeoLocation(40.4168, -3.7038));
        WorkflowComputingAppliance node4 = new WorkflowComputingAppliance(
                AgentTestUNC.createNode("node4", 4, 0.001, 4L * 1_073_741_824L, 32L * 1_073_741_824L,
                        1, 3.5, 7.5, 62_500, 15, new HashMap<>()),
                new GeoLocation(41.0082, 28.9784));
        WorkflowComputingAppliance node7 = new WorkflowComputingAppliance(
                AgentTestUNC.createNode("node7", 4, 0.001, 4L * 1_073_741_824L, 32L * 1_073_741_824L,
                        1, 3.5, 7.5, 62_500, 15, new HashMap<>()),
                new GeoLocation(59.3293, 18.0686));
        WorkflowComputingAppliance node2 = new WorkflowComputingAppliance(
                AgentTestUNC.createNode("node2", 4, 0.001, 4L * 1_073_741_824L, 32L * 1_073_741_824L,
                        1, 3.5, 7.5, 62_500, 15, new HashMap<>()),
                new GeoLocation(52.5200, 13.4050));

        WorkflowComputingAppliance.setDistanceBasedLatency();

        ArrayList<WorkflowComputingAppliance> nodes = new ArrayList<>();
        nodes.add(node0);  nodes.add(node1);  nodes.add(node2);  nodes.add(node3);
        nodes.add(node4);  nodes.add(node5);  nodes.add(node6);  nodes.add(node7);
        nodes.add(node8);  nodes.add(node9);  nodes.add(node10); nodes.add(node11);
        nodes.add(node12); nodes.add(node13); nodes.add(node14); nodes.add(node15);
        nodes.add(node16); nodes.add(node17); nodes.add(node18); nodes.add(node19);
        return nodes;
    }

    /**
     * Run the centralised ANT-based clusterer on the given node set and return
     * the cluster list ordered by average pairwise distance.
     */
    public static List<ArrayList<WorkflowComputingAppliance>> cluster(
            ArrayList<WorkflowComputingAppliance> nodes) {
        HashMap<Integer, ArrayList<WorkflowComputingAppliance>> assignments =
                CentralisedAntOptimiser.runOptimiser(1, nodes, 50, 200, 0.5, 0.2, 0.15, 0.3);
        CentralisedAntOptimiser.printClusterAssignments(assignments);
        return CentralisedAntOptimiser.sortClustersByAveragePairwiseDistance(assignments);
    }

    /**
     * Attach an EnergyDataCollector to every node so the run-end energy
     * footprint can be computed for the training reward signal.
     */
    public static void attachEnergyCollectors(ArrayList<WorkflowComputingAppliance> nodes) {
        for (int i = 0; i < nodes.size(); i++) {
            new EnergyDataCollector("node-" + i, nodes.get(i).iaas, true);
        }
    }
}
