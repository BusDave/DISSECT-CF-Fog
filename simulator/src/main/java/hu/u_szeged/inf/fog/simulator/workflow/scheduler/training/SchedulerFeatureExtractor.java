package hu.u_szeged.inf.fog.simulator.workflow.scheduler.training;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.u_szeged.inf.fog.simulator.node.WorkflowComputingAppliance;
import hu.u_szeged.inf.fog.simulator.workflow.WorkflowJob;
import java.util.List;
import java.util.Map;

/**
 * Builds a fixed-size feature vector describing the scheduling state at a
 * decision point. The same schema is used by every *SchedulerWithLogging so
 * the training notebook can load CSVs from different schedulers into one
 * dataframe.
 *
 * <p>Layout (50 dims):
 * <pre>
 *   [0..4]    task-level  (5):  runtime, rank_up, in_deg, out_deg, total_input_bytes
 *   [5..9]    global      (5):  elapsed_s, tasks_done, tasks_remaining,
 *                               total_active_vms, avg_node_load
 *   [10..49]  per-node   (40):  for each of 20 nodes -> queue_size, running_tasks
 * </pre>
 *
 * <p>Nodes are indexed by position in {@code computeArchitecture}. If fewer
 * than 20 nodes exist, the unused slots are zero-filled. More than 20 is
 * truncated.
 */
public final class SchedulerFeatureExtractor {

    public static final int MAX_NODES = 20;
    public static final int TASK_FEATURES = 5;
    public static final int GLOBAL_FEATURES = 5;
    public static final int PER_NODE_FEATURES = 2;
    public static final int FEATURE_DIM =
            TASK_FEATURES + GLOBAL_FEATURES + MAX_NODES * PER_NODE_FEATURES;

    public static final String[] FEATURE_NAMES = buildFeatureNames();

    private SchedulerFeatureExtractor() {
    }

    /**
     * Build a 50-dim feature vector for the decision at hand.
     *
     * @param job          the task being scheduled
     * @param rankUp       upward-rank map (may be empty for non-HEFT schedulers)
     * @param predecessors task -&gt; list of parent task ids
     * @param successors   task -&gt; list of child task ids
     * @param commData     parent -&gt; (child -&gt; bytes) inter-task data sizes
     * @param nodes        the compute architecture (first 20 used)
     * @param tasksDone    decisions made so far in this scheduler run
     * @param totalTasks   total number of jobs in the workflow
     * @param elapsedSec   simulated time elapsed (seconds)
     */
    public static double[] extract(
            WorkflowJob job,
            Map<String, Double> rankUp,
            Map<String, List<String>> predecessors,
            Map<String, List<String>> successors,
            Map<String, Map<String, Long>> commData,
            List<WorkflowComputingAppliance> nodes,
            int tasksDone,
            int totalTasks,
            double elapsedSec) {

        double[] f = new double[FEATURE_DIM];

        // ----- task-level (5) -----
        f[0] = job.runtime;
        f[1] = rankUp != null ? rankUp.getOrDefault(job.id, 0.0) : 0.0;
        f[2] = predecessors != null
                ? predecessors.getOrDefault(job.id, java.util.Collections.emptyList()).size()
                : 0.0;
        f[3] = successors != null
                ? successors.getOrDefault(job.id, java.util.Collections.emptyList()).size()
                : 0.0;
        f[4] = totalInputBytes(job, predecessors, commData);

        // ----- global (5) -----
        f[5] = elapsedSec;
        f[6] = tasksDone;
        f[7] = totalTasks - tasksDone;
        f[8] = totalActiveVms(nodes);
        f[9] = avgNodeLoad(nodes);

        // ----- per-node (2 each, up to MAX_NODES) -----
        int n = Math.min(MAX_NODES, nodes.size());
        for (int i = 0; i < n; i++) {
            WorkflowComputingAppliance ca = nodes.get(i);
            int base = TASK_FEATURES + GLOBAL_FEATURES + i * PER_NODE_FEATURES;
            f[base] = ca.workflowQueue != null ? ca.workflowQueue.size() : 0;
            f[base + 1] = countRunningTasks(ca);
        }
        return f;
    }

    private static double totalInputBytes(WorkflowJob job,
                                          Map<String, List<String>> predecessors,
                                          Map<String, Map<String, Long>> commData) {
        if (predecessors == null || commData == null) {
            return 0.0;
        }
        long total = 0;
        for (String predId : predecessors.getOrDefault(job.id, java.util.Collections.emptyList())) {
            Map<String, Long> edges = commData.get(predId);
            if (edges != null) {
                total += edges.getOrDefault(job.id, 0L);
            }
        }
        return total;
    }

    private static int totalActiveVms(List<WorkflowComputingAppliance> nodes) {
        int n = 0;
        for (WorkflowComputingAppliance ca : nodes) {
            for (VirtualMachine vm : ca.iaas.listVMs()) {
                if (VirtualMachine.State.RUNNING.equals(vm.getState())) {
                    n++;
                }
            }
        }
        return n;
    }

    private static double avgNodeLoad(List<WorkflowComputingAppliance> nodes) {
        if (nodes.isEmpty()) {
            return 0.0;
        }
        long sum = 0;
        for (WorkflowComputingAppliance ca : nodes) {
            sum += countRunningTasks(ca);
        }
        return sum / (double) nodes.size();
    }

    private static int countRunningTasks(WorkflowComputingAppliance ca) {
        int n = 0;
        for (VirtualMachine vm : ca.iaas.listVMs()) {
            if (VirtualMachine.State.RUNNING.equals(vm.getState())) {
                n += vm.underProcessing.size();
            }
        }
        return n;
    }

    private static String[] buildFeatureNames() {
        String[] names = new String[FEATURE_DIM];
        names[0] = "task_runtime";
        names[1] = "task_rank_up";
        names[2] = "task_in_degree";
        names[3] = "task_out_degree";
        names[4] = "task_total_input_bytes";
        names[5] = "elapsed_sec";
        names[6] = "tasks_done";
        names[7] = "tasks_remaining";
        names[8] = "total_active_vms";
        names[9] = "avg_node_load";
        for (int i = 0; i < MAX_NODES; i++) {
            int base = TASK_FEATURES + GLOBAL_FEATURES + i * PER_NODE_FEATURES;
            names[base] = "node" + i + "_queue";
            names[base + 1] = "node" + i + "_running";
        }
        return names;
    }
}
