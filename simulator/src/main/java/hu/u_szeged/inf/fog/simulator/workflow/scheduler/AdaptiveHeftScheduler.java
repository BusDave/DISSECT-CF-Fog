package hu.u_szeged.inf.fog.simulator.workflow.scheduler;

import hu.u_szeged.inf.fog.simulator.iot.Actuator;
import hu.u_szeged.inf.fog.simulator.node.WorkflowComputingAppliance;
import hu.u_szeged.inf.fog.simulator.provider.Instance;
import hu.u_szeged.inf.fog.simulator.util.SimLogger;
import hu.u_szeged.inf.fog.simulator.workflow.WorkflowJob;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang3.tuple.Pair;

/**
 * HEFT variant that picks its placement strategy from DAG characteristics
 * instead of always using EFT-greedy.
 *
 * <p>Inherits from {@link HeftDsScheduler} so the EFT-greedy fallback already
 * carries the energy-aware HEFT-DS optimisations (critical-path detection,
 * sub-deadline aware MIPS selection, lonely-node task merging). When the
 * workflow looks "wide and sparse" we overlay a capacity-weighted distribution
 * on top instead.
 *
 * <p>Motivation: classical HEFT prefers fast processors via min-EFT, which on
 * communication-heavy DAGs is the right call. On wide, low-communication
 * workflows (think bioinformatics blast: 200 mostly-independent jobs, total
 * data sub-MB) the bias to the 3 cloud nodes serialises work that could fan
 * out across all 20 nodes. A plain round-robin distribution beats HEFT by
 * ~2x makespan on such workflows.
 *
 * <p>Why round-robin (and not capacity-weighted): in this simulator every VM
 * is provisioned with the same {@code (1 CPU, 0.001 power)} constraint, so
 * each VM runs a single task at the same speed regardless of which node hosts
 * it. The node-level CPU count only multiplies the available parallelism, and
 * the autoscaling policy adds VMs reactively (only when {@code jobCount &gt;
 * listVMs()}). Empirically the system stabilises at ~1 VM per node, so the
 * makespan is dominated by the most-loaded node. Round-robin keeps all nodes
 * at the same load (= optimal bottleneck); capacity-weighted overloads the
 * big nodes and slows the bottleneck down ~2.5x.
 *
 * <p>Decision: at {@code init()} we compute
 * <ul>
 *   <li>{@code width} - max number of mutually independent tasks
 *       (max topological-level size)</li>
 *   <li>{@code commIntensity} - (total bytes / commRate) / total compute time;
 *       the fraction of the schedule that is data movement under the assumed
 *       link speed</li>
 * </ul>
 * If {@code width >= numNodes} <b>and</b>
 * {@code commIntensity < COMM_INTENSITY_THRESHOLD}, we replace the EFT-greedy
 * static assignment with a round-robin distribution and freeze the dynamic
 * phase to obey it. Otherwise the scheduler behaves identically to its
 * {@link HeftDsScheduler} parent.
 */
public class AdaptiveHeftScheduler extends HeftDsScheduler {

    /** Below this ratio of comm-to-compute time we treat the workflow as
     *  effectively communication-free. 0.05 picked from blast / cycles
     *  examples where actual commIntensity is &lt; 0.001. */
    private static final double COMM_INTENSITY_THRESHOLD = 0.05;

    /** Deterministic shuffle seed for reproducibility. */
    private static final long SHUFFLE_SEED = 42L;

    /** Diagnostics — set in {@link #init()} so callers can log the decision. */
    public int dagWidth;
    public double commIntensity;
    public boolean distributedMode;

    public AdaptiveHeftScheduler(ArrayList<WorkflowComputingAppliance> computeArchitecture,
                                 Instance instance,
                                 ArrayList<Actuator> actuatorArchitecture,
                                 Pair<String, ArrayList<WorkflowJob>> jobs) {
        super(computeArchitecture, instance, actuatorArchitecture, jobs);
    }

    @Override
    public void init() {
        super.init();   // builds DAG, ranks, HEFT-DS assignment, enqueues roots

        dagWidth = computeMaxConcurrency();
        double totalCompute = sumRuntimes();
        double totalDataBytes = sumDataBytes();
        commIntensity = (totalDataBytes / commRate) / Math.max(totalCompute, 1.0);
        distributedMode = dagWidth >= computeArchitecture.size()
                && commIntensity < COMM_INTENSITY_THRESHOLD;

        SimLogger.logRes(String.format(
                "Adaptive-HEFT [%s] width=%d nodes=%d commIntensity=%.5f -> %s",
                this.appName, dagWidth, computeArchitecture.size(), commIntensity,
                distributedMode ? "distributed (round-robin)" : "HEFT-DS (EFT-greedy)"));

        if (distributedMode) {
            redistributeRoundRobin();
        }
    }

    /**
     * Replace the HEFT-DS static assignment with a round-robin one. Every
     * node gets the same number of tasks (modulo the leftover); cycle order
     * is randomised by a deterministic shuffle so adjacent task IDs do not
     * cluster onto adjacent nodes.
     */
    private void redistributeRoundRobin() {
        for (WorkflowJob job : this.jobs) {
            if (job.ca != null && job.ca.workflowQueue != null) {
                job.ca.workflowQueue.remove(job);
            }
        }

        List<WorkflowJob> shuffled = new ArrayList<>(this.jobs);
        Collections.shuffle(shuffled, new Random(SHUFFLE_SEED));

        Map<String, WorkflowComputingAppliance> newAssignment = new HashMap<>();
        int nodeIdx = 0;
        for (WorkflowJob job : shuffled) {
            WorkflowComputingAppliance target = this.computeArchitecture.get(nodeIdx);
            nodeIdx = (nodeIdx + 1) % this.computeArchitecture.size();

            job.ca = target;
            newAssignment.put(job.id, target);

            if (job.inputs.get(0).amount == 0) {
                target.workflowQueue.add(job);
            }
        }
        this.staticAssignment = newAssignment;
    }

    /**
     * In distributed mode the dynamic phase must obey the capacity-weighted
     * static assignment, otherwise the EFT-greedy logic inherited from
     * {@link HeftScheduler#findBestProcessor} will re-pile everything onto
     * the fastest cloud nodes. In normal mode we defer to the HEFT-DS parent.
     */
    @Override
    protected WorkflowComputingAppliance findBestProcessor(WorkflowJob job) {
        if (distributedMode) {
            WorkflowComputingAppliance assigned = this.staticAssignment.get(job.id);
            if (assigned != null) {
                return assigned;
            }
        }
        return super.findBestProcessor(job);
    }

    // ============================================================
    //  DAG analysis helpers
    // ============================================================

    /**
     * Max number of tasks that can run concurrently in any topological level.
     * A task's level is one more than the max level of its predecessors; the
     * width is the size of the largest level.
     */
    private int computeMaxConcurrency() {
        Map<String, Integer> levels = new HashMap<>();
        for (WorkflowJob job : this.jobs) {
            assignLevel(job.id, levels);
        }
        Map<Integer, Integer> sizes = new HashMap<>();
        for (int lvl : levels.values()) {
            sizes.merge(lvl, 1, Integer::sum);
        }
        int max = 1;
        for (int s : sizes.values()) {
            if (s > max) {
                max = s;
            }
        }
        return max;
    }

    private int assignLevel(String jobId, Map<String, Integer> levels) {
        Integer cached = levels.get(jobId);
        if (cached != null) {
            return cached;
        }
        int maxPredLevel = -1;
        for (String predId : this.predecessors.getOrDefault(jobId, Collections.emptyList())) {
            maxPredLevel = Math.max(maxPredLevel, assignLevel(predId, levels));
        }
        int lvl = maxPredLevel + 1;
        levels.put(jobId, lvl);
        return lvl;
    }

    private double sumRuntimes() {
        double s = 0;
        for (WorkflowJob job : this.jobs) {
            s += job.runtime;
        }
        return s;
    }

    private double sumDataBytes() {
        long s = 0;
        for (Map<String, Long> edges : this.commData.values()) {
            for (long bytes : edges.values()) {
                s += bytes;
            }
        }
        return s;
    }
}
