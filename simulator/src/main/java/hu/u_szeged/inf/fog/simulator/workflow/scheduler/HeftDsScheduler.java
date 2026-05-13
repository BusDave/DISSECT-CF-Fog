package hu.u_szeged.inf.fog.simulator.workflow.scheduler;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.u_szeged.inf.fog.simulator.iot.Actuator;
import hu.u_szeged.inf.fog.simulator.node.WorkflowComputingAppliance;
import hu.u_szeged.inf.fog.simulator.provider.Instance;
import hu.u_szeged.inf.fog.simulator.util.SimLogger;
import hu.u_szeged.inf.fog.simulator.workflow.WorkflowJob;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Energy-aware HEFT scheduler (HEFT-DS) following Sun, Cao, Lu (2022) more
 * faithfully than the first version.
 *
 * <p>Implements the paper's formulas directly:
 * <ul>
 *   <li>Formula (9):  SlackT  = DLT - max{EFT_i}</li>
 *   <li>Formula (10): subDLT_i = min{EFT_i,j} + SlackT / deepmax</li>
 *   <li>Formula (11): MIPS_min = MIPS_max * ET_i / (subDLT_i - EST_i)</li>
 * </ul>
 *
 * <p>Algorithm structure (matches Table 4 pseudocode):
 * <ol>
 *   <li>Inherit DVR-HEFT static schedule (rank + EFT per task) from {@link HeftScheduler}.</li>
 *   <li>Identify the critical path: longest rank-chain from the heaviest root.</li>
 *   <li>Compute deepmax = maximum depth of the DAG (longest dependency chain).</li>
 *   <li>For each task: compute subDLT_i using its static EFT plus per-level slack.</li>
 *   <li>Override {@link #findBestProcessor}: critical → fastest available VM;
 *       non-critical → slowest VM whose MIPS >= MIPS_min (so the task still
 *       finishes within subDLT but draws less power).</li>
 *   <li>Task merging: nodes that ended up with a single statically-assigned
 *       task get their workload migrated to the fastest already-loaded node,
 *       so the lonely node can stay idle / be shut down.</li>
 * </ol>
 */
public class HeftDsScheduler extends HeftScheduler {

    /** Tasks identified as being on the critical path. */
    protected Set<String> criticalPath;

    /** Per-task sub-deadline (absolute finish time, seconds). */
    protected Map<String, Double> subDeadlines;

    /** Global deadline = winning DVR-HEFT makespan, with a small slack factor. */
    protected double globalDeadline;

    /** Maximum DAG depth (longest source-to-sink path in number of nodes). */
    protected int deepmax;

    /** Per-task DAG depth (longest path from any source to this task). */
    protected Map<String, Integer> taskDepth;

    /**
     * Slack factor applied to the deadline. The paper assumes a user-supplied
     * deadline notably larger than the HEFT optimum; we don't have one here,
     * so we synthesise it as {@code winningMakespan * DEADLINE_SLACK}.
     *
     * <p>A larger value gives the non-critical tasks more room to land on
     * slower (lower-power) nodes via Formula 10's subDLT distribution.
     * Too small (e.g. 1.10) → subDLT collapses onto the static EFT → every
     * non-critical task is forced back onto the fastest VM. 1.5 = +50% slack
     * over the optimistic HEFT makespan, which leaves usable per-level slack.
     */
    private static final double DEADLINE_SLACK = 1.10;

    /**
     * Threshold for task-merging: nodes with at most this many tasks get
     * drained. Set to a negative value to disable merging entirely on small
     * workflows where consolidation creates more contention than it saves.
     */
    private static final int MERGE_THRESHOLD = 1;

    /**
     * Skip task merging when the workload is sparse — specifically, when the
     * job-to-node ratio is below this value, merging tends to over-concentrate
     * onto a few fast nodes and increase makespan / energy. Tuned empirically.
     */
    private static final double MERGE_MIN_JOBS_PER_NODE = 8.0;

    public HeftDsScheduler(ArrayList<WorkflowComputingAppliance> computeArchitecture,
                           Instance instance,
                           ArrayList<Actuator> actuatorArchitecture,
                           Pair<String, ArrayList<WorkflowJob>> jobs) {
        super(computeArchitecture, instance, actuatorArchitecture, jobs);
    }

    @Override
    public void init() {
        super.init();

        this.criticalPath = identifyCriticalPath();
        this.taskDepth = computeTaskDepths();
        this.deepmax = taskDepth.values().stream().mapToInt(Integer::intValue).max().orElse(1);

        double winningMakespan = this.variantMakespans[this.winningVariant];
        this.globalDeadline = winningMakespan * DEADLINE_SLACK;
        this.subDeadlines = computeSubDeadlines();

        int merged = shouldMerge() ? mergeSingleTaskNodes() : 0;

        SimLogger.logRes(String.format(
                "HEFT-DS [%s] critical-path=%d/%d  deepmax=%d  deadline=%.3f  merged=%d",
                this.appName, this.criticalPath.size(), this.jobs.size(),
                this.deepmax, this.globalDeadline, merged));

        // Re-apply static assignments to job.ca (task merging may have changed them)
        for (WorkflowJob job : this.jobs) {
            WorkflowComputingAppliance assigned = staticAssignment.get(job.id);
            if (assigned != null) {
                job.ca = assigned;
            }
        }
    }

    // ================================================================
    //  Critical path identification
    // ================================================================

    /**
     * Greedy walk: start from the highest-rank root, follow the highest-rank
     * successor until reaching a sink. Single longest expected execution chain.
     */
    protected Set<String> identifyCriticalPath() {
        Set<String> path = new HashSet<>();
        String current = null;
        double maxRank = -1;
        for (WorkflowJob job : this.jobs) {
            if (predecessors.get(job.id).isEmpty()) {
                double r = rankUp.getOrDefault(job.id, 0.0);
                if (r > maxRank) {
                    maxRank = r;
                    current = job.id;
                }
            }
        }
        while (current != null) {
            path.add(current);
            String next = null;
            double bestRank = -1;
            for (String succ : successors.get(current)) {
                double r = rankUp.getOrDefault(succ, 0.0);
                if (r > bestRank) {
                    bestRank = r;
                    next = succ;
                }
            }
            current = next;
        }
        return path;
    }

    // ================================================================
    //  DAG depth (deepmax) — paper Formula (10)
    // ================================================================

    /**
     * depth(task) = length of the longest path from any source down to this
     * task (in number of nodes). The global deepmax is then the maximum of
     * all per-task depths.
     */
    protected Map<String, Integer> computeTaskDepths() {
        Map<String, Integer> depth = new HashMap<>();
        for (WorkflowJob job : this.jobs) {
            depthRecursive(job.id, depth);
        }
        return depth;
    }

    private int depthRecursive(String jobId, Map<String, Integer> memo) {
        if (memo.containsKey(jobId)) {
            return memo.get(jobId);
        }
        int max = 0;
        for (String pred : predecessors.get(jobId)) {
            max = Math.max(max, depthRecursive(pred, memo));
        }
        int d = max + 1;
        memo.put(jobId, d);
        return d;
    }

    // ================================================================
    //  Sub-deadlines — paper Formula (9) + (10)
    // ================================================================

    /**
     * Hybrid sub-deadline formulation adapted for heterogeneous fog/cloud
     * resources.
     *
     * <p><b>Paper Formula (10):</b> {@code subDLT_i = min{EFT_i,j} + SlackT / deepmax}
     * distributes the global slack evenly across DAG levels. This works on
     * Sun et al.'s homogeneous-VM + DVFS setup, but in our heterogeneous
     * cluster a "uniform per-level slack" is too tight: a non-critical task
     * routed to a 4-core fog node takes ~13x longer than on the 52-core
     * cloud, so the small per-level allowance can never accommodate the
     * slowdown. Every non-critical task then falls back to the fastest VM,
     * defeating the purpose of energy-aware placement.
     *
     * <p><b>Our variant:</b> we keep Formula (9)'s SlackT and deepmax as
     * diagnostics, but distribute slack <i>by rank position</i>:
     * <pre>
     *   subDLT_i = globalDeadline * (1 - rank_up(i) / max(rank_up))
     * </pre>
     * High-rank tasks (close to the source / on the critical path) get
     * little slack; low-rank tasks (near the sinks) get most of the
     * deadline, so they fit on slower fog nodes. This matches the spirit
     * of the paper (more slack → can use slower / lower-power resource)
     * but adapts to heterogeneous hardware where slowdown is multiplicative
     * rather than DVFS-tunable.
     *
     * <p>Floor: each task gets at least {@code 1.5 * worst-case ET} so the
     * eligible set never collapses to the empty set on slow nodes.
     */
    protected Map<String, Double> computeSubDeadlines() {
        Map<String, Double> map = new HashMap<>();

        double maxRank = 0;
        for (double r : rankUp.values()) {
            if (r > maxRank) {
                maxRank = r;
            }
        }
        if (maxRank <= 0) {
            for (WorkflowJob job : this.jobs) {
                map.put(job.id, this.globalDeadline);
            }
            return map;
        }

        for (WorkflowJob job : this.jobs) {
            double r = rankUp.getOrDefault(job.id, 0.0);
            double subDlt = this.globalDeadline * (1.0 - r / maxRank);
            // Floor at worst-case ET so the candidate set is never empty.
            double maxExec = 0;
            for (WorkflowComputingAppliance ca : this.computeArchitecture) {
                maxExec = Math.max(maxExec, executionTime(job, ca));
            }
            map.put(job.id, Math.max(subDlt, maxExec * 1.5));
        }
        return map;
    }

    // ================================================================
    //  Energy-aware processor selection — paper Formula (11)
    // ================================================================

    @Override
    protected WorkflowComputingAppliance findBestProcessor(WorkflowJob job) {
        if (criticalPath != null && criticalPath.contains(job.id)) {
            // Critical path → vanilla HEFT EFT-minimisation.
            return super.findBestProcessor(job);
        }
        return findEnergyEfficientNode(job);
    }

    /**
     * Paper Formula (11): MIPS_min = MIPS_max * ET_i / (subDLT_i - EST_i)
     *
     * <p>For each candidate node, compute the actual EST (max of parents'
     * finish + transfer), execution time on that node, and EFT. A node is
     * eligible if its EFT <= subDLT_i. Among eligible nodes, prefer the one
     * with the smallest MIPS (slowest, lowest-power).
     */
    protected WorkflowComputingAppliance findEnergyEfficientNode(WorkflowJob job) {
        double subDlt = subDeadlines.getOrDefault(job.id, globalDeadline);

        WorkflowComputingAppliance bestNode = null;
        double bestMips = Double.MAX_VALUE;
        // Tie-breaker among nodes with equal MIPS: prefer the one with lower
        // estimated finish (= less-loaded). Tracked SEPARATELY from fallbackEft
        // so it stays scoped to "eligible-and-best-MIPS so far" — comparing to
        // the global minimum (fallback) would always favour the fastest VM and
        // pin all traffic to the first low-MIPS node in iteration order.
        double bestFitEft = Double.MAX_VALUE;

        WorkflowComputingAppliance fallbackNode = null;
        double fallbackEft = Double.MAX_VALUE;

        for (WorkflowComputingAppliance ca : this.computeArchitecture) {
            int queueSize = ca.workflowQueue.size();
            int running = 0;
            for (VirtualMachine vm : ca.iaas.listVMs()) {
                if (vm.getState().equals(VirtualMachine.State.RUNNING)) {
                    running += vm.underProcessing.size();
                }
            }

            double et = executionTime(job, ca);
            double commCost = computeCommCost(job, ca);
            // Runtime EFT estimate: wait for queued + running tasks, then run
            // this task, plus the data transfer from any non-local predecessor.
            double eft = (queueSize + running) * et + et + commCost;

            double mips = nodeMips(ca);

            if (eft <= subDlt) {
                // Eligible: choose slowest VM (lowest MIPS) that meets subDLT.
                // Tie-break among equal-MIPS nodes by least-loaded (lowest EFT).
                if (mips < bestMips
                        || (mips == bestMips && eft < bestFitEft)) {
                    bestMips = mips;
                    bestFitEft = eft;
                    bestNode = ca;
                }
            }

            // Fallback: track minimum-EFT node in case none meets subDLT.
            if (eft < fallbackEft) {
                fallbackEft = eft;
                fallbackNode = ca;
            }
        }

        if (bestNode != null) {
            return bestNode;
        }
        return fallbackNode != null ? fallbackNode : this.computeArchitecture.get(0);
    }

    private double computeCommCost(WorkflowJob job, WorkflowComputingAppliance ca) {
        double commCost = 0;
        for (String predId : predecessors.get(job.id)) {
            WorkflowJob pred = jobMap.get(predId);
            if (pred != null && pred.ca != ca) {
                long dataSize = commData.get(predId).getOrDefault(job.id, 0L);
                commCost += dataSize / commRate;
            }
        }
        return commCost;
    }

    /**
     * EST for a job on candidate node ca, using the winning HEFT schedule's
     * predecessor finish times and the candidate node's local data status.
     */
    private double computeEst(WorkflowJob job, WorkflowComputingAppliance ca) {
        double est = 0;
        for (String predId : predecessors.get(job.id)) {
            double predFinish = staticJobEft.getOrDefault(predId, 0.0);
            WorkflowComputingAppliance predCa = staticAssignment.get(predId);
            double comm = 0;
            if (predCa != null && predCa != ca) {
                long dataSize = commData.get(predId).getOrDefault(job.id, 0L);
                comm = dataSize / commRate;
            }
            est = Math.max(est, predFinish + comm);
        }
        return est;
    }

    private double nodeMips(WorkflowComputingAppliance ca) {
        return ca.iaas.getCapacities().getRequiredCPUs()
                * ca.iaas.getCapacities().getRequiredProcessingPower();
    }

    // ================================================================
    //  Task merging — paper Table 4, lines 19-28
    // ================================================================

    /**
     * Identify nodes that ended up with at most {@link #MERGE_THRESHOLD} tasks
     * in the static schedule, and migrate those tasks to the fastest node that
     * already has work. This consolidates load onto fewer nodes so the
     * "lonely" nodes can stay idle (and be shut down via auto-scaling).
     *
     * <p>A task is only migrated if the new node can still satisfy its
     * sub-deadline.
     *
     * @return number of tasks migrated.
     */
    /**
     * Merging only helps when there are enough non-critical tasks to keep
     * the merge targets busy without piling up. With few tasks per node,
     * draining a node forces extra contention on the targets.
     */
    private boolean shouldMerge() {
        if (MERGE_THRESHOLD < 0) {
            return false;
        }
        double ratio = (double) this.jobs.size() / this.computeArchitecture.size();
        return ratio >= MERGE_MIN_JOBS_PER_NODE;
    }

    protected int mergeSingleTaskNodes() {
        // Count tasks per node in the static schedule.
        Map<WorkflowComputingAppliance, List<String>> tasksByNode = new HashMap<>();
        for (WorkflowComputingAppliance ca : this.computeArchitecture) {
            tasksByNode.put(ca, new ArrayList<>());
        }
        for (Map.Entry<String, WorkflowComputingAppliance> e : staticAssignment.entrySet()) {
            List<String> list = tasksByNode.get(e.getValue());
            if (list != null) {
                list.add(e.getKey());
            }
        }

        // Identify sparse nodes (candidates for draining) and busy nodes
        // sorted by descending MIPS (preferred merge targets).
        List<WorkflowComputingAppliance> busyNodes = new ArrayList<>();
        List<WorkflowComputingAppliance> sparseNodes = new ArrayList<>();
        for (WorkflowComputingAppliance ca : this.computeArchitecture) {
            int count = tasksByNode.get(ca).size();
            if (count == 0) {
                continue;
            }
            if (count <= MERGE_THRESHOLD) {
                sparseNodes.add(ca);
            } else {
                busyNodes.add(ca);
            }
        }
        busyNodes.sort((a, b) -> Double.compare(nodeMips(b), nodeMips(a)));

        if (busyNodes.isEmpty() || sparseNodes.isEmpty()) {
            return 0;
        }

        int migrated = 0;
        for (WorkflowComputingAppliance sparseCa : sparseNodes) {
            for (String taskId : new ArrayList<>(tasksByNode.get(sparseCa))) {
                WorkflowJob job = jobMap.get(taskId);
                if (job == null) {
                    continue;
                }
                // Don't migrate critical-path tasks.
                if (criticalPath.contains(taskId)) {
                    continue;
                }
                // Pick the first busy node where the task still fits.
                double subDlt = subDeadlines.getOrDefault(taskId, globalDeadline);
                for (WorkflowComputingAppliance target : busyNodes) {
                    if (target == sparseCa) {
                        continue;
                    }
                    double est = computeEst(job, target);
                    double eft = est + executionTime(job, target);
                    if (eft <= subDlt) {
                        staticAssignment.put(taskId, target);
                        tasksByNode.get(sparseCa).remove(taskId);
                        tasksByNode.get(target).add(taskId);
                        // Update EFT map so subsequent decisions stay coherent.
                        staticJobEft.put(taskId, eft);
                        migrated++;
                        break;
                    }
                }
            }
        }
        return migrated;
    }
}
