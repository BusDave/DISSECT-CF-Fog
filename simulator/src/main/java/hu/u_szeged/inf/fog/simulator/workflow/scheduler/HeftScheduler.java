package hu.u_szeged.inf.fog.simulator.workflow.scheduler;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager.VMManagementException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.u_szeged.inf.fog.simulator.iot.Actuator;
import hu.u_szeged.inf.fog.simulator.node.WorkflowComputingAppliance;
import hu.u_szeged.inf.fog.simulator.provider.Instance;
import hu.u_szeged.inf.fog.simulator.util.SimLogger;
import hu.u_szeged.inf.fog.simulator.workflow.WorkflowJob;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;

/**
 * HEFT (Heterogeneous Earliest Finish Time) scheduler with DVR-HEFT improvements.
 *
 * <p>Based on:
 * <ul>
 *   <li>Topcuoglu et al. (2002) - original HEFT algorithm</li>
 *   <li>Sandokji and Eassa (2019) - Dynamic Variant Rank HEFT (DVR-HEFT):
 *       three rank computation schemes, picks the schedule with minimum makespan,
 *       and dynamic runtime task assignment</li>
 *   <li>Sun, Cao and Lu (2022) - HEFT-DS: energy-aware improvements with
 *       critical path analysis and data locality</li>
 * </ul>
 *
 * <p>The algorithm has two phases:
 * <ol>
 *   <li><b>Static phase ({@link #init}):</b> Builds the task dependency DAG,
 *       computes upward ranks using three variant schemes (average, best-case,
 *       worst-case computation cost), generates a full HEFT schedule for each
 *       variant, and selects the one with the minimum makespan.</li>
 *   <li><b>Dynamic phase ({@link #schedule}):</b> When a task becomes ready at
 *       runtime, re-evaluates the best processor considering current node load
 *       and data locality (communication cost from predecessors).</li>
 * </ol>
 */
public class HeftScheduler extends WorkflowScheduler {

    /** Default estimated network bandwidth: 62.5 MB/s (bytes per second). */
    private static final double DEFAULT_COMM_RATE = 62_500_000.0;

    protected final double commRate;

    // Job lookup by ID
    protected Map<String, WorkflowJob> jobMap;

    // Dependency graph
    protected Map<String, List<String>> successors;
    protected Map<String, List<String>> predecessors;

    // Data transfer sizes: parentId -> childId -> bytes
    protected Map<String, Map<String, Long>> commData;

    // Upward rank values (from the winning variant)
    protected Map<String, Double> rankUp;

    // Static HEFT assignment: jobId -> assigned node
    protected Map<String, WorkflowComputingAppliance> staticAssignment;

    // Earliest Finish Time from the winning variant's static schedule
    // (jobId -> EFT in seconds). Used by HEFT-DS for sub-deadline computation.
    protected Map<String, Double> staticJobEft;

    /** DVR-HEFT diagnostics: makespan per variant (index = variant id). */
    public double[] variantMakespans;

    /** DVR-HEFT diagnostics: winning variant id (0=avg, 1=min, 2=max). */
    public int winningVariant;

    private static final String[] VARIANT_NAMES = {"avg", "min", "max"};

    public HeftScheduler(ArrayList<WorkflowComputingAppliance> computeArchitecture,
                         Instance instance,
                         ArrayList<Actuator> actuatorArchitecture,
                         Pair<String, ArrayList<WorkflowJob>> jobs) {
        this(computeArchitecture, instance, actuatorArchitecture, jobs, DEFAULT_COMM_RATE);
    }

    public HeftScheduler(ArrayList<WorkflowComputingAppliance> computeArchitecture,
                         Instance instance,
                         ArrayList<Actuator> actuatorArchitecture,
                         Pair<String, ArrayList<WorkflowJob>> jobs,
                         double commRate) {
        this.computeArchitecture = computeArchitecture;
        this.instance = instance;
        this.actuatorArchitecture = actuatorArchitecture;
        this.jobs = jobs.getRight();
        this.appName = jobs.getLeft();
        this.commRate = commRate;
        WorkflowScheduler.schedulers.add(this);
    }

    //  Scheduler interface

    @Override
    public void init() {
        // 1. Build dependency graph from job inputs/outputs
        buildJobGraph();

        // 2. DVR-HEFT: compute 3 rank variants, pick best schedule
        computeRanksAndSchedule();

        // 3. Initialise computing appliances with HEFT priority queue
        Comparator<WorkflowJob> heftComparator = (o1, o2) -> {
            double r1 = rankUp.getOrDefault(o1.id, 0.0);
            double r2 = rankUp.getOrDefault(o2.id, 0.0);
            return Double.compare(r2, r1); // higher rank = higher priority
        };

        for (WorkflowComputingAppliance ca : this.computeArchitecture) {
            ca.workflowQueue = new PriorityQueue<>(heftComparator);
            ca.iaas.repositories.get(0).registerObject(this.instance.va);
            try {
                ca.workflowVms.add(ca.iaas.requestVM(this.instance.va, this.instance.arc,
                        ca.iaas.repositories.get(0), 1)[0]);
            } catch (VMManagementException e) {
                e.printStackTrace();
            }
        }

        // 4. Apply static assignment; enqueue root jobs (no dependencies)
        for (WorkflowJob job : this.jobs) {
            WorkflowComputingAppliance assigned = staticAssignment.get(job.id);
            job.ca = assigned != null ? assigned : this.computeArchitecture.get(0);
            if (job.inputs.get(0).amount == 0) {
                job.ca.workflowQueue.add(job);
            }
        }
    }

    @Override
    public void schedule(WorkflowJob workflowJob) {
        if (workflowJob.inputs.get(0).amount == 0) {
            // Dynamic phase: find the best processor at runtime
            WorkflowComputingAppliance bestNode = findBestProcessor(workflowJob);
            workflowJob.ca = bestNode;
            bestNode.workflowQueue.add(workflowJob);
        }

        // VM auto-scaling (same policy as MaxMinScheduler)
        int jobCount = 0;
        for (VirtualMachine vm : workflowJob.ca.iaas.listVMs()) {
            jobCount += vm.underProcessing.size();
        }
        if (jobCount > workflowJob.ca.iaas.listVMs().size()) {
            this.addVm(workflowJob.ca, 1);
        } else {
            this.shutdownVm(workflowJob.ca, 1);
        }
    }

    // ================================================================
    //  1. Graph construction
    // ================================================================

    private void buildJobGraph() {
        jobMap = new HashMap<>();
        successors = new HashMap<>();
        predecessors = new HashMap<>();
        commData = new HashMap<>();

        for (WorkflowJob job : this.jobs) {
            jobMap.put(job.id, job);
            successors.put(job.id, new ArrayList<>());
            predecessors.put(job.id, new ArrayList<>());
            commData.put(job.id, new HashMap<>());
        }

        for (WorkflowJob job : this.jobs) {
            for (WorkflowJob.Uses uses : job.outputs) {
                if (uses.type == WorkflowJob.Uses.Type.DATA && jobMap.containsKey(uses.id)) {
                    successors.get(job.id).add(uses.id);
                    predecessors.get(uses.id).add(job.id);
                    commData.get(job.id).put(uses.id, uses.size);
                }
            }
        }
    }

    // ================================================================
    //  2. DVR-HEFT: variant rank computation + best schedule selection
    // ================================================================

    /**
     * DVR-HEFT core: compute upward ranks with 3 variant schemes, generate a
     * full HEFT schedule for each, and keep the one with minimum makespan.
     *
     * <p>Variant schemes for computation cost f(Wi):
     * <ul>
     *   <li>0 - average: mean execution time across processors</li>
     *   <li>1 - best case: minimum execution time</li>
     *   <li>2 - worst case: maximum execution time</li>
     * </ul>
     * In a homogeneous setup all three yield the same value, but the rank
     * propagation through different DAG paths can still produce different
     * task orderings, leading to different schedules.
     */
    private void computeRanksAndSchedule() {
        @SuppressWarnings("unchecked")
        Map<String, Double>[] rankVariants = new Map[3];
        @SuppressWarnings("unchecked")
        Map<String, WorkflowComputingAppliance>[] scheduleVariants = new Map[3];
        @SuppressWarnings("unchecked")
        Map<String, Double>[] eftVariants = new Map[3];
        double[] makespans = new double[3];

        for (int v = 0; v < 3; v++) {
            rankVariants[v] = computeUpwardRank(v);
            Object[] result = simulateHeftSchedule(rankVariants[v]);
            scheduleVariants[v] = castAssignment(result[0]);
            makespans[v] = (double) result[1];
            eftVariants[v] = castEft(result[2]);
        }

        // Pick the variant with the smallest makespan
        int best = 0;
        for (int v = 1; v < 3; v++) {
            if (makespans[v] < makespans[best]) {
                best = v;
            }
        }

        this.rankUp = rankVariants[best];
        this.staticAssignment = scheduleVariants[best];
        this.staticJobEft = eftVariants[best];
        this.variantMakespans = makespans;
        this.winningVariant = best;

        SimLogger.logRes(String.format(
                "DVR-HEFT [%s] makespans  avg=%.3f  min=%.3f  max=%.3f  winner=%s",
                this.appName, makespans[0], makespans[1], makespans[2], VARIANT_NAMES[best]));
    }

    @SuppressWarnings("unchecked")
    private Map<String, WorkflowComputingAppliance> castAssignment(Object obj) {
        return (Map<String, WorkflowComputingAppliance>) obj;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Double> castEft(Object obj) {
        return (Map<String, Double>) obj;
    }

    // ================================================================
    //  3. Upward rank calculation
    // ================================================================

    /**
     * rank_up(ni) = w(ni, variant) + max_{nj in succ(ni)} { c_avg(ni,nj) + rank_up(nj) }
     *
     * <p>Communication cost is averaged over processor pairs:
     * c_avg = dataSize / commRate * (P-1)/P  (probability of different processors).
     */
    private Map<String, Double> computeUpwardRank(int variant) {
        Map<String, Double> rank = new HashMap<>();
        Set<String> computed = new HashSet<>();
        for (WorkflowJob job : this.jobs) {
            rankRecursive(job.id, variant, rank, computed);
        }
        return rank;
    }

    private double rankRecursive(String jobId, int variant,
                                  Map<String, Double> rank, Set<String> computed) {
        if (computed.contains(jobId)) {
            return rank.get(jobId);
        }

        WorkflowJob job = jobMap.get(jobId);
        double compCost = computationCost(job, variant);
        int numProcs = this.computeArchitecture.size();

        double maxSucc = 0;
        for (String succId : successors.get(jobId)) {
            double succRank = rankRecursive(succId, variant, rank, computed);
            long dataSize = commData.get(jobId).getOrDefault(succId, 0L);
            double avgComm = (dataSize / commRate) * (numProcs - 1.0) / numProcs;
            maxSucc = Math.max(maxSucc, avgComm + succRank);
        }

        double r = compCost + maxSucc;
        rank.put(jobId, r);
        computed.add(jobId);
        return r;
    }

    /**
     * Heterogeneous execution time w(t_i, p_j): how long {@code job} runs on
     * {@code ca}. The reference runtime in the workflow XML is measured for
     * the configured Instance; on a node with more / faster cores the same
     * task finishes proportionally faster. Derived from the NOI formula in
     * {@link hu.u_szeged.inf.fog.simulator.workflow.WorkflowExecutor#execute}.
     */
    protected double executionTime(WorkflowJob job, WorkflowComputingAppliance ca) {
        double instanceMips = this.instance.arc.getRequiredCPUs()
                * this.instance.arc.getRequiredProcessingPower();
        // getCapacities() returns per-core values; aggregate via CPUs * PP so a
        // 52-core node ends up ~13x faster than a 4-core one for the same task.
        double nodeMips = ca.iaas.getCapacities().getRequiredCPUs()
                * ca.iaas.getCapacities().getRequiredProcessingPower();
        if (nodeMips <= 0) {
            return job.runtime;
        }
        return job.runtime * instanceMips / nodeMips;
    }

    /**
     * DVR variant computation cost across the heterogeneous node set:
     *   0 = average execution time across processors
     *   1 = best-case (minimum) execution time
     *   2 = worst-case (maximum) execution time
     */
    private double computationCost(WorkflowJob job, int variant) {
        double sum = 0;
        double min = Double.POSITIVE_INFINITY;
        double max = 0;
        for (WorkflowComputingAppliance ca : this.computeArchitecture) {
            double w = executionTime(job, ca);
            sum += w;
            if (w < min) {
                min = w;
            }
            if (w > max) {
                max = w;
            }
        }
        switch (variant) {
            case 1:
                return min;
            case 2:
                return max;
            default:
                return sum / this.computeArchitecture.size();
        }
    }

    // ================================================================
    //  4. Static HEFT scheduling simulation
    // ================================================================

    /**
     * For each task in rank-descending order, assign it to the processor
     * that yields the smallest Earliest Finish Time (EFT).
     *
     * <p>EFT(ni, pj) = EST(ni, pj) + w(ni, pj)
     * <br>EST(ni, pj) = max{ avail(pj), max_{nm in pred(ni)} { AFT(nm) + c(nm,ni,pj) } }
     * <br>c(nm,ni,pj) = 0 if nm was assigned to pj (local), dataSize/commRate otherwise.
     *
     * @return Object[]{ Map&lt;String, WorkflowComputingAppliance&gt; assignment, double makespan }
     */
    private Object[] simulateHeftSchedule(Map<String, Double> rank) {
        Map<String, WorkflowComputingAppliance> assignment = new HashMap<>();
        Map<String, Double> jobEft = new HashMap<>();
        Map<WorkflowComputingAppliance, Double> procAvail = new HashMap<>();

        for (WorkflowComputingAppliance ca : this.computeArchitecture) {
            procAvail.put(ca, 0.0);
        }

        // Sort by rank descending (highest priority first)
        List<WorkflowJob> sorted = new ArrayList<>(this.jobs);
        sorted.sort((a, b) -> Double.compare(
                rank.getOrDefault(b.id, 0.0),
                rank.getOrDefault(a.id, 0.0)));

        double makespan = 0;

        for (WorkflowJob job : sorted) {
            WorkflowComputingAppliance bestProc = null;
            double bestEft = Double.MAX_VALUE;

            for (WorkflowComputingAppliance ca : this.computeArchitecture) {
                // EST: processor must be free AND all predecessor data must arrive
                double est = procAvail.get(ca);

                for (String predId : predecessors.get(job.id)) {
                    double predFinish = jobEft.getOrDefault(predId, 0.0);
                    WorkflowComputingAppliance predProc = assignment.get(predId);
                    double comm = 0;
                    if (predProc != null && predProc != ca) {
                        long dataSize = commData.get(predId).getOrDefault(job.id, 0L);
                        comm = dataSize / commRate;
                    }
                    est = Math.max(est, predFinish + comm);
                }

                double eft = est + executionTime(job, ca);
                if (eft < bestEft) {
                    bestEft = eft;
                    bestProc = ca;
                }
            }

            assignment.put(job.id, bestProc);
            jobEft.put(job.id, bestEft);
            procAvail.put(bestProc, bestEft);
            makespan = Math.max(makespan, bestEft);
        }

        return new Object[]{assignment, makespan, jobEft};
    }

    // ================================================================
    //  5. Dynamic runtime scheduling (DVR-HEFT dynamic part)
    // ================================================================

    /**
     * At runtime, find the best processor for a ready job by considering:
     * <ol>
     *   <li>Current node workload (queue + running jobs)</li>
     *   <li>Data locality (communication cost from predecessor nodes)</li>
     *   <li>Static assignment as tie-breaker</li>
     * </ol>
     * This implements the dynamic phase of DVR-HEFT: tasks arriving at
     * runtime are mapped to the processor with the lowest estimated
     * completion time, and idle processors can be turned off.
     */
    protected WorkflowComputingAppliance findBestProcessor(WorkflowJob job) {
        WorkflowComputingAppliance bestNode = null;
        double bestScore = Double.MAX_VALUE;

        for (WorkflowComputingAppliance ca : this.computeArchitecture) {
            // Current workload
            int queueSize = ca.workflowQueue.size();
            int running = 0;
            for (VirtualMachine vm : ca.iaas.listVMs()) {
                if (vm.getState().equals(VirtualMachine.State.RUNNING)) {
                    running += vm.underProcessing.size();
                }
            }

            // Communication cost: penalty for data from predecessors on other nodes
            double commCost = 0;
            for (String predId : predecessors.get(job.id)) {
                WorkflowJob pred = jobMap.get(predId);
                if (pred != null && pred.ca != ca) {
                    long dataSize = commData.get(predId).getOrDefault(job.id, 0L);
                    commCost += dataSize / commRate;
                }
            }

            // Estimated completion time on this node (heterogeneous w_ij)
            double wij = executionTime(job, ca);
            double estimatedWait = (queueSize + running) * wij;
            double score = estimatedWait + wij + commCost;

            // Slight preference for the static HEFT assignment (tie-breaker)
            if (ca == staticAssignment.get(job.id)) {
                score -= 0.001;
            }

            if (score < bestScore) {
                bestScore = score;
                bestNode = ca;
            }
        }

        return bestNode != null ? bestNode : this.computeArchitecture.get(0);
    }
}
