package hu.u_szeged.inf.fog.simulator.workflow.scheduler.training;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.u_szeged.inf.fog.simulator.iot.Actuator;
import hu.u_szeged.inf.fog.simulator.node.WorkflowComputingAppliance;
import hu.u_szeged.inf.fog.simulator.provider.Instance;
import hu.u_szeged.inf.fog.simulator.workflow.WorkflowJob;
import hu.u_szeged.inf.fog.simulator.workflow.scheduler.MaxMinScheduler;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;

/**
 * MaxMin scheduler with per-decision logging for imitation learning.
 *
 * <p>Unlike the HEFT family, the base {@link MaxMinScheduler} does not build a
 * DAG nor compute upward ranks. To stay compatible with the
 * {@link SchedulerFeatureExtractor} schema (which expects {@code rank_up},
 * predecessors, successors and inter-task data sizes), we reconstruct those
 * here. The same constant comm-rate used by HEFT lets us produce a
 * comparable {@code rank_up} value, so cross-scheduler training data uses a
 * uniform feature space.
 *
 * <p>Decisions are logged from {@link #schedule(WorkflowJob)} so that the
 * state captured for each task reflects what the scheduler sees when the task
 * actually becomes ready — not the all-zero state at {@code init()} time.
 * MaxMin's choice is the round-robin static assignment made by the parent's
 * {@code init()}, so we simply read {@code workflowJob.ca} after the parent's
 * scheduling step.
 */
public class MaxMinSchedulerWithLogging extends MaxMinScheduler {

    /** Same default as {@link hu.u_szeged.inf.fog.simulator.workflow.scheduler.HeftScheduler}
     *  so the {@code rank_up} feature is on the same scale across schedulers. */
    private static final double DEFAULT_COMM_RATE = 62_500_000.0;

    private final TrainingLogger logger;

    private Map<String, WorkflowJob> jobMap;
    private Map<String, List<String>> successors;
    private Map<String, List<String>> predecessors;
    private Map<String, Map<String, Long>> commData;
    private Map<String, Double> rankUp;

    public MaxMinSchedulerWithLogging(ArrayList<WorkflowComputingAppliance> computeArchitecture,
                                      Instance instance,
                                      ArrayList<Actuator> actuatorArchitecture,
                                      Pair<String, ArrayList<WorkflowJob>> jobs) {
        super(computeArchitecture, instance, actuatorArchitecture, jobs);
        this.logger = new TrainingLogger(jobs.getLeft(), "maxmin");
    }

    @Override
    public void init() {
        super.init();
        // After super.init() every job has its round-robin .ca; now we build
        // the auxiliary DAG / rank maps the feature extractor needs.
        buildJobGraph();
        rankUp = computeUpwardRank();
    }

    @Override
    public void schedule(WorkflowJob workflowJob) {
        double elapsed = Timed.getFireCount() / 1000.0;
        int totalTasks = this.jobs.size();
        int tasksDone = logger.getDecisionCount();

        double[] features = SchedulerFeatureExtractor.extract(
                workflowJob, rankUp, predecessors, successors, commData,
                this.computeArchitecture, tasksDone, totalTasks, elapsed);

        super.schedule(workflowJob);   // executes the round-robin assignment

        int nodeIdx = workflowJob.ca == null
                ? -1
                : this.computeArchitecture.indexOf(workflowJob.ca);
        if (nodeIdx >= 0) {
            logger.logDecision(workflowJob.id, features, nodeIdx);
        }
    }

    public void finishLogging(double makespanSeconds, double energyKwh) {
        logger.finish(makespanSeconds, energyKwh,
                this.jobs.size(), this.computeArchitecture.size());
    }

    public TrainingLogger getLogger() {
        return logger;
    }

    // ================================================================
    //  DAG construction (mirrors HeftScheduler.buildJobGraph)
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

    /**
     * Upward rank using the avg-computation-cost variant of HEFT. Recreates
     * just enough of HeftScheduler's logic to give the feature extractor a
     * comparable {@code rank_up} value.
     */
    private Map<String, Double> computeUpwardRank() {
        Map<String, Double> rank = new HashMap<>();
        Set<String> computed = new HashSet<>();
        for (WorkflowJob job : this.jobs) {
            rankRecursive(job.id, rank, computed);
        }
        return rank;
    }

    private double rankRecursive(String jobId, Map<String, Double> rank, Set<String> computed) {
        if (computed.contains(jobId)) {
            return rank.get(jobId);
        }
        WorkflowJob job = jobMap.get(jobId);
        double compCost = avgExecutionTime(job);
        int numProcs = this.computeArchitecture.size();

        double maxSucc = 0;
        for (String succId : successors.get(jobId)) {
            double succRank = rankRecursive(succId, rank, computed);
            long dataSize = commData.get(jobId).getOrDefault(succId, 0L);
            double avgComm = (dataSize / DEFAULT_COMM_RATE) * (numProcs - 1.0) / numProcs;
            maxSucc = Math.max(maxSucc, avgComm + succRank);
        }
        double r = compCost + maxSucc;
        rank.put(jobId, r);
        computed.add(jobId);
        return r;
    }

    private double avgExecutionTime(WorkflowJob job) {
        double instanceMips = this.instance.arc.getRequiredCPUs()
                * this.instance.arc.getRequiredProcessingPower();
        double sum = 0;
        int n = 0;
        for (WorkflowComputingAppliance ca : this.computeArchitecture) {
            double nodeMips = ca.iaas.getCapacities().getRequiredCPUs()
                    * ca.iaas.getCapacities().getRequiredProcessingPower();
            if (nodeMips > 0) {
                sum += job.runtime * instanceMips / nodeMips;
                n++;
            }
        }
        return n == 0 ? job.runtime : sum / n;
    }
}
