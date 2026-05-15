package hu.u_szeged.inf.fog.simulator.workflow.scheduler.training;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.u_szeged.inf.fog.simulator.iot.Actuator;
import hu.u_szeged.inf.fog.simulator.node.WorkflowComputingAppliance;
import hu.u_szeged.inf.fog.simulator.provider.Instance;
import hu.u_szeged.inf.fog.simulator.workflow.WorkflowJob;
import hu.u_szeged.inf.fog.simulator.workflow.scheduler.AdaptiveHeftScheduler;
import java.util.ArrayList;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Adaptive HEFT scheduler with per-decision logging for imitation learning.
 *
 * <p>Mirrors {@link HeftSchedulerWithLogging}: roots are logged after
 * {@code super.init()} with their final assignment (which is either the
 * EFT-greedy HEFT-DS pick or the round-robin pick depending on whether the
 * Adaptive scheduler entered distributed mode); dynamic-phase decisions are
 * captured by overriding {@code findBestProcessor}.
 *
 * <p>The {@code rankUp} / {@code predecessors} / {@code successors} maps
 * needed by {@link SchedulerFeatureExtractor} are inherited via the HEFT
 * family, so no extra DAG analysis code is required here.
 */
public class AdaptiveHeftSchedulerWithLogging extends AdaptiveHeftScheduler {

    private final TrainingLogger logger;
    private boolean rootsLogged = false;

    public AdaptiveHeftSchedulerWithLogging(ArrayList<WorkflowComputingAppliance> computeArchitecture,
                                            Instance instance,
                                            ArrayList<Actuator> actuatorArchitecture,
                                            Pair<String, ArrayList<WorkflowJob>> jobs) {
        this(computeArchitecture, instance, actuatorArchitecture, jobs, null);
    }

    public AdaptiveHeftSchedulerWithLogging(ArrayList<WorkflowComputingAppliance> computeArchitecture,
                                            Instance instance,
                                            ArrayList<Actuator> actuatorArchitecture,
                                            Pair<String, ArrayList<WorkflowJob>> jobs,
                                            String workflowXmlPath) {
        super(computeArchitecture, instance, actuatorArchitecture, jobs);
        this.logger = new TrainingLogger(jobs.getLeft(), "adaptive", workflowXmlPath);
    }

    @Override
    public void init() {
        super.init();
        logRootTasks();
    }

    private void logRootTasks() {
        if (rootsLogged) {
            return;
        }
        rootsLogged = true;

        double elapsed = Timed.getFireCount() / 1000.0;
        int totalTasks = this.jobs.size();
        int tasksDone = 0;

        for (WorkflowJob job : this.jobs) {
            if (job.inputs.get(0).amount != 0) {
                continue;
            }
            WorkflowComputingAppliance assigned = this.staticAssignment.get(job.id);
            if (assigned == null) {
                continue;
            }
            int nodeIdx = this.computeArchitecture.indexOf(assigned);
            if (nodeIdx < 0) {
                continue;
            }
            double[] features = SchedulerFeatureExtractor.extract(
                    job, this.rankUp, this.predecessors, this.successors, this.commData,
                    this.computeArchitecture, tasksDone, totalTasks, elapsed);
            logger.logDecision(job.id, features, nodeIdx);
            tasksDone = logger.getDecisionCount();
        }
    }

    @Override
    protected WorkflowComputingAppliance findBestProcessor(WorkflowJob job) {
        double elapsed = Timed.getFireCount() / 1000.0;
        int totalTasks = this.jobs.size();
        int tasksDone = logger.getDecisionCount();

        double[] features = SchedulerFeatureExtractor.extract(
                job, this.rankUp, this.predecessors, this.successors, this.commData,
                this.computeArchitecture, tasksDone, totalTasks, elapsed);

        WorkflowComputingAppliance chosen = super.findBestProcessor(job);
        int nodeIdx = this.computeArchitecture.indexOf(chosen);
        if (nodeIdx >= 0) {
            logger.logDecision(job.id, features, nodeIdx);
        }
        return chosen;
    }

    public void finishLogging(double makespanSeconds, double energyKwh) {
        logger.finish(makespanSeconds, energyKwh,
                this.jobs.size(), this.computeArchitecture.size());
    }

    public TrainingLogger getLogger() {
        return logger;
    }
}
