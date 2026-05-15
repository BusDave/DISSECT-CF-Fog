package hu.u_szeged.inf.fog.simulator.workflow.scheduler.training;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.u_szeged.inf.fog.simulator.iot.Actuator;
import hu.u_szeged.inf.fog.simulator.node.WorkflowComputingAppliance;
import hu.u_szeged.inf.fog.simulator.provider.Instance;
import hu.u_szeged.inf.fog.simulator.workflow.WorkflowJob;
import hu.u_szeged.inf.fog.simulator.workflow.scheduler.HeftDsScheduler;
import java.util.ArrayList;
import org.apache.commons.lang3.tuple.Pair;

/**
 * HEFT-DS scheduler with the same training-data logging pattern as
 * {@link HeftSchedulerWithLogging}.
 *
 * <p>The label is whichever node HEFT-DS chose, which means a mix of:
 * <ul>
 *   <li><b>Critical-path tasks</b> — HEFT-DS delegates to {@code super.findBestProcessor},
 *       i.e. vanilla HEFT EFT-minimisation.</li>
 *   <li><b>Non-critical tasks</b> — HEFT-DS's energy-aware MIPS-based selection
 *       ({@code findEnergyEfficientNode}), the choice that distinguishes
 *       HEFT-DS from plain HEFT.</li>
 * </ul>
 *
 * <p>Because HEFT-DS overrides {@code findBestProcessor} in its parent class
 * to dispatch between these two paths, hooking the same method here captures
 * the union of both decision types.
 */
public class HeftDsSchedulerWithLogging extends HeftDsScheduler {

    private final TrainingLogger logger;
    private boolean rootsLogged = false;

    public HeftDsSchedulerWithLogging(ArrayList<WorkflowComputingAppliance> computeArchitecture,
                                      Instance instance,
                                      ArrayList<Actuator> actuatorArchitecture,
                                      Pair<String, ArrayList<WorkflowJob>> jobs) {
        this(computeArchitecture, instance, actuatorArchitecture, jobs, null);
    }

    public HeftDsSchedulerWithLogging(ArrayList<WorkflowComputingAppliance> computeArchitecture,
                                      Instance instance,
                                      ArrayList<Actuator> actuatorArchitecture,
                                      Pair<String, ArrayList<WorkflowJob>> jobs,
                                      String workflowXmlPath) {
        super(computeArchitecture, instance, actuatorArchitecture, jobs);
        this.logger = new TrainingLogger(jobs.getLeft(), "heftds", workflowXmlPath);
    }

    @Override
    public void init() {
        super.init();
        logRootTasks();
    }

    /**
     * Capture each root task's static assignment after task merging. Called
     * once after {@code super.init()}, which includes both the HEFT static
     * schedule and the HEFT-DS task-merging pass that may have moved roots.
     */
    private void logRootTasks() {
        if (rootsLogged) {
            return;
        }
        rootsLogged = true;

        double elapsed = secondsFromFireCount(Timed.getFireCount());
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
        double elapsed = secondsFromFireCount(Timed.getFireCount());
        int totalTasks = this.jobs.size();
        int tasksDone = logger.getDecisionCount();

        double[] features = SchedulerFeatureExtractor.extract(
                job, this.rankUp, this.predecessors, this.successors, this.commData,
                this.computeArchitecture, tasksDone, totalTasks, elapsed);

        WorkflowComputingAppliance chosen = super.findBestProcessor(job);
        int nodeIdx = this.computeArchitecture.indexOf(chosen);
        logger.logDecision(job.id, features, nodeIdx);

        return chosen;
    }

    public void finishLogging(double makespanSeconds, double energyKwh) {
        logger.finish(makespanSeconds, energyKwh,
                this.jobs.size(), this.computeArchitecture.size());
    }

    public TrainingLogger getLogger() {
        return logger;
    }

    private static double secondsFromFireCount(long fireCount) {
        return fireCount / 1000.0;
    }
}
