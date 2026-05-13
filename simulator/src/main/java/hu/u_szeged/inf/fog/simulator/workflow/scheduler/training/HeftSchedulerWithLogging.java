package hu.u_szeged.inf.fog.simulator.workflow.scheduler.training;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.u_szeged.inf.fog.simulator.iot.Actuator;
import hu.u_szeged.inf.fog.simulator.node.WorkflowComputingAppliance;
import hu.u_szeged.inf.fog.simulator.provider.Instance;
import hu.u_szeged.inf.fog.simulator.workflow.WorkflowJob;
import hu.u_szeged.inf.fog.simulator.workflow.scheduler.HeftScheduler;
import java.util.ArrayList;
import org.apache.commons.lang3.tuple.Pair;

/**
 * HEFT scheduler that logs every scheduling decision to a CSV for offline
 * imitation-learning training.
 *
 * <p>Two decision moments are captured:
 * <ul>
 *   <li><b>Init-time root tasks</b> — root jobs (in-degree 0) get their node
 *       from the static HEFT schedule produced in {@code init()}. After
 *       {@code super.init()} we iterate the roots and log each with the
 *       state-vector-at-init (mostly zeros, since nothing has run yet) and
 *       the static assignment as the chosen label.</li>
 *   <li><b>Dynamic tasks</b> — non-root jobs go through {@link #findBestProcessor},
 *       which we override to capture the live state vector and the
 *       dynamic-phase choice.</li>
 * </ul>
 *
 * <p>To collect a run's training data: pass a unique {@code workflowId}
 * (anything human-readable + reproducible), run the simulation to
 * completion, then call {@link #finishLogging} with the final makespan and
 * energy figures so the sidecar JSON has the reward signal.
 */
public class HeftSchedulerWithLogging extends HeftScheduler {

    private final TrainingLogger logger;
    private boolean rootsLogged = false;

    public HeftSchedulerWithLogging(ArrayList<WorkflowComputingAppliance> computeArchitecture,
                                    Instance instance,
                                    ArrayList<Actuator> actuatorArchitecture,
                                    Pair<String, ArrayList<WorkflowJob>> jobs) {
        super(computeArchitecture, instance, actuatorArchitecture, jobs);
        this.logger = new TrainingLogger(jobs.getLeft(), "heft");
    }

    @Override
    public void init() {
        super.init();
        logRootTasks();
    }

    /**
     * Capture each root task's static-HEFT assignment. Called once after
     * {@code super.init()} completes.
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

    /**
     * Close the CSV and emit the sidecar JSON. Call once after the simulation
     * has finished, with the actual makespan and energy values measured for
     * this run (so the training notebook can compute the reward weight).
     */
    public void finishLogging(double makespanSeconds, double energyKwh) {
        logger.finish(makespanSeconds, energyKwh,
                this.jobs.size(), this.computeArchitecture.size());
    }

    public TrainingLogger getLogger() {
        return logger;
    }

    private static double secondsFromFireCount(long fireCount) {
        // Timed.getFireCount() is in milliseconds; convert to seconds.
        return fireCount / 1000.0;
    }
}
