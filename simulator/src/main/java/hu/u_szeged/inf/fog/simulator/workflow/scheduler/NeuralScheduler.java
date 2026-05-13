package hu.u_szeged.inf.fog.simulator.workflow.scheduler;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.u_szeged.inf.fog.simulator.iot.Actuator;
import hu.u_szeged.inf.fog.simulator.node.WorkflowComputingAppliance;
import hu.u_szeged.inf.fog.simulator.provider.Instance;
import hu.u_szeged.inf.fog.simulator.util.SimLogger;
import hu.u_szeged.inf.fog.simulator.workflow.WorkflowJob;
import hu.u_szeged.inf.fog.simulator.workflow.scheduler.training.SchedulerFeatureExtractor;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.commons.lang3.tuple.Pair;

/**
 * MLP-based scheduler trained to imitate HEFT / HEFT-DS via reward-weighted
 * supervised learning. Loads an ONNX model exported from the
 * {@code simulator/notebooks/train_heft_imitation.ipynb} pipeline.
 *
 * <p>Inherits from {@link AdaptiveHeftScheduler} so we reuse the DAG
 * construction, upward-rank computation (the network expects
 * {@code task_rank_up} as a feature) and the Adaptive-HEFT fallback path.
 * The previous version extended {@link HeftScheduler} directly, which made
 * the fallback plain HEFT - empirically the worst of our baselines on wide
 * low-comm workflows. Falling back to Adaptive HEFT means the worst-case
 * behaviour matches the strongest heuristic instead.
 *
 * <p>Decision flow at runtime:
 * <ol>
 *   <li>Build the same 50-dim state vector the training CSVs used
 *       ({@link SchedulerFeatureExtractor}).</li>
 *   <li>Run the ONNX session, get logits → softmax.</li>
 *   <li>If the top-1 probability is at least {@link #confidenceThreshold},
 *       pick that node. Otherwise fall back to the Adaptive-HEFT
 *       {@code findBestProcessor} (which itself routes to round-robin in
 *       distributed mode or EFT-greedy HEFT-DS otherwise).</li>
 * </ol>
 *
 * <p>The ONNX wrapper exported from the notebook bakes the z-score
 * normalisation in, so we feed raw features and read logits back.
 */
public class NeuralScheduler extends AdaptiveHeftScheduler {

    private static final String DEFAULT_MODEL_PATH =
            "src/main/resources/models/heft_imitation.onnx";

    private final OrtEnvironment env;
    private final OrtSession session;
    private final String inputName;
    private final double confidenceThreshold;

    /** Diagnostics: how many decisions used the model vs the HEFT fallback. */
    public int neuralDecisions = 0;
    public int fallbackDecisions = 0;

    /** Diagnostics: total nanoseconds spent in ONNX inference. */
    public long inferenceTimeNs = 0;

    public NeuralScheduler(ArrayList<WorkflowComputingAppliance> computeArchitecture,
                           Instance instance,
                           ArrayList<Actuator> actuatorArchitecture,
                           Pair<String, ArrayList<WorkflowJob>> jobs) {
        this(computeArchitecture, instance, actuatorArchitecture, jobs,
                DEFAULT_MODEL_PATH, 0.5);
    }

    public NeuralScheduler(ArrayList<WorkflowComputingAppliance> computeArchitecture,
                           Instance instance,
                           ArrayList<Actuator> actuatorArchitecture,
                           Pair<String, ArrayList<WorkflowJob>> jobs,
                           String modelPath,
                           double confidenceThreshold) {
        super(computeArchitecture, instance, actuatorArchitecture, jobs);
        this.confidenceThreshold = confidenceThreshold;
        try {
            this.env = OrtEnvironment.getEnvironment();
            this.session = env.createSession(modelPath, new OrtSession.SessionOptions());
            this.inputName = session.getInputNames().iterator().next();
        } catch (OrtException e) {
            throw new RuntimeException("Cannot load ONNX model from " + modelPath, e);
        }
        SimLogger.logRes(String.format(
                "NeuralScheduler [%s] loaded model %s  (input=%s, threshold=%.2f)",
                this.appName, modelPath, inputName, confidenceThreshold));
    }

    /**
     * Root tasks (in-degree 0) — let the network choose for these too instead
     * of taking the static HEFT assignment. We piggyback on
     * {@code super.init()} for the DAG/rank/static-schedule build, then
     * overwrite the root-job CA pointers with neural picks before any of
     * them get enqueued.
     */
    @Override
    public void init() {
        super.init();
        // If the parent decided this workflow needs round-robin distribution,
        // leave the assignments alone - the neural net would just undo them.
        if (this.distributedMode) {
            return;
        }
        // EFT-greedy mode: super.init() has added root jobs to their HEFT-DS
        // assigned queues. Rebuild those root assignments with the neural
        // model so we exercise the trained policy from the very first task.
        int totalTasks = this.jobs.size();
        for (WorkflowJob job : this.jobs) {
            if (job.inputs.get(0).amount != 0) {
                continue;                       // not a root
            }
            WorkflowComputingAppliance heftPick = job.ca;
            if (heftPick != null) {
                heftPick.workflowQueue.remove(job);
            }
            WorkflowComputingAppliance neuralPick = decide(job, 0, totalTasks, 0.0);
            job.ca = neuralPick;
            neuralPick.workflowQueue.add(job);
        }
    }

    /**
     * Overridden dynamic-phase decision point. In distributed mode the parent
     * (Adaptive HEFT) has already determined that this workflow looks "wide
     * and sparse" and a capacity-uniform distribution wins. The neural model
     * was trained on EFT-greedy teachers (HEFT, HEFT-DS) so its predictions
     * will pull tasks back toward the cloud nodes, undoing the distribution.
     * Defer to the parent's static assignment in that case. In normal mode
     * we ask the network and fall back to the parent's EFT-greedy logic only
     * when confidence is below threshold.
     */
    @Override
    protected WorkflowComputingAppliance findBestProcessor(WorkflowJob job) {
        if (this.distributedMode) {
            // Adaptive HEFT has switched to round-robin for this workflow;
            // the neural net would actively harm us here.
            return super.findBestProcessor(job);
        }
        int tasksDone = totalTasks() - countRemainingJobs();
        double elapsed = Timed.getFireCount() / 1000.0;
        return decide(job, tasksDone, totalTasks(), elapsed);
    }

    private WorkflowComputingAppliance decide(WorkflowJob job, int tasksDone,
                                              int totalTasks, double elapsed) {
        double[] features = SchedulerFeatureExtractor.extract(
                job, this.rankUp, this.predecessors, this.successors, this.commData,
                this.computeArchitecture, tasksDone, totalTasks, elapsed);

        long t0 = System.nanoTime();
        float[] logits;
        try {
            logits = runInference(features);
        } catch (OrtException e) {
            SimLogger.logRes("Neural inference failed, falling back to HEFT: " + e.getMessage());
            fallbackDecisions++;
            return superFindBestProcessor(job);
        } finally {
            inferenceTimeNs += System.nanoTime() - t0;
        }

        // Mask out node indices that don't exist in this cluster (the model
        // always outputs 20 logits but the cluster may be smaller).
        int validNodes = Math.min(SchedulerFeatureExtractor.MAX_NODES,
                this.computeArchitecture.size());
        float[] probs = softmax(logits, validNodes);
        int topIdx = 0;
        for (int i = 1; i < validNodes; i++) {
            if (probs[i] > probs[topIdx]) {
                topIdx = i;
            }
        }

        if (probs[topIdx] < confidenceThreshold) {
            fallbackDecisions++;
            return superFindBestProcessor(job);
        }
        neuralDecisions++;
        return this.computeArchitecture.get(topIdx);
    }

    private float[] runInference(double[] features) throws OrtException {
        float[] floatFeatures = new float[features.length];
        for (int i = 0; i < features.length; i++) {
            floatFeatures[i] = (float) features[i];
        }
        FloatBuffer buffer = FloatBuffer.wrap(floatFeatures);
        long[] shape = {1, features.length};
        try (OnnxTensor input = OnnxTensor.createTensor(env, buffer, shape);
             OrtSession.Result out = session.run(Collections.singletonMap(inputName, input))) {
            float[][] logits = (float[][]) out.get(0).getValue();
            return logits[0];
        }
    }

    private static float[] softmax(float[] logits, int validNodes) {
        // Numerically stable softmax over the first {@code validNodes} entries.
        float max = Float.NEGATIVE_INFINITY;
        for (int i = 0; i < validNodes; i++) {
            if (logits[i] > max) {
                max = logits[i];
            }
        }
        float sum = 0;
        float[] out = new float[validNodes];
        for (int i = 0; i < validNodes; i++) {
            out[i] = (float) Math.exp(logits[i] - max);
            sum += out[i];
        }
        for (int i = 0; i < validNodes; i++) {
            out[i] /= sum;
        }
        return out;
    }

    private WorkflowComputingAppliance superFindBestProcessor(WorkflowJob job) {
        return super.findBestProcessor(job);
    }

    private int totalTasks() {
        return this.jobs.size();
    }

    private int countRemainingJobs() {
        int n = 0;
        for (WorkflowComputingAppliance ca : this.computeArchitecture) {
            if (ca.workflowQueue != null) {
                n += ca.workflowQueue.size();
            }
            for (VirtualMachine vm : ca.iaas.listVMs()) {
                n += vm.underProcessing.size();
            }
        }
        return n;
    }

    /** Release the ONNX session — call once the simulation has finished. */
    public void close() {
        try {
            session.close();
        } catch (OrtException e) {
            // best-effort cleanup, nothing meaningful to do here
        }
    }
}
