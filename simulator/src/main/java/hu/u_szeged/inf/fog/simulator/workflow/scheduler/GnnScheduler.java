package hu.u_szeged.inf.fog.simulator.workflow.scheduler;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.u_szeged.inf.fog.simulator.iot.Actuator;
import hu.u_szeged.inf.fog.simulator.node.WorkflowComputingAppliance;
import hu.u_szeged.inf.fog.simulator.provider.Instance;
import hu.u_szeged.inf.fog.simulator.util.SimLogger;
import hu.u_szeged.inf.fog.simulator.workflow.WorkflowJob;
import hu.u_szeged.inf.fog.simulator.workflow.scheduler.training.GnnGraphBuilder;
import java.nio.FloatBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Graph-Neural-Network based scheduler. Extends {@link AdaptiveHeftScheduler}
 * (so distributed-mode workflows defer to the round-robin fallback) and feeds
 * an ONNX-exported GNN model the workflow DAG <em>and</em> the cluster topology
 * as two separate graphs. Unlike a flat MLP this is dynamic w.r.t. the cluster
 * size: the same ONNX file works on a 20-node and a 25-node setup with no
 * retraining.
 *
 * <p>Inference inputs (per scheduling decision):
 * <ol>
 *   <li>{@code wf_node_features}  shape [N_tasks, 6]</li>
 *   <li>{@code wf_adj_norm}       shape [N_tasks, N_tasks]</li>
 *   <li>{@code cl_node_features}  shape [N_nodes, 8]</li>
 *   <li>{@code cl_adj_norm}       shape [N_nodes, N_nodes]</li>
 *   <li>{@code current_task_idx}  shape [1] int64</li>
 * </ol>
 * Output: {@code node_scores [N_nodes]}.
 *
 * <p>The effective top-1 threshold is computed dynamically per decision as
 * {@code confidenceFactor / N_nodes}, i.e. "how many times better than
 * uniform random the top pick must be". This keeps the criterion invariant
 * to cluster size — a fixed absolute threshold would get harder to clear
 * as N grows, because the softmax mass spreads across more outputs even
 * when the model's preference structure is unchanged.
 */
public class GnnScheduler extends AdaptiveHeftScheduler {

    private static final String DEFAULT_MODEL_PATH =
            "src/main/resources/models/gnn_scheduler.onnx";

    private final OrtEnvironment env;
    private final OrtSession session;
    private final double confidenceFactor;
    private final String[] inputNames;     // wf_node_features, wf_adj_norm, cl_node_features, cl_adj_norm, current_task_idx

    /** Diagnostics. */
    public int gnnDecisions      = 0;
    public int fallbackDecisions = 0;
    public long inferenceTimeNs  = 0;

    public GnnScheduler(ArrayList<WorkflowComputingAppliance> computeArchitecture,
                        Instance instance,
                        ArrayList<Actuator> actuatorArchitecture,
                        Pair<String, ArrayList<WorkflowJob>> jobs) {
        // Factor 6.0 → on the 20-node training cluster this yields threshold 0.30
        // (identical to the original absolute calibration). On 25 nodes it
        // becomes 0.24, compensating for the softmax dilution that would
        // otherwise force the policy into fallback on nearly every decision.
        // A diagnostic run at factor=3.0 confirmed that lower thresholds let
        // out-of-distribution mispredictions through on the 25-node cluster:
        // GNN-ratio climbed to 36 % but makespan ballooned 2.9× (510 → 1473 s)
        // — evidence that the trained policy does not generalise to topology
        // changes, and that the conservative 6.0 calibration is correct.
        this(computeArchitecture, instance, actuatorArchitecture, jobs,
                DEFAULT_MODEL_PATH, 6.0);
    }

    public GnnScheduler(ArrayList<WorkflowComputingAppliance> computeArchitecture,
                        Instance instance,
                        ArrayList<Actuator> actuatorArchitecture,
                        Pair<String, ArrayList<WorkflowJob>> jobs,
                        String modelPath,
                        double confidenceFactor) {
        super(computeArchitecture, instance, actuatorArchitecture, jobs);
        this.confidenceFactor = confidenceFactor;
        try {
            this.env = OrtEnvironment.getEnvironment();
            this.session = env.createSession(modelPath, new OrtSession.SessionOptions());
            // ONNX model has 5 inputs in this order; capture for createTensor calls.
            this.inputNames = new String[] {
                    "wf_node_features",
                    "wf_adj_norm",
                    "cl_node_features",
                    "cl_adj_norm",
                    "current_task_idx",
            };
            // Sanity-check that the model's actual input names match.
            for (String name : inputNames) {
                if (!session.getInputNames().contains(name)) {
                    throw new RuntimeException("ONNX model missing expected input: " + name
                            + "  (model exposes: " + session.getInputNames() + ")");
                }
            }
        } catch (OrtException e) {
            throw new RuntimeException("Cannot load ONNX model from " + modelPath, e);
        }
        SimLogger.logRes(String.format(
                "GnnScheduler [%s] loaded GNN %s  (confidenceFactor=%.2f, "
                        + "→ threshold=%.3f at N=%d nodes)",
                this.appName, modelPath, confidenceFactor,
                confidenceFactor / computeArchitecture.size(),
                computeArchitecture.size()));
    }

    /**
     * If Adaptive HEFT switched to round-robin (distributed mode), don't touch
     * the assignments — they would only get worse if pulled through a learned
     * policy. Otherwise let the GNN re-pick the root tasks before they get
     * enqueued.
     */
    @Override
    public void init() {
        super.init();
        if (this.distributedMode) {
            return;
        }
        int totalTasks = this.jobs.size();
        for (WorkflowJob job : this.jobs) {
            if (job.inputs.get(0).amount != 0) {
                continue;
            }
            WorkflowComputingAppliance current = job.ca;
            if (current != null && current.workflowQueue != null) {
                current.workflowQueue.remove(job);
            }
            WorkflowComputingAppliance gnnPick = decide(job, 0, totalTasks, 0.0);
            job.ca = gnnPick;
            gnnPick.workflowQueue.add(job);
        }
    }

    @Override
    protected WorkflowComputingAppliance findBestProcessor(WorkflowJob job) {
        if (this.distributedMode) {
            return super.findBestProcessor(job);
        }
        int tasksDone = totalTasks() - countRemainingJobs();
        double elapsed = Timed.getFireCount() / 1000.0;
        return decide(job, tasksDone, totalTasks(), elapsed);
    }

    private WorkflowComputingAppliance decide(WorkflowJob job, int tasksDone,
                                              int totalTasks, double elapsed) {
        // 1. Build the two graphs from the live simulator state.
        GnnGraphBuilder.WorkflowGraph wfGraph = GnnGraphBuilder.buildWorkflowGraph(
                this.jobs, this.rankUp, this.predecessors, this.successors,
                this.commData, job.id);
        if (wfGraph.currentTaskIdx < 0) {
            fallbackDecisions++;
            return super.findBestProcessor(job);
        }
        GnnGraphBuilder.ClusterGraph clGraph = GnnGraphBuilder.buildClusterGraph(
                this.computeArchitecture);

        // 2. ONNX inference.
        long t0 = System.nanoTime();
        float[] scores;
        try {
            scores = runInference(wfGraph, clGraph);
        } catch (OrtException e) {
            SimLogger.logRes("GNN inference failed, falling back to Adaptive HEFT: " + e.getMessage());
            fallbackDecisions++;
            return super.findBestProcessor(job);
        } finally {
            inferenceTimeNs += System.nanoTime() - t0;
        }

        // 3. Softmax + top-1 with confidence threshold.
        int validNodes = clGraph.numNodes;
        float[] probs = softmax(scores, validNodes);
        int topIdx = 0;
        for (int i = 1; i < validNodes; i++) {
            if (probs[i] > probs[topIdx]) {
                topIdx = i;
            }
        }
        double effectiveThreshold = confidenceFactor / validNodes;
        if (probs[topIdx] < effectiveThreshold) {
            fallbackDecisions++;
            return super.findBestProcessor(job);
        }
        gnnDecisions++;
        return this.computeArchitecture.get(topIdx);
    }

    private float[] runInference(GnnGraphBuilder.WorkflowGraph wf,
                                  GnnGraphBuilder.ClusterGraph cl) throws OrtException {
        int nt = wf.numTasks;
        int nn = cl.numNodes;
        long[] shapeWfFeat = {nt, 6};
        long[] shapeWfAdj  = {nt, nt};
        long[] shapeClFeat = {nn, 8};
        long[] shapeClAdj  = {nn, nn};
        long[] shapeIdx    = {1};

        try (OnnxTensor tWfFeat = OnnxTensor.createTensor(env, FloatBuffer.wrap(wf.nodeFeatures), shapeWfFeat);
             OnnxTensor tWfAdj  = OnnxTensor.createTensor(env, FloatBuffer.wrap(wf.adjNormFlat), shapeWfAdj);
             OnnxTensor tClFeat = OnnxTensor.createTensor(env, FloatBuffer.wrap(cl.nodeFeatures), shapeClFeat);
             OnnxTensor tClAdj  = OnnxTensor.createTensor(env, FloatBuffer.wrap(cl.adjNormFlat), shapeClAdj);
             OnnxTensor tIdx    = OnnxTensor.createTensor(env, LongBuffer.wrap(new long[] {wf.currentTaskIdx}), shapeIdx)) {
            Map<String, OnnxTensor> inputs = new HashMap<>();
            inputs.put(inputNames[0], tWfFeat);
            inputs.put(inputNames[1], tWfAdj);
            inputs.put(inputNames[2], tClFeat);
            inputs.put(inputNames[3], tClAdj);
            inputs.put(inputNames[4], tIdx);
            try (OrtSession.Result out = session.run(inputs)) {
                float[] result = (float[]) out.get(0).getValue();
                return result;
            }
        }
    }

    private static float[] softmax(float[] logits, int validNodes) {
        float max = Float.NEGATIVE_INFINITY;
        for (int i = 0; i < validNodes; i++) {
            if (logits[i] > max) {
                max = logits[i];
            }
        }
        float sum = 0f;
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

    private int totalTasks() {
        return this.jobs.size();
    }

    private int countRemainingJobs() {
        int n = 0;
        for (WorkflowComputingAppliance ca : this.computeArchitecture) {
            if (ca.workflowQueue != null) {
                n += ca.workflowQueue.size();
            }
            // Running tasks across this node's VMs.
            for (hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine vm : ca.iaas.listVMs()) {
                n += vm.underProcessing.size();
            }
        }
        return n;
    }

    public void close() {
        try {
            session.close();
        } catch (OrtException ignored) {
            // best-effort cleanup
        }
    }
}
