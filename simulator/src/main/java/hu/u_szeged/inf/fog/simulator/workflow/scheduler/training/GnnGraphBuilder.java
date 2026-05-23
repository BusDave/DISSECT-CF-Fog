package hu.u_szeged.inf.fog.simulator.workflow.scheduler.training;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.u_szeged.inf.fog.simulator.iot.mobility.GeoLocation;
import hu.u_szeged.inf.fog.simulator.node.WorkflowComputingAppliance;
import hu.u_szeged.inf.fog.simulator.workflow.WorkflowJob;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Java mirror of {@code notebooks/gnn_data_pipeline.py}: converts the live
 * simulator state into the tensors the GNN expects as input.
 *
 * <p>Two graphs are built every inference call:
 * <ul>
 *   <li><b>Workflow graph</b> — one node per {@link WorkflowJob}; edges from
 *       parent task to child task (using the {@code commData} map). Features
 *       per task: log-runtime, log-rank_up, normalised in/out-degree,
 *       log-input-bytes, is-current-task indicator.</li>
 *   <li><b>Cluster graph</b> — one node per {@link WorkflowComputingAppliance};
 *       edges fully-connected with weight {@code 1 / (1 + dist_km / 1000)}.
 *       Features per node: log-cpu, log-ram, log-bandwidth, lat/90, lon/180,
 *       is-cloud, log-queue-size, log-running-tasks.</li>
 * </ul>
 *
 * <p>Both adjacency matrices are returned in symmetrically-normalised form
 * (D^{-1/2} (A + I) D^{-1/2}), so the GNN forward pass can do a plain dense
 * mat-mul to aggregate neighbours.
 *
 * <p>This featurization MUST stay byte-for-byte compatible with the Python
 * pipeline used at training time, otherwise the ONNX inference will produce
 * gibberish. Any change here needs a matching change in
 * {@code gnn_data_pipeline.py} and a fresh re-train.
 */
public final class GnnGraphBuilder {

    private static final double COMM_RATE_BPS = 62_500_000.0;
    private static final double EARTH_RADIUS_KM = 6371.0;

    private GnnGraphBuilder() {
    }

    // ============================================================
    //  Workflow graph
    // ============================================================

    public static final class WorkflowGraph {
        public final int numTasks;
        /** Row-major flat array of shape [N, 6]. */
        public final float[] nodeFeatures;
        /** Row-major flat array of shape [N, N]. */
        public final float[] adjNormFlat;
        /** Index of the current task being scheduled (0..N-1). */
        public final int currentTaskIdx;

        public WorkflowGraph(int numTasks, float[] nodeFeatures,
                              float[] adjNormFlat, int currentTaskIdx) {
            this.numTasks       = numTasks;
            this.nodeFeatures   = nodeFeatures;
            this.adjNormFlat    = adjNormFlat;
            this.currentTaskIdx = currentTaskIdx;
        }
    }

    /**
     * Build the workflow graph tensor pair. {@code rankUp},
     * {@code predecessors}, {@code successors}, and {@code commData} are the
     * same protected maps the HEFT family already maintains.
     */
    public static WorkflowGraph buildWorkflowGraph(
            List<WorkflowJob> jobs,
            Map<String, Double> rankUp,
            Map<String, List<String>> predecessors,
            Map<String, List<String>> successors,
            Map<String, Map<String, Long>> commData,
            String currentTaskId) {

        int n = jobs.size();
        Map<String, Integer> idToIdx = new HashMap<>(n);
        for (int i = 0; i < n; i++) {
            idToIdx.put(jobs.get(i).id, i);
        }
        int currentIdx = idToIdx.getOrDefault(currentTaskId, -1);

        // ---- node features [N, 6] ----
        float[] feats = new float[n * 6];
        double maxIn = 1.0;
        double maxOut = 1.0;
        if (predecessors != null) {
            for (Map.Entry<String, List<String>> e : predecessors.entrySet()) {
                if (e.getValue().size() > maxIn) {
                    maxIn = e.getValue().size();
                }
            }
        }
        if (successors != null) {
            for (Map.Entry<String, List<String>> e : successors.entrySet()) {
                if (e.getValue().size() > maxOut) {
                    maxOut = e.getValue().size();
                }
            }
        }

        for (int i = 0; i < n; i++) {
            WorkflowJob job = jobs.get(i);
            int base = i * 6;
            feats[base    ] = (float) Math.log1p(job.runtime);
            double rank = rankUp != null ? rankUp.getOrDefault(job.id, 0.0) : 0.0;
            feats[base + 1] = (float) Math.log1p(Math.max(rank, 0.0));
            int inDeg = predecessors != null
                    ? predecessors.getOrDefault(job.id, Collections.emptyList()).size() : 0;
            int outDeg = successors != null
                    ? successors.getOrDefault(job.id, Collections.emptyList()).size() : 0;
            feats[base + 2] = (float) (inDeg  / maxIn);
            feats[base + 3] = (float) (outDeg / maxOut);
            feats[base + 4] = (float) Math.log1p(totalInputBytes(job, predecessors, commData));
            feats[base + 5] = (i == currentIdx) ? 1.0f : 0.0f;
        }

        // ---- adjacency (undirected for GNN message passing) ----
        float[] adj = new float[n * n];
        if (commData != null) {
            for (Map.Entry<String, Map<String, Long>> entry : commData.entrySet()) {
                Integer p = idToIdx.get(entry.getKey());
                if (p == null) {
                    continue;
                }
                for (String childId : entry.getValue().keySet()) {
                    Integer c = idToIdx.get(childId);
                    if (c == null) {
                        continue;
                    }
                    adj[p * n + c] = 1.0f;
                    adj[c * n + p] = 1.0f;
                }
            }
        }
        // Symmetric normalize with self-loops: D^{-1/2} (A + I) D^{-1/2}
        normalizeAdjacencyInPlace(adj, n);

        return new WorkflowGraph(n, feats, adj, currentIdx);
    }

    private static long totalInputBytes(WorkflowJob job,
                                         Map<String, List<String>> predecessors,
                                         Map<String, Map<String, Long>> commData) {
        if (predecessors == null || commData == null) {
            return 0L;
        }
        long total = 0L;
        for (String pid : predecessors.getOrDefault(job.id, Collections.emptyList())) {
            Map<String, Long> edges = commData.get(pid);
            if (edges != null) {
                total += edges.getOrDefault(job.id, 0L);
            }
        }
        return total;
    }

    // ============================================================
    //  Cluster graph
    // ============================================================

    public static final class ClusterGraph {
        public final int numNodes;
        /** Row-major [N, 8]. */
        public final float[] nodeFeatures;
        /** Row-major [N, N]. */
        public final float[] adjNormFlat;

        public ClusterGraph(int numNodes, float[] nodeFeatures, float[] adjNormFlat) {
            this.numNodes     = numNodes;
            this.nodeFeatures = nodeFeatures;
            this.adjNormFlat  = adjNormFlat;
        }
    }

    /**
     * Static cluster features (CPU / RAM / bandwidth / lat / lon / is-cloud)
     * + dynamic per-node queue size and running task count.
     */
    public static ClusterGraph buildClusterGraph(
            List<WorkflowComputingAppliance> nodes) {

        int n = nodes.size();
        float[] feats = new float[n * 8];
        for (int i = 0; i < n; i++) {
            WorkflowComputingAppliance ca = nodes.get(i);
            int base = i * 8;
            double cpuCount = ca.iaas.getCapacities().getRequiredCPUs();
            double powerPerCpu = ca.iaas.getCapacities().getRequiredProcessingPower();
            double cpuMips = cpuCount * powerPerCpu;
            double ramGb = ca.iaas.getCapacities().getRequiredMemory()
                    / (1024.0 * 1024.0 * 1024.0);
            double bwMbps = estimateBandwidthMbps(ca);
            GeoLocation loc = ca.geoLocation;
            double lat = loc != null ? loc.latitude  : 0.0;
            double lon = loc != null ? loc.longitude : 0.0;
            boolean isCloud = cpuCount >= 32;

            feats[base    ] = (float) Math.log1p(cpuCount);
            feats[base + 1] = (float) Math.log1p(ramGb);
            feats[base + 2] = (float) Math.log1p(bwMbps);
            feats[base + 3] = (float) (lat / 90.0);
            feats[base + 4] = (float) (lon / 180.0);
            feats[base + 5] = isCloud ? 1.0f : 0.0f;
            feats[base + 6] = (float) Math.log1p(
                    ca.workflowQueue != null ? ca.workflowQueue.size() : 0);
            feats[base + 7] = (float) Math.log1p(countRunningTasks(ca));
            // Suppress unused warning if cpuMips becomes useful later.
            if (cpuMips < 0) {
                feats[base] = 0f;
            }
        }

        // ---- adjacency: fully-connected weighted by 1/(1 + dist_km/1000) ----
        float[] adj = new float[n * n];
        for (int i = 0; i < n; i++) {
            GeoLocation a = nodes.get(i).geoLocation;
            for (int j = 0; j < n; j++) {
                if (i == j) {
                    continue;
                }
                GeoLocation b = nodes.get(j).geoLocation;
                if (a == null || b == null) {
                    continue;
                }
                double dist = haversineKm(a.latitude, a.longitude, b.latitude, b.longitude);
                adj[i * n + j] = (float) (1.0 / (1.0 + dist / 1000.0));
            }
        }
        normalizeAdjacencyInPlace(adj, n);

        return new ClusterGraph(n, feats, adj);
    }

    private static int countRunningTasks(WorkflowComputingAppliance ca) {
        int total = 0;
        for (VirtualMachine vm : ca.iaas.listVMs()) {
            total += vm.underProcessing.size();
        }
        return total;
    }

    private static double estimateBandwidthMbps(WorkflowComputingAppliance ca) {
        // Same heuristic the Python pipeline uses: cloud=1000, fog=500, edge=100.
        // Inferred from CPU count since that's what's accessible Java-side.
        double cpus = ca.iaas.getCapacities().getRequiredCPUs();
        if (cpus >= 32) {
            return 1000.0;
        }
        if (cpus >= 8) {
            return 500.0;
        }
        return 100.0;
    }

    private static double haversineKm(double lat1, double lon1,
                                       double lat2, double lon2) {
        double p1 = Math.toRadians(lat1);
        double p2 = Math.toRadians(lat2);
        double dp = Math.toRadians(lat2 - lat1);
        double dl = Math.toRadians(lon2 - lon1);
        double a  = Math.sin(dp / 2) * Math.sin(dp / 2)
                  + Math.cos(p1) * Math.cos(p2) * Math.sin(dl / 2) * Math.sin(dl / 2);
        return 2 * EARTH_RADIUS_KM * Math.asin(Math.sqrt(a));
    }

    /**
     * In-place computation of D^{-1/2} (A + I) D^{-1/2}.
     * adj is given as flat row-major [N, N].
     */
    private static void normalizeAdjacencyInPlace(float[] adj, int n) {
        // Step 1: add self-loops (A + I).
        for (int i = 0; i < n; i++) {
            adj[i * n + i] += 1.0f;
        }
        // Step 2: row sums.
        double[] deg = new double[n];
        for (int i = 0; i < n; i++) {
            double s = 0.0;
            int base = i * n;
            for (int j = 0; j < n; j++) {
                s += adj[base + j];
            }
            deg[i] = s;
        }
        // Step 3: D^{-1/2}, with safe floor.
        double[] dInvSqrt = new double[n];
        for (int i = 0; i < n; i++) {
            dInvSqrt[i] = 1.0 / Math.sqrt(Math.max(deg[i], 1e-9));
        }
        // Step 4: A_norm[i, j] = D^{-1/2}[i] * A[i, j] * D^{-1/2}[j].
        for (int i = 0; i < n; i++) {
            int base = i * n;
            double di = dInvSqrt[i];
            for (int j = 0; j < n; j++) {
                adj[base + j] = (float) (di * adj[base + j] * dInvSqrt[j]);
            }
        }
    }
}
