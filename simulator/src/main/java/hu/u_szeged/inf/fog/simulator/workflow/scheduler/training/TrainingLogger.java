package hu.u_szeged.inf.fog.simulator.workflow.scheduler.training;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

/**
 * Writes scheduler decision rows to a CSV plus a sidecar JSON with run
 * metadata (makespan, energy, scheduler name, etc.). One pair of files per
 * (workflow, scheduler) run.
 *
 * <p>File layout:
 * <pre>
 *   sim_res/training/&lt;workflow&gt;_&lt;scheduler&gt;_&lt;timestamp&gt;.csv
 *   sim_res/training/&lt;workflow&gt;_&lt;scheduler&gt;_&lt;timestamp&gt;.meta.json
 * </pre>
 *
 * <p>The CSV columns are:
 * <pre>
 *   decision_idx, task_id, f0, f1, ..., f49, chosen_node_idx
 * </pre>
 * with one header row using {@link SchedulerFeatureExtractor#FEATURE_NAMES}.
 *
 * <p>The notebook computes the sample weight as {@code 1 / makespan_seconds}
 * from the sidecar JSON to enable reward-weighted imitation learning.
 */
public class TrainingLogger {

    private static final Path TRAINING_DIR = Paths.get("sim_res", "training");

    private final Path csvPath;
    private final Path metaPath;
    private final BufferedWriter csv;
    private int decisionCount = 0;

    private final String workflowId;
    private final String schedulerName;

    public TrainingLogger(String workflowId, String schedulerName) {
        this.workflowId = workflowId;
        this.schedulerName = schedulerName;
        try {
            Files.createDirectories(TRAINING_DIR);
            long ts = System.currentTimeMillis();
            String stem = sanitize(workflowId) + "_" + schedulerName + "_" + ts;
            this.csvPath = TRAINING_DIR.resolve(stem + ".csv");
            this.metaPath = TRAINING_DIR.resolve(stem + ".meta.json");
            this.csv = new BufferedWriter(new FileWriter(csvPath.toFile()));
            writeHeader();
        } catch (IOException e) {
            throw new RuntimeException("Cannot open training log", e);
        }
    }

    private void writeHeader() throws IOException {
        StringBuilder sb = new StringBuilder("decision_idx,task_id");
        for (String name : SchedulerFeatureExtractor.FEATURE_NAMES) {
            sb.append(',').append(name);
        }
        sb.append(",chosen_node_idx\n");
        csv.write(sb.toString());
    }

    /**
     * Append a single scheduling decision to the CSV.
     */
    public void logDecision(String taskId, double[] features, int chosenNodeIdx) {
        if (features.length != SchedulerFeatureExtractor.FEATURE_DIM) {
            throw new IllegalArgumentException("Feature vector dim mismatch: "
                    + features.length + " vs " + SchedulerFeatureExtractor.FEATURE_DIM);
        }
        StringBuilder sb = new StringBuilder();
        sb.append(decisionCount).append(',').append(taskId);
        for (double v : features) {
            sb.append(',').append(formatNumber(v));
        }
        sb.append(',').append(chosenNodeIdx).append('\n');
        try {
            csv.write(sb.toString());
        } catch (IOException e) {
            throw new RuntimeException("Cannot write training row", e);
        }
        decisionCount++;
    }

    /**
     * Close the CSV and write the metadata sidecar. Call this once after the
     * simulation has finished and the makespan / energy figures are known.
     */
    public void finish(double makespanSec, double energyKwh, int totalTasks, int nodeCount) {
        try {
            csv.flush();
            csv.close();
        } catch (IOException e) {
            throw new RuntimeException("Cannot close CSV", e);
        }

        String json = "{\n"
                + "  \"workflow\": \"" + escape(workflowId) + "\",\n"
                + "  \"scheduler\": \"" + schedulerName + "\",\n"
                + "  \"makespan_seconds\": " + formatNumber(makespanSec) + ",\n"
                + "  \"energy_kwh\": " + formatNumber(energyKwh) + ",\n"
                + "  \"total_tasks\": " + totalTasks + ",\n"
                + "  \"total_decisions\": " + decisionCount + ",\n"
                + "  \"node_count\": " + nodeCount + ",\n"
                + "  \"csv_file\": \"" + csvPath.getFileName() + "\"\n"
                + "}\n";
        try (BufferedWriter meta = new BufferedWriter(new FileWriter(metaPath.toFile()))) {
            meta.write(json);
        } catch (IOException e) {
            throw new RuntimeException("Cannot write metadata JSON", e);
        }
    }

    public Path getCsvPath() {
        return csvPath;
    }

    public int getDecisionCount() {
        return decisionCount;
    }

    private static String sanitize(String s) {
        return s.replaceAll("[^A-Za-z0-9_-]", "_");
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String formatNumber(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            return "0";
        }
        if (v == Math.floor(v) && Math.abs(v) < 1e15) {
            return String.format(Locale.ROOT, "%.0f", v);
        }
        return String.format(Locale.ROOT, "%.6g", v);
    }
}
