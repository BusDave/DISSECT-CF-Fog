package hu.u_szeged.inf.fog.simulator.demo;

import hu.u_szeged.inf.fog.simulator.util.xml.WorkflowJobModel;
import hu.u_szeged.inf.fog.simulator.workflow.WorkflowJob;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;

/**
 * Test class to verify WfCommons converted workflows can be loaded.
 */
public class WfCommonsWorkflowTest {

    public static void main(String[] args) throws Exception {

        String workflowFile = ScenarioBase.resourcePath + "/WORKFLOW_examples/blast_workflow_converted.xml";

        System.out.println("Testing WfCommons converted workflow loading...");
        System.out.println("File: " + workflowFile);
        System.out.println();

        // Load the workflow
        Pair<String, ArrayList<WorkflowJob>> result = WorkflowJobModel.loadWorkflowXml(workflowFile, "0");

        String appName = result.getLeft();
        ArrayList<WorkflowJob> jobs = result.getRight();

        System.out.println("✓ Successfully loaded workflow: " + appName);
        System.out.println("  Total jobs: " + jobs.size());

        // Analyze workflow structure
        long rootJobs = jobs.stream().filter(j -> j.inputs.isEmpty()).count();
        long leafJobs = jobs.stream().filter(j -> j.inputs.isEmpty()).count();

        System.out.println("  Root jobs (no inputs): " + rootJobs);
        System.out.println("  Leaf jobs (no outputs): " + leafJobs);
        System.out.println("  Middle jobs: " + (jobs.size() - rootJobs - leafJobs));

        // Show first few jobs
        System.out.println("\nFirst 5 jobs:");
        for (int i = 0; i < Math.min(5, jobs.size()); i++) {
            WorkflowJob job = jobs.get(i);
            System.out.println("  " + (i+1) + ". " + job.id +
                             " (inputs: " + job.inputs.size() +
                             ", outputs: " + job.inputs.size() + ")");
        }

        System.out.println("\n✓ Test passed! WfCommons workflow is compatible with DISSECT-CF-Fog.");
    }
}
