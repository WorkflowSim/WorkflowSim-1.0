/**
 * Copyright 2012-2013 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.workflowsim.failure;

import java.util.Iterator;
import java.util.Random;
import org.apache.commons.math3.distribution.RealDistribution;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.Job;
import org.workflowsim.Task;
import org.workflowsim.utils.Parameters;
import org.apache.commons.math3.distribution.WeibullDistribution;
import org.cloudbus.cloudsim.Vm;

/**
 * FailureGenerator creates a failure when a job returns
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class FailureGenerator {

    /**
     *
     */
    private static final int FAILURE_SAMPLE_SIZE = 1000;
    private static Random generator;
    private static double[][] failureSamples;

    private static void initFailureSamples() {
        if (FailureParameters.getFailureGeneratorMode() == FailureParameters.FTCFailure.FAILURE_NONE) {
            return;
        }
        RealDistribution distribution;
        //failureSamples = new double[Parameters.getVmNum()][FailureParameters.getAlpha().size()][FAILURE_SAMPLE_SIZE];

        double[] samples;
        switch (FailureParameters.getFailureGeneratorMode()) {
            /**
             * Every task is considered.
             */
            case FAILURE_ALL:
                //by default
                double alpha = (Double) (FailureParameters.getAlpha().get(0));
                distribution = new WeibullDistribution(1.0, 1.0 / alpha); // the same distribution shared by all
                samples = distribution.sample(FAILURE_SAMPLE_SIZE);

                failureSamples = new double[1][FAILURE_SAMPLE_SIZE];
                failureSamples[0][0] = samples[0];
                for (int sampleId = 1; sampleId < failureSamples[0].length; sampleId++) {
                    failureSamples[0][sampleId] = failureSamples[0][sampleId - 1]
                            + samples[sampleId];
                }


                break;
            /**
             * Generate failures based on the type of job.
             */
            case FAILURE_JOB:

                for (int taskIndex = 0; taskIndex < FailureParameters.getAlpha().size(); taskIndex++) {
                    alpha = (Double) (FailureParameters.getAlpha().get(taskIndex));
                    distribution = new WeibullDistribution(1.0, 1.0 / alpha);
                    samples = distribution.sample(FAILURE_SAMPLE_SIZE);
                    failureSamples = new double[FailureParameters.getAlpha().size()][FAILURE_SAMPLE_SIZE];
                    failureSamples[taskIndex][0] = samples[0];
                    for (int sampleId = 1; sampleId < failureSamples[taskIndex].length; sampleId++) {
                        failureSamples[taskIndex][sampleId] = failureSamples[taskIndex][sampleId - 1]
                                + samples[sampleId];
                    }

                }
                break;
            /**
             * Generate failures based on the index of vm.
             */
            case FAILURE_VM:

                //alpha = (Double) (Parameters.getAlpha().get(job.getVmId()));
                for (int vmIndex = 0; vmIndex < Parameters.getVmNum(); vmIndex++) {
                    alpha = (Double) (FailureParameters.getAlpha().get(vmIndex));
                    distribution = new WeibullDistribution(1.0, 1.0 / alpha);
                    samples = distribution.sample(FAILURE_SAMPLE_SIZE);
                    failureSamples = new double[FailureParameters.getAlpha().size()][FAILURE_SAMPLE_SIZE];
                    failureSamples[vmIndex][0] = samples[0];
                    for (int sampleId = 1; sampleId < failureSamples[vmIndex].length; sampleId++) {
                        failureSamples[vmIndex][sampleId] = failureSamples[vmIndex][sampleId - 1]
                                + samples[sampleId];
                    }

                }

                break;

        }

    }

    /**
     * Initialize a Failure Generator.
     */
    public static void init() {
        generator = new Random(System.currentTimeMillis());

        initFailureSamples();
    }

    private static boolean checkFailureStatus(Task task, int vmId) {

        double[] samples = null;
        switch (FailureParameters.getFailureGeneratorMode()) {
            /**
             * Every task is considered.
             */
            case FAILURE_ALL:

                samples = failureSamples[0];
                break;
            /**
             * Generate failures based on the type of job.
             */
            case FAILURE_JOB:
                samples = failureSamples[task.getDepth()];
                break;
            /**
             * Generate failures based on the index of vm.
             */
            case FAILURE_VM:
                samples = failureSamples[vmId];
                break;
            default:
                return false;
        }

        double start = task.getExecStartTime();
        double end = task.getTaskFinishTime();
        for (int sampleId = 0; sampleId < samples.length; sampleId++) {
            if (end < samples[sampleId]) {
                //no failure
                return false;
            }
            if (start <= samples[sampleId]) {
                //has a failure
                return true;
            }
        }

        return false;
    }

    /**
     * Generates a failure or not
     *
     * @param job
     * @return whether it fails
     */
    //true means has failure
    //false means no failure
    public static boolean generate(Job job) {
        boolean jobFailed = false;
        if (FailureParameters.getFailureGeneratorMode() == FailureParameters.FTCFailure.FAILURE_NONE) {
            return jobFailed;
        }
        try {

            int randomValue = 0;//randome value
            for (Iterator it = job.getTaskList().iterator(); it.hasNext();) {
                Task task = (Task) it.next();
                double alpha = 0.0;
                switch (FailureParameters.getFailureGeneratorMode()) {
                    /**
                     * Every task is considered.
                     */
                    case FAILURE_ALL:
                        //by default

                        alpha = (Double) (FailureParameters.getAlpha().get(0));
                        break;
                    /**
                     * Generate failures based on the type of job.
                     */
                    case FAILURE_JOB:

                        if (FailureParameters.getAlpha().size() <= task.getDepth()) {
                            Log.printLine("Your setting of alpha list job is wrong");
                            System.exit(1);
                        }

                        alpha = (Double) (FailureParameters.getAlpha().get(task.getDepth()));
                        break;
                    /**
                     * Generate failures based on the type of vm.
                     */
                    case FAILURE_VM:

                        if (!FailureParameters.getAlpha().containsKey(job.getVmId())) {
                            Log.printLine("Your setting of alpha list vm is wrong");
                            System.exit(1);
                        }
                        alpha = (Double) (FailureParameters.getAlpha().get(job.getVmId()));

                        break;

                }

                int bound = (int) (alpha * 1000);
                randomValue = generator.nextInt(1000);
                int failedTaskSum = 0;
                if (checkFailureStatus(task, job.getVmId())) {
                    //if (randomValue <= bound) {
                    //this task fail
                    jobFailed = true;
                    failedTaskSum++;
                    task.setCloudletStatus(Cloudlet.FAILED);
                }
                FailureRecord record = new FailureRecord(0, failedTaskSum, task.getDepth(), 1, job.getVmId(), task.getCloudletId(), job.getUserId());
                FailureMonitor.postFailureRecord(record);
            }

            if (jobFailed) {
                job.setCloudletStatus(Cloudlet.FAILED);
            } else {
                job.setCloudletStatus(Cloudlet.SUCCESS);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return jobFailed;

    }
}
