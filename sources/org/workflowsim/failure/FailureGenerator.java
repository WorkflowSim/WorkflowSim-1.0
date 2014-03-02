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
    private static int FAILURE_SAMPLE_SIZE;
    private static Random generator;
    private static double[][][] failureSamples;

    private static void initFailureSamples() {
        if (FailureParameters.getFailureGeneratorMode() == FailureParameters.FTCFailure.FAILURE_NONE) {
            return;
        }
        FAILURE_SAMPLE_SIZE = FailureParameters.getFailureSampleSize();
        RealDistribution distribution;
        double[] samples;
        
        //first index is vm, second is job depth
        failureSamples = new double[FailureParameters.getAlphaMaxFirstIndex()][FailureParameters.getAlphaMaxSecondIndex()][FAILURE_SAMPLE_SIZE];
        for(int vmIndex = 0 ; vmIndex < FailureParameters.getAlphaMaxFirstIndex(); vmIndex ++){
            for(int taskDepth = 0 ; taskDepth < FailureParameters.getAlphaMaxSecondIndex(); taskDepth ++){
                double alpha = FailureParameters.getAlpha(vmIndex, taskDepth);
                double beta = FailureParameters.getBeta(vmIndex, taskDepth);
                distribution = new WeibullDistribution(beta, 1.0 / alpha);
                samples = distribution.sample(FAILURE_SAMPLE_SIZE);
                failureSamples[vmIndex][taskDepth][0] = samples[0];
                for (int sampleId = 1; sampleId < failureSamples[vmIndex][taskDepth].length; sampleId++) {
                    failureSamples[vmIndex][taskDepth][sampleId] = failureSamples[vmIndex][taskDepth][sampleId - 1]
                            + samples[sampleId];
                }
            }
        }
        /*
        switch (FailureParameters.getFailureGeneratorMode()) {

            case FAILURE_ALL:
                //by default
                failureSamples = new double[1][1][FAILURE_SAMPLE_SIZE];
                double alpha = FailureParameters.getAlpha()[0][0];
                double beta = FailureParameters.getBeta()[0][0];
                distribution = new WeibullDistribution(beta, 1.0 / alpha); // the same distribution shared by all
                samples = distribution.sample(FAILURE_SAMPLE_SIZE);

                
                failureSamples[0][0][0] = samples[0];
                for (int sampleId = 1; sampleId < failureSamples[0][0].length; sampleId++) {
                    failureSamples[0][0][sampleId] = failureSamples[0][0][sampleId - 1]
                            + samples[sampleId];
                }


                break;

            case FAILURE_JOB:

                failureSamples = new double[1][FailureParameters.getAlpha()[0].length][FAILURE_SAMPLE_SIZE];
                for (int taskIndex = 0; taskIndex < FailureParameters.getAlpha()[0].length; taskIndex++) {
                    alpha = FailureParameters.getAlpha()[0][taskIndex];
                    beta = FailureParameters.getBeta()[0][taskIndex];
                    distribution = new WeibullDistribution(beta, 1.0 / alpha);
                    samples = distribution.sample(FAILURE_SAMPLE_SIZE);
                    
                    failureSamples[0][taskIndex][0] = samples[0];
                    for (int sampleId = 1; sampleId < failureSamples[taskIndex].length; sampleId++) {
                        failureSamples[0][taskIndex][sampleId] = failureSamples[0][taskIndex][sampleId - 1]
                                + samples[sampleId];
                    }

                }
                break;

            case FAILURE_VM:

                failureSamples = new double[FailureParameters.getAlpha().size()][1][FAILURE_SAMPLE_SIZE];
                for (int vmIndex = 0; vmIndex < Parameters.getVmNum(); vmIndex++) {
                    alpha = (Double) (FailureParameters.getAlpha().get(vmIndex));
                    beta = (Double) (FailureParameters.getBeta().get(vmIndex));
                    distribution = new WeibullDistribution(beta, 1.0 / alpha);
                    samples = distribution.sample(FAILURE_SAMPLE_SIZE);
                    
                    failureSamples[vmIndex][0][0] = samples[0];
                    for (int sampleId = 1; sampleId < failureSamples[vmIndex].length; sampleId++) {
                        failureSamples[vmIndex][0][sampleId] = failureSamples[vmIndex][0][sampleId - 1]
                                + samples[sampleId];
                    }

                }

                break;
            case FAILURE_VM_JOB:
                
                break;
        }
    */

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

                samples = failureSamples[0][0];
                break;
            /**
             * Generate failures based on the type of job.
             */
            case FAILURE_JOB:
                samples = failureSamples[0][task.getDepth()];
                break;
            /**
             * Generate failures based on the index of vm.
             */
            case FAILURE_VM:
                samples = failureSamples[vmId][0];
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

                        //alpha = (Double) (FailureParameters.getAlpha().get(0));
                        break;
                    /**
                     * Generate failures based on the type of job.
                     */
                    case FAILURE_JOB:

                        //alpha = (Double) (FailureParameters.getAlpha().get(task.getDepth()));
                        break;
                    /**
                     * Generate failures based on the type of vm.
                     */
                    case FAILURE_VM:

                        //alpha = (Double) (FailureParameters.getAlpha().get(job.getVmId()));

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
