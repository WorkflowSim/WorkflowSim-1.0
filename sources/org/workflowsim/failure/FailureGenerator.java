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
import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.cloudbus.cloudsim.Cloudlet;
import org.workflowsim.Job;
import org.workflowsim.Task;
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
    private static double[][][] failureSamples;

    protected static RealDistribution getDistribution(double alpha, double beta) {
        RealDistribution distribution = null;
        switch (FailureParameters.getFailureDistribution()) {
            case LOGNORMAL:
                distribution = new LogNormalDistribution(1.0 / alpha, beta);
                break;
            case WEIBULL:
                distribution = new WeibullDistribution(beta, 1.0 / alpha);
                break;
            case GAMMA:
                distribution = new GammaDistribution(beta, 1.0 / alpha);
                break;
            case NORMAL:
                //beta is the std, 1.0/alpha is the mean
                distribution = new NormalDistribution(1.0 / alpha, beta);
                break;
            default:
                break;
        }
        return distribution;
    }

    protected static void initFailureSamples() {
        if (FailureParameters.getFailureGeneratorMode() == FailureParameters.FTCFailure.FAILURE_NONE) {
            return;
        }
        FAILURE_SAMPLE_SIZE = FailureParameters.getFailureSampleSize();
        RealDistribution distribution;
        double[] samples;

        //first index is vm, second is job depth
        failureSamples = new double[FailureParameters.getAlphaMaxFirstIndex()][FailureParameters.getAlphaMaxSecondIndex()][FAILURE_SAMPLE_SIZE];
        for (int vmIndex = 0; vmIndex < FailureParameters.getAlphaMaxFirstIndex(); vmIndex++) {
            for (int taskDepth = 0; taskDepth < FailureParameters.getAlphaMaxSecondIndex(); taskDepth++) {
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
    }

    /**
     * Initialize a Failure Generator.
     */
    public static void init() {

        initFailureSamples();
    }

    protected static boolean checkFailureStatus(Task task, int vmId) {

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

            for (Iterator it = job.getTaskList().iterator(); it.hasNext();) {
                Task task = (Task) it.next();
                int failedTaskSum = 0;
                if (checkFailureStatus(task, job.getVmId())) {
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
