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

import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.cloudbus.cloudsim.Cloudlet;
import org.workflowsim.Job;
import org.workflowsim.Task;
import org.apache.commons.math3.distribution.WeibullDistribution;
import org.workflowsim.utils.DistributionGenerator;

/**
 * FailureGenerator creates a failure when a job returns
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class FailureGenerator {

    /**
     * FailureGenerator doubles the size of distribution samples each time 
     * but only limits to maxFailureSizeExtension. Otherwise your failure rate 
     * is too high for this workflow
     */
    private static final int maxFailureSizeExtension = 50;
    private static int failureSizeExtension = 0;
    private static final boolean hasChangeTime = false;
    /**
     *
     * @param alpha
     * @param beta
     * @return 
     */
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
    }

    /**
     * Initialize a Failure Generator.
     */
    public static void init() {

        initFailureSamples();
    }

    protected static boolean checkFailureStatus(Task task, int vmId) throws Exception {


        DistributionGenerator generator;
        switch (FailureParameters.getFailureGeneratorMode()) {
            /**
             * Every task follows the same distribution.
             */
            case FAILURE_ALL:
                generator = FailureParameters.getGenerator(0, 0);
                break;
            /**
             * Generate failures based on the type of job.
             */
            case FAILURE_JOB:
                generator = FailureParameters.getGenerator(0, task.getDepth());
                break;
            /**
             * Generate failures based on the index of vm.
             */
            case FAILURE_VM:
                generator = FailureParameters.getGenerator(vmId, 0);
                break;
            /**
             * Generator failures based on vmId and level both
             */
            case FAILURE_VM_JOB:
                generator = FailureParameters.getGenerator(vmId, task.getDepth());
                break;
            default:
                return false;
        }
        
        double start = task.getExecStartTime();
        double end = task.getTaskFinishTime();
        
        
        double[] samples = generator.getCumulativeSamples();
        
        while (samples[samples.length - 1] < start) {
            generator.extendSamples();
            samples = generator.getCumulativeSamples();
            failureSizeExtension++;
            if (failureSizeExtension >= maxFailureSizeExtension) {
                throw new Exception("Error rate is too high such that the simulator terminates");

            }
        }

        for (int sampleId = 0; sampleId < samples.length; sampleId++) {
            if (end < samples[sampleId]) {
                //no failure
                return false;
            }
            if (start <= samples[sampleId]) {
                //has a failure
                /** The idea is we need to update the cursor in generator**/
                generator.getNextSample();
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

            for (Task task : job.getTaskList()) {
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
