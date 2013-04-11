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
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.Job;
import org.workflowsim.Task;
import org.workflowsim.utils.Parameters;

/**
 * FailureGenerator creates a failure when a job returns
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class FailureGenerator {

    /**
     * The Random generator.
     */
    private static Random generator;

    /**
     * Initialize a Failure Generator.
     */
    public static void init() {
        generator = new Random(System.currentTimeMillis());
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

        try {

            int randomValue = 0;//randome value
            for (Iterator it = job.getTaskList().iterator(); it.hasNext();) {
                Task task = (Task) it.next();
                double alpha = 0.0;
                switch (Parameters.getFailureGeneratorMode()) {
                    /**
                     * Every task is considered.
                     */
                    case FAILURE_ALL:
                        //by default

                        alpha = (Double) (Parameters.getAlpha().get(0));
                        break;
                    /**
                     * Generate failures based on the type of job.
                     */
                    case FAILURE_JOB:

                        if (Parameters.getAlpha().size() <= task.getDepth()) {
                            Log.printLine("Your setting of alpha list job is wrong");
                            System.exit(1);
                        }

                        alpha = (Double) (Parameters.getAlpha().get(task.getDepth()));
                        break;
                    /**
                     * Generate failures based on the type of vm.
                     */
                    case FAILURE_VM:

                        if (!Parameters.getAlpha().containsKey(job.getVmId())) {
                            Log.printLine("Your setting of alpha list vm is wrong");
                            System.exit(1);
                        }
                        alpha = (Double) (Parameters.getAlpha().get(job.getVmId()));

                        break;
                    case FAILURE_NONE:
                        /**
                         * 0.0 doesn't work.
                         */
                        alpha = (-0.1);
                        break;
                }

                int bound = (int) (alpha * 1000);
                randomValue = generator.nextInt(1000);
                int failedTaskSum = 0;
                if (randomValue <= bound) {
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
