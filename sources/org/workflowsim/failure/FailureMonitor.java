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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.cloudbus.cloudsim.Log;

/**
 * FailureMonitor collects failure information
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class FailureMonitor {

    /**
     * VM ID to a Failure Record. *
     */
    protected static Map<Integer, List<FailureRecord>> vm2record;
    /**
     * Type to a Failure Record. *
     */
    protected static Map<Integer, List<FailureRecord>> type2record;
    /**
     * JobID to a Failure Record. *
     */
    protected static Map<Integer, FailureRecord> jobid2record;
    /**
     * All the record list.
     */
    protected static List<FailureRecord> recordList;
    /**
     * Id to a Job.
     */
    public static Map index2job;

    /**
     * Initialize a FailureMonitor object.
     */
    public static void init() {
        vm2record = new HashMap<>();
        type2record = new HashMap<>();
        jobid2record = new HashMap<>();
        recordList = new ArrayList<>();
    }

    /**
     * Gets the optimal clustering factor based on analysis
     *
     * @param d delay
     * @param a task failure rate monitored
     * @param t task runtime
     * @return optimal clustering factor
     */
    protected static double getK(double d, double a, double t) {
        double k = (-d + Math.sqrt(d * d - 4 * d / Math.log(1 - a))) / (2 * t);
        return k;
    }

    /**
     * Gets the clustering factor
     *
     * @param record, a request
     * @return the clustering factor suggested
     */
    public static int getClusteringFactor(FailureRecord record) {

        double d = record.delayLength;

        double t = record.length;
        double a = 0.0;
        switch (FailureParameters.getMonitorMode()) {
            case MONITOR_JOB:
            /**
             * not supported *
             */
            case MONITOR_ALL:
                a = analyze(0, record.depth);
                break;
            case MONITOR_VM:
                a = analyze(0, record.vmId);
                break;
        }

        if (a <= 0.0) {
            return record.allTaskNum;
        } else {
            double k = getK(d, a, t);

            if (k <= 1) {
                k = 1;//minimal
            }

            return (int) k;
        }
    }

    /**
     * A post from a broker so that we can update record list
     *
     * @param record a failure record
     */
    public static void postFailureRecord(FailureRecord record) {

        if (record.workflowId < 0 || record.jobId < 0 || record.vmId < 0) {
            Log.printLine("Error in receiving failure record");
            return;
        }

        switch (FailureParameters.getMonitorMode()) {
            case MONITOR_VM:

                if (!vm2record.containsKey(record.vmId)) {
                    vm2record.put(record.vmId, new ArrayList<>());
                }
                vm2record.get(record.vmId).add(record);

                break;
            case MONITOR_JOB:

                if (!type2record.containsKey(record.depth)) {
                    type2record.put(record.depth, new ArrayList<>());
                }
                type2record.get(record.depth).add(record);

                break;
            case MONITOR_NONE:
                break;
        }

        recordList.add(record);
    }

    /**
     * Update the detected task failure rate based on record lists
     *
     * @param workflowId, doesn't work in this version
     * @param type, the type of job or vm
     * @return task failure rate
     */
    public static double analyze(int workflowId, int type) {

        /**
         * workflow level : all jobs together *
         */
        int sumFailures = 0;
        int sumJobs = 0;
        switch (FailureParameters.getMonitorMode()) {
            case MONITOR_ALL:

                for (FailureRecord record : recordList) {
                    sumFailures += record.failedTasksNum;
                    sumJobs += record.allTaskNum;
                }

                break;

            case MONITOR_JOB:

                if (type2record.containsKey(type)) {
                    for (FailureRecord record : type2record.get(type)) {

                        sumFailures += record.failedTasksNum;
                        sumJobs += record.allTaskNum;
                    }
                }

                break;
            case MONITOR_VM:

                if (vm2record.containsKey(type)) {
                    for (FailureRecord record : vm2record.get(type)) {

                        sumFailures += record.failedTasksNum;
                        sumJobs += record.allTaskNum;
                    }
                }

                break;
        }


        if (sumFailures == 0) {
            return 0;
        }
        double alpha = (double) ((double) sumFailures / (double) sumJobs);
        return alpha;
    }
}
