/*
 * 
 *   Copyright 2012-2013 University Of Southern California
 * 
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 * 
 */
package org.workflowsim.failure;

import java.util.Map;
import org.cloudbus.cloudsim.Log;

/**
 *
 * @author chenweiwei
 */
public class FailureParameters {
    
    /**
     * Task Failure Rate key = level value = task failure rate
     *
     * @pre 0.0<= value <= 1.0
     */
    private static Map<Integer, Double> alpha;
    
    /**
     * Fault Tolerant Clustering algorithm
     */
    public enum FTCluteringAlgorithm {

        FTCLUSTERING_DC, FTCLUSTERING_SR, FTCLUSTERING_DR, FTCLUSTERING_NOOP, 
        FTCLUSTERING_BLOCK, FTCLUSTERING_BINARY
    }
    /*
     * FTC Monitor mode
     */

    public enum FTCMonitor {

        MONITOR_NONE, MONITOR_ALL, MONITOR_VM, MONITOR_JOB
    }
    /*
     * FTC Failure Generator mode
     */

    public enum FTCFailure {

        FAILURE_NONE, FAILURE_ALL, FAILURE_VM, FAILURE_JOB
    }
    
    /**
     * Fault Tolerant Clustering method
     */
    private static FTCluteringAlgorithm FTClusteringAlgorithm;
    
    /**
     * Fault Tolerant Clustering monitor mode
     */
    private static FTCMonitor monitorMode;
    
    /**
     * Fault Tolerant Clustering failure generation mode
     */
    private static FTCFailure failureMode;
    
    /**
     * The failure sample size. 
     * make sure: mean * size > makespan
     * But don't set it to be too high since it has memory cost
     * Only used when FAILURE is turn on
     */
    private static int FAILURE_SAMPLE_SIZE = 1000;
    
    public static void init(FTCluteringAlgorithm fMethod, FTCMonitor monitor, FTCFailure failure, Map failureList){
         FTClusteringAlgorithm = fMethod;
        monitorMode = monitor;
        failureMode = failure;
        alpha = failureList;
    }
    /**
     * Gets the task failure rate
     *
     * @return the task failure rate
     * @pre $none
     * @post $none
     */
    public static Map getAlpha() {
        return alpha;

    }

    /**
     * Gets the job failure rate (not supported yet)
     *
     * @return the job failure rate
     * @pre $none
     * @post $none
     */
    public static Map getBeta() {
        Log.printLine("Not supported");
        return null;
    }
    
     /**
     * Gets the failure generation mode
     *
     * @return the failure generation mode
     * @pre $none
     * @post $none
     */
    public static FTCFailure getFailureGeneratorMode() {
        return failureMode;
    }

    /**
     * Gets the fault tolerant clustering monitor mode
     *
     * @return the fault tolerant clustering monitor mode
     * @pre $none
     * @post $none
     */
    public static FTCMonitor getMonitorMode() {
        return monitorMode;
    }
    
    /**
     * Gets the fault tolerant clustering method
     *
     * @return the fault tolerant clustering method
     * @pre $none
     * @post $none
     */
    public static FTCluteringAlgorithm getFTCluteringAlgorithm() {
        return FTClusteringAlgorithm;
    }
    
    /**
     * Gets the failure sample size (it is related to the makespan)
     * @return failure sample size
     */
    public static int getFailureSampleSize(){
        return FAILURE_SAMPLE_SIZE;
    }
    
    /**
     * Sets the failure sample size according to makespan. 
     * make sure: mean * size > makespan
     */
    public static void setFailureSampleSize(int size){
        FAILURE_SAMPLE_SIZE = size;
    }

}
