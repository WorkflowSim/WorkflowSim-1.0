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

import org.cloudbus.cloudsim.Log;

/**
 *
 * @author chenweiwei
 */
public class FailureParameters {

    /**
     * Task Failure Rate 
     * first index is vmId ;second index is task depth
     * If FAILURE_JOB is specified first index is 0 only
     * If FAILURE_VM is specified second index is 0 only
     *
     * @pre 0.0<= value <= 1.0
     */
    private static double[][] alpha;
    private static double[][] beta;
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

        MONITOR_NONE, MONITOR_ALL, MONITOR_VM, MONITOR_JOB, MONITOR_VM_JOB
    }
    /*
     * FTC Failure Generator mode
     */

    public enum FTCFailure {

        FAILURE_NONE, FAILURE_ALL, FAILURE_VM, FAILURE_JOB, FAILURE_VM_JOB
    }
    /**
     * Fault Tolerant Clustering method
     */
    private static FTCluteringAlgorithm FTClusteringAlgorithm = FTCluteringAlgorithm.FTCLUSTERING_NOOP;
    /**
     * Fault Tolerant Clustering monitor mode
     */
    private static FTCMonitor monitorMode = FTCMonitor.MONITOR_NONE;
    /**
     * Fault Tolerant Clustering failure generation mode
     */
    private static FTCFailure failureMode = FTCFailure.FAILURE_NONE;
    /**
     * The failure sample size. make sure: mean * size > makespan But don't set
     * it to be too high since it has memory cost Only used when FAILURE is turn
     * on
     */
    private static int FAILURE_SAMPLE_SIZE = 1000;
    
    /**
     * Invalid return value
     */
    private static int INVALID = -1;

    public static void init(FTCluteringAlgorithm fMethod, FTCMonitor monitor, 
            FTCFailure failure, double[][] failureRate, double[][] failureShape) {
        FTClusteringAlgorithm = fMethod;
        monitorMode = monitor;
        failureMode = failure;
        alpha = failureRate;
        beta = failureShape;
    }

    /**
     * Gets the task failure rate
     *
     * @return the task failure rate
     * @pre $none
     * @post $none
     */
    public static double[][] getAlpha() {
        if(alpha==null){
            Log.printLine("ERROR: alpha is not initialized");
        }
        return alpha;
    }
    
    /**
     * Gets the max first index in alpha
     * @return max
     */
    public static int getAlphaMaxFirstIndex(){
        if(alpha==null || alpha.length == 0){
            Log.printLine("ERROR: alpha is not initialized");
            return INVALID;
        }
        return alpha.length;
    }
    
    /**
     * Gets the max second Index in alpha
     * @return max
     */
    public static int getAlphaMaxSecondIndex(){
        //Test whether it is valid
        getAlphaMaxFirstIndex();
        if(alpha[0]==null || alpha[0].length == 0){
            Log.printLine("ERROR: alpha is not initialized");
            return INVALID;
        }
        return alpha[0].length;
    }
    

    /**
     * Gets the task failure rate
     * @param vmIndex vm Index
     * @param taskDepth task depth
     * @return task failure rate
     */
    public static double getAlpha(int vmIndex, int taskDepth) {
        return alpha[vmIndex][taskDepth];
    }
    
    /**
     * Second parameter of failure model
     *
     * @return the beta
     * @pre $none
     * @post $none
     */
    public static double[][] getBeta() {
        return beta;
    }

    /**
     * Gets the second parameter of failure model
     * @param vmIndex vmIndex
     * @param taskDepth taskDepth
     * @return second parameter
     */
    public static double getBeta(int vmIndex, int taskDepth){
        return beta[vmIndex][taskDepth];
    }
    
    
        /**
     * Gets the max first index in alpha
     * @return max
     */
    public static int getBetaMaxFirstIndex(){
        if(beta==null || beta.length == 0){
            Log.printLine("ERROR: beta is not initialized");
            return INVALID;
        }
        /**
         * the length of alpha and beta should be the same
         */
        if(beta.length != getAlphaMaxFirstIndex()){
            Log.printLine("ERROR: beta is not initialized correctly");
            return INVALID;
        }
        return beta.length;
    }
    
    /**
     * Gets the max second Index in alpha
     * @return max
     */
    public static int getBetaMaxSecondIndex(){
        //Test whether it is valid
        getBetaMaxFirstIndex();
        if(beta[0]==null || beta[0].length == 0){
            Log.printLine("ERROR: beta is not initialized");
            return INVALID;
        }
        
        /**
         * the length of alpha and beta should be the same
         */
        if(beta[0].length != getBetaMaxFirstIndex()){
            Log.printLine("ERROR: beta is not initialized correctly");
            return INVALID;
        }
        return beta[0].length;
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
     *
     * @return failure sample size
     */
    public static int getFailureSampleSize() {
        return FAILURE_SAMPLE_SIZE;
    }

    /**
     * Sets the failure sample size according to makespan. make sure: mean *
     * size > makespan
     */
    public static void setFailureSampleSize(int size) {
        FAILURE_SAMPLE_SIZE = size;
    }
}
