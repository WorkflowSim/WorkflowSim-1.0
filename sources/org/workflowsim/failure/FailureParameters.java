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
import org.workflowsim.utils.DistributionGenerator;
import org.workflowsim.utils.DistributionGenerator.DistributionFamily;

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
    private static DistributionGenerator[][] generators;
    /**
     * Fault Tolerant Clustering algorithm
     */
    public enum FTCluteringAlgorithm {

        FTCLUSTERING_DC, FTCLUSTERING_SR, FTCLUSTERING_DR, FTCLUSTERING_NOOP,
        FTCLUSTERING_BLOCK, FTCLUSTERING_VERTICAL
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
     * The distribution of the failure 
     */
    private static DistributionFamily distribution = DistributionFamily.WEIBULL;
    /**
     * Invalid return value
     */
    private static final int INVALID = -1;

    /**
     * 
     *  Init a FailureParameters
     * 
     * @param fMethod Fault Tolerant Clustering Algorithm
     * @param monitor Fault Tolerant Clustering Monitor mode
     * @param failure Failure generator mode
     * @param failureGenerators
     */
    public static void init(FTCluteringAlgorithm fMethod, FTCMonitor monitor, 
            FTCFailure failure, DistributionGenerator[][] failureGenerators) {
        FTClusteringAlgorithm = fMethod;
        monitorMode = monitor;
        failureMode = failure;
        generators = failureGenerators;
    }

    /**
     * 
     * Init a FailureParameters with distibution
     * @param fMethod
     * @param monitor
     * @param dist 
     * @param failureGenerators 
     * @param failure 
     */
    public static void init(FTCluteringAlgorithm fMethod, FTCMonitor monitor, 
            FTCFailure failure, DistributionGenerator[][] failureGenerators, 
            DistributionFamily dist) {
        distribution = dist;
        init(fMethod, monitor, failure, failureGenerators);
    }
    /**
     * Gets the task failure rate
     *
     * @return the task failure rate
     * @pre $none
     * @post $none
     */
    public static DistributionGenerator[][] getFailureGenerators() {
        if(generators==null){
            Log.printLine("ERROR: alpha is not initialized");
        }
        return generators;
    }
    
    /**
     * Gets the max first index in alpha
     * @return max
     */
    public static int getFailureGeneratorsMaxFirstIndex(){
        if(generators==null || generators.length == 0){
            Log.printLine("ERROR: alpha is not initialized");
            return INVALID;
        }
        return generators.length;
    }
    
    /**
     * Gets the max second Index in alpha
     * @return max
     */
    public static int getFailureGeneratorsMaxSecondIndex(){
        //Test whether it is valid
        getFailureGeneratorsMaxFirstIndex();
        if(generators[0]==null || generators[0].length == 0){
            Log.printLine("ERROR: alpha is not initialized");
            return INVALID;
        }
        return generators[0].length;
    }
    

    /**
     * Gets the task failure rate
     * @param vmIndex vm Index
     * @param taskDepth task depth
     * @return task failure rate
     */
    public static DistributionGenerator getGenerator(int vmIndex, int taskDepth) {
        return generators[vmIndex][taskDepth];
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
     * Gets the failure distribution
     * @return distribution
     */
    public static DistributionFamily getFailureDistribution(){
        return distribution;
    }
}
