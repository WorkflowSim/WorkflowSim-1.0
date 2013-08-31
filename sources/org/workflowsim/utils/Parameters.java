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
package org.workflowsim.utils;

import java.util.Map;
import org.cloudbus.cloudsim.Log;

/**
 * This class includes most parameters a user can specify in a configuration
 * file
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class Parameters {

    /**
     * Fault Tolerant Clustering method
     */
    public enum FTCMethod {

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
    /*
     * Scheduling Algorithm
     */

    public enum SCHMethod {

        MAXMIN_SCH, MINMIN_SCH, MCT_SCH, DATA_SCH, 
        STATIC_SCH, FCFS_SCH, INVALID_SCH
    }
    
    /**
     * Planning Algorithm (Global Scheduling Algorithm)
     * 
     */
    public enum PLNMethod{
        INVALID, RANDOM, 
    }
    
    public static final int BASE = 0;
    /**
     * Fault Tolerant Clustering method
     */
    private static FTCMethod FTCMethod;
    /**
     * Fault Tolerant Clustering monitor mode
     */
    private static FTCMonitor monitorMode;
    /**
     * Fault Tolerant Clustering failure generation mode
     */
    private static FTCFailure failureMode;
    /**
     * Scheduling mode
     */
    private static SCHMethod schedulerMode;
    
    /**
     * Planning mode
     */
    private static PLNMethod plannerMode;
    /**
     * Task Failure Rate key = level value = task failure rate
     *
     * @pre 0.0<= value <= 1.0
     */
    private static Map<Integer, Double> alpha;
    /**
     * Reducer mode
     */
    private static String reduceMethod;
    /**
     * Number of vms available
     */
    private static int vmNum;
    /**
     * The physical path to DAX file
     */
    private static String daxPath;
    /**
     * The physical path to runtime file In the runtime file, please use format
     * as below ID1 1.0 ID2 2.0 ... This is optional, if you have specified task
     * runtime in DAX then you don't need to specify this file
     */
    private static String runtimePath;
    /**
     * The physical path to datasize file In the datasize file, please use
     * format as below DATA1 1000 DATA2 2000 ... This is optional, if you have
     * specified datasize in DAX then you don't need to specify this file
     */
    private static String datasizePath;
    /**
     * Version number
     */
    private static final String version = "1.0.0";
    /**
     * Note information
     */
    private static final String note = " fixed all bugs I know at Apr 9, 2013";
    /**
     * Overhead parameters
     */
    private static OverheadParameters oParams;
    /**
     * Clustering parameters
     */
    private static ClusteringParameters cParams;
    /**
     * Deadline of a workflow
     */
    private static long deadline;

    /**
     * A static function so that you can specify them in any place
     *
     * @param fMethod, the fault tolerant clustering method
     * @param monitor, the fault tolerant clustering monitor
     * @param failure, the failure generation mode
     * @param failureList, the task failure list
     * @param vm, the number of vms
     * @param dax, the DAX path
     * @param runtime, optional, the runtime file path
     * @param datasize, optional, the datasize file path
     * @param op, overhead parameters
     * @param cp, clustering parameters
     * @param scheduler, scheduling mode
     * @param planner, planning mode
     * @param rMethod , reducer mode
     * @param deadline, deadline of a workflow
     */
    public static void init(FTCMethod fMethod, FTCMonitor monitor, FTCFailure failure,
            Map failureList,
            int vm, String dax, String runtime, String datasize,
            OverheadParameters op, ClusteringParameters cp,
            SCHMethod scheduler, PLNMethod planner, String rMethod,
            long dl) {

        cParams = cp;
        FTCMethod = fMethod;
        monitorMode = monitor;
        failureMode = failure;
        alpha = failureList;

        vmNum = vm;
        daxPath = dax;
        runtimePath = runtime;
        datasizePath = datasize;

        oParams = op;
        schedulerMode = scheduler;
        plannerMode = planner;
        reduceMethod = rMethod;
        deadline = dl;
    }

    /**
     * Gets the overhead parameters
     *
     * @return the overhead parameters
     * @pre $none
     * @post $none
     */
    public static OverheadParameters getOverheadParams() {
        return oParams;
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
     * Gets the reducer mode
     *
     * @return the reducer
     * @pre $none
     * @post $none
     */
    public static String getReduceMethod() {
        return reduceMethod;
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
     * Gets the DAX path
     *
     * @return the DAX path
     * @pre $none
     * @post $none
     */
    public static String getDaxPath() {
        return daxPath;
    }

    /**
     * Gets the runtime file path
     *
     * @return the runtime file path
     * @pre $none
     * @post $none
     */
    public static String getRuntimePath() {
        return runtimePath;
    }

    /**
     * Gets the data size path
     *
     * @return the datasize file path
     * @pre $none
     * @post $none
     */
    public static String getDatasizePath() {
        return datasizePath;
    }

    /**
     * Gets the fault tolerant clustering method
     *
     * @return the fault tolerant clustering method
     * @pre $none
     * @post $none
     */
    public static FTCMethod getFTCMethod() {
        return FTCMethod;
    }

    /**
     * Gets the vm number
     *
     * @return the vm number
     * @pre $none
     * @post $none
     */
    public static int getVmNum() {
        return vmNum;
    }

    /**
     * Sets the vm number
     *
     * @param vmNum
     * @pre $none
     * @post $none
     */
    public static void setVmNum(int num) {
        vmNum = num;
    }

    /**
     * Gets the clustering parameters
     *
     * @return the clustering parameters
     * @pre $none
     * @post $none
     */
    public static ClusteringParameters getClusteringParameters() {
        return cParams;
    }

    /**
     * Gets the scheduling method
     *
     * @return the scheduling method
     * @pre $none
     * @post $none
     */
    public static SCHMethod getSchedulerMode() {
        return schedulerMode;
    }
    
    /**
     * Gets the planning method
     * @return the planning method
     * 
     */
    public static PLNMethod getPlannerMode() {
        return plannerMode;
    }
    /**
     * Gets the version
     * @return version
     */
    public static String getVersion(){
        return version;
    }

    public static void printVersion() {
        Log.printLine("WorkflowSim Version: " + version);
        Log.printLine("Change Note: " + note);
    }
    /*
     * Gets the deadline
     */
    public static long getDeadline(){
    	return deadline;
    }
    
}
