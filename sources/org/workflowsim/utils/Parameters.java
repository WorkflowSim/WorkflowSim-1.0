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

import java.util.List;
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

    
    /*
     * Scheduling Algorithm (Local Scheduling Algorithm)
     */

    public enum SchedulingAlgorithm {

        MAXMIN, MINMIN, MCT, DATA, 
        STATIC, FCFS, ROUNDROBIN, INVALID
    }
    
    /**
     * Planning Algorithm (Global Scheduling Algorithm)
     * 
     */
    public enum PlanningAlgorithm{
        INVALID, RANDOM, HEFT, DHEFT
    }
    
    /**
     * File Type
     */
    public enum FileType{
        NONE(0), INPUT(1), OUTPUT(2);
        public final int value;
        private FileType(int fType){
            this.value = fType;
        }
    }
    
    /**
     * File Type
     */
    public enum ClassType{
        STAGE_IN(1), COMPUTE(2), STAGE_OUT(3), CLEAN_UP(4);
        public final int value;
        private ClassType(int cType){
            this.value = cType;
        }
    }
    
    /**
     * The cost model
     * DATACENTER: specify the cost per data center
     * VM: specify the cost per VM
     */
    public enum CostModel{
        DATACENTER(1), VM(2);
        public final int value;
        private CostModel(int model){
            this.value = model;
        }
    }
    
    /** 
     * Source Host (submit host)
     */
    public static String SOURCE = "source";
    
    public static final int BASE = 0;
    
    /**
     * Scheduling mode
     */
    private static SchedulingAlgorithm schedulingAlgorithm;
    
    /**
     * Planning mode
     */
    private static PlanningAlgorithm planningAlgorithm;
    
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
     * The physical path to DAX files
     */
    private static List<String> daxPaths;
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
    private static final String version = "1.1.0";
    /**
     * Note information
     */
    private static final String note = " supports planning algorithm at Nov 9, 2013";
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
     * the bandwidth from one vm to one vm
     */
    private static double[][] bandwidths;
    
    
    /**
     * The maximum depth. It is inited manually and used in FailureGenerator
     */
    private static int maxDepth;
    
    /**
     * Invalid String
     */
    private static final String INVALID = "Invalid";
    
    /**
     * The scale of runtime. Multiple runtime by this
     */
    private static double runtime_scale = 1.0;
    
    /**
     * The default cost model is based on datacenter, similar to CloudSim
     */
    private static CostModel costModel = CostModel.DATACENTER;
    
    /**
     * A static function so that you can specify them in any place
     *
     * @param vm, the number of vms
     * @param dax, the DAX path
     * @param runtime, optional, the runtime file path
     * @param datasize, optional, the datasize file path
     * @param op, overhead parameters
     * @param cp, clustering parameters
     * @param scheduler, scheduling mode
     * @param planner, planning mode
     * @param rMethod , reducer mode
     * @param dl, deadline
     */
    public static void init(
            int vm, String dax, String runtime, String datasize,
            OverheadParameters op, ClusteringParameters cp,
            SchedulingAlgorithm scheduler, PlanningAlgorithm planner, String rMethod,
            long dl) {

        cParams = cp;
        vmNum = vm;
        daxPath = dax;
        runtimePath = runtime;
        datasizePath = datasize;

        oParams = op;
        schedulingAlgorithm = scheduler;
        planningAlgorithm = planner;
        reduceMethod = rMethod;
        deadline = dl;
        maxDepth = 0;
    }
    
    /**
     * A static function so that you can specify them in any place
     *
     * @param vm, the number of vms
     * @param dax, the list of DAX paths 
     * @param runtime, optional, the runtime file path
     * @param datasize, optional, the datasize file path
     * @param op, overhead parameters
     * @param cp, clustering parameters
     * @param scheduler, scheduling mode
     * @param planner, planning mode
     * @param rMethod , reducer mode
     * @param dl, deadline of a workflow
     */
    public static void init(
            int vm, List<String> dax, String runtime, String datasize,
            OverheadParameters op, ClusteringParameters cp,
            SchedulingAlgorithm scheduler, PlanningAlgorithm planner, String rMethod,
            long dl) {

        cParams = cp;
        vmNum = vm;
        daxPaths = dax;
        runtimePath = runtime;
        datasizePath = datasize;

        oParams = op;
        schedulingAlgorithm = scheduler;
        planningAlgorithm = planner;
        reduceMethod = rMethod;
        deadline = dl;
        maxDepth = 0;
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
     * Gets the reducer mode
     *
     * @return the reducer
     * @pre $none
     * @post $none
     */
    public static String getReduceMethod() {
        if(reduceMethod!=null){
            return reduceMethod;
        }else{
            return INVALID;
        }
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
     * Gets the cost model
     * 
     * @return costModel
     */
    public static CostModel getCostModel(){
        return costModel;
    }
    
    /**
     * Sets the vm number
     *
     * @param num
     */
    public static void setVmNum(int num) {
        vmNum = num;
    }

    /**
     * Gets the clustering parameters
     *
     * @return the clustering parameters
     */
    public static ClusteringParameters getClusteringParameters() {
        return cParams;
    }

    /**
     * Gets the scheduling method
     *
     * @return the scheduling method
     */
    public static SchedulingAlgorithm getSchedulingAlgorithm() {
        return schedulingAlgorithm;
    }
    
    /**
     * Gets the planning method
     * @return the planning method
     * 
     */
    public static PlanningAlgorithm getPlanningAlgorithm() {
        return planningAlgorithm;
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
    
    /**
     * Gets the maximum depth
     * @return the maxDepth
     */
    public static int getMaxDepth(){
        return maxDepth;
    }
    
    /**
     * Sets the maximum depth
     * @param depth the maxDepth
     */
    public static void setMaxDepth(int depth){
        maxDepth = depth;
    }
    
    /**
     * Sets the runtime scale
     * @param scale 
     */
    public static void setRuntimeScale(double scale){
        runtime_scale = scale;
    }
    
    /**
     * Sets the cost model
     * @param model
     */
    public static void setCostModel(CostModel model){
        costModel = model;
    }
    
    /**
     * Gets the runtime scale
     * @return 
     */
    public static double getRuntimeScale(){
        return runtime_scale;
    }
    
    /**
     * Gets the dax paths
     * @return 
     */
    public static List<String> getDAXPaths() {
        return daxPaths;
    }
}
