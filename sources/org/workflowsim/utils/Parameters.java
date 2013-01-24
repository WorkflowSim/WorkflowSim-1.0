/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.workflowsim.utils;

import java.util.List;
import java.util.Map;
import org.cloudbus.cloudsim.Log;

/**
 *
 * @author Weiwei Chen
 */
public class Parameters {
    
    

    /**
     * Fault Tolerant Clustering method
     */
    public enum FTCMethod {
        FTCLUSTERING_DC, FTCLUSTERING_SR, FTCLUSTERING_DR, FTCLUSTERING_NOOP, FTCLUSTERING_BLOCK, FTCLUSTERING_BINARY
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
        MAXMIN_SCH, MINMIN_SCH, ROUNDR_SCH, HEFT_SCH, MCT_SCH
    }

    public static final int BASE = 0;
    
    private static FTCMethod FTCMethod;

    
    private static FTCMonitor monitorMode;
    
    
    
    private static FTCFailure failureMode;
    

    
    private static SCHMethod schedulerMode;
    
    
    
    private static Map<Integer, Double> alpha;
    

    private static String reduceMethod;
    
    
    
    private static int vmNum;
    
    private static String daxPath;
    
    private static String runtimePath;
    
    private static String datasizePath;
    

    private static final String version     =  "1.2.0";
    
    private static final String note        = " fixed all bugs I know at Aug 20, 2012";
    
    private static OverheadParameters oParams;
    
    private static ClusteringParameters cParams;
    
    public static void init( FTCMethod fMethod, FTCMonitor monitor, FTCFailure failure, 
            Map failureList,
            int vm, String dax, String runtime, String datasize, 
            OverheadParameters op, ClusteringParameters cp , 
            SCHMethod scheduler, String rMethod){

        cParams              = cp;
        FTCMethod            = fMethod;
        monitorMode          = monitor;
        failureMode          = failure;
        alpha                = failureList;

        vmNum                = vm;
        daxPath              = dax;
        runtimePath          = runtime;
        datasizePath         = datasize;

        oParams              = op;
        schedulerMode        = scheduler;
        reduceMethod         = rMethod;
    }
    
    
    
    public static OverheadParameters getOverheadParams(){
        return oParams;
    }
    
    
    public static Map getAlpha(){
        return alpha;
        
    }
    public static String getReduceMethod(){
        return reduceMethod;
    }

    public static FTCFailure getFailureGeneratorMode(){
        return failureMode;
    }
    
    public static FTCMonitor getMonitorMode(){
        return monitorMode;
    }
    
    public static String getDaxPath(){
        return daxPath;
    }
    
    public static String getRuntimePath(){
        return runtimePath;
    }
    
    public static String getDatasizePath(){
        return datasizePath;
    }
    public static FTCMethod getFTCMethod(){
        return FTCMethod;
    }
    
    public static int getVmNum(){
        return vmNum;
    }

    public static ClusteringParameters getClusteringParameters(){
        return cParams;
    }
    
    public static SCHMethod getSchedulerMode(){
        return schedulerMode;
    }

    public static void printVersion(){
        Log.printLine("WorkflowSim Version: " + version);
        Log.printLine("Change Note: " + note);
    }
}
