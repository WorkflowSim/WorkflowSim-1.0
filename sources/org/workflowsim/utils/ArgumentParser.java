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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.utils.ClusteringParameters.ClusteringMethod;
import org.workflowsim.utils.Parameters.FTCFailure;
import org.workflowsim.utils.Parameters.FTCluteringAlgorithm;
import org.workflowsim.utils.Parameters.FTCMonitor;
import org.workflowsim.utils.Parameters.PlanningAlgorithm;
import org.workflowsim.utils.Parameters.SchedulingAlgorithm;
import org.workflowsim.utils.ReplicaCatalog.FileSystem;

/**
 * This class implements ArgumentParser that parse arguments and set the
 * parameters
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class ArgumentParser {

    /**
     * This method initialize a ArgumentParser object
     *
     * @param args argument set in WorkflowSim
     */
    public ArgumentParser(String[] args) {

        int i = 0;
        String properties = null;
        String code = null;
        String dax = null;
        double bandwidth = 2e8; // by default 
        String clustering = null;
        while (i < args.length) {
            switch (args[i].charAt(1)) {
                case 'p':
                    properties = args[++i];
                    break;
                case 'd':
                    dax = args[++i];
                    break;
                case 'c':
                    code = args[++i];
                    break;
                case 'k':
                    clustering = args[++i];
                    break;
                case 'b':
                    bandwidth = Double.parseDouble(args[++i]);
                    break;
                case 'h':

                    break;
                case 'v':

                default: //same as help
                    printVersion();
                    System.exit(0);
            }
            i++;
        }

        try {
            if (properties == null) {
                throw new Exception("Properties File Not specified. "
                        + "Please add \"-p path_to_your_config_file\" to your arguments");

            }
            File file = new File(properties);
            if (!file.exists() || !file.canRead()) {
                throw new Exception("Properties File Not Reachable. Please use physical path");

            }
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = null;
            int cSize = 0;
            int cNum = 0;
            int interval = 0;
            int vmNum = 0;
            long deadline = 0;
            
            String datasizePath = null;
            String runtimePath = null;
            String daxPath = null;
            String cMethod = null;
            String rMethod = null;
            FTCluteringAlgorithm ftc_method = null;
            FTCMonitor ftc_monitor = FTCMonitor.MONITOR_NONE;
            FTCFailure ftc_failure = FTCFailure.FAILURE_NONE;
            SchedulingAlgorithm sch_method = SchedulingAlgorithm.INVALID_SCH;
            PlanningAlgorithm pln_method = PlanningAlgorithm.INVALID;
            FileSystem file_system = FileSystem.SHARED;
            OverheadParameters op = null;
            ClusteringParameters cp = null;
            Map<Integer, Double> WEDelay = new HashMap<Integer, Double>();
            Map<Integer, Double> QueueDelay = new HashMap<Integer, Double>();
            Map<Integer, Double> PostDelay = new HashMap<Integer, Double>();
            Map<Integer, Double> ClustDelay = new HashMap<Integer, Double>();
            Map<Integer, Double> failureMap = new HashMap<Integer, Double>();
            while ((line = br.readLine()) != null) {
                line = line.replaceAll("\\s", "");
                if (line.startsWith("#") || line.startsWith("//") || !line.contains("=")) {
                    continue;
                }
                String[] items = line.split("=");
                if (items == null || items.length != 2) {
                    throw new Exception("Config File Format Error: key = value");

                }
                String key = items[0];
                String value = items[1];
                if (key.equals("dax.path")) {
                    daxPath = value;
                } else if (key.equals("runtime.path")) {
                    runtimePath = value;
                } else if (key.equals("size.path")) {
                    datasizePath = value;
                } else if (key.equals("file.system")) {
                    file_system = FileSystem.valueOf(value);
                } else if (key.equals("clusters.num")) {
                    cNum = Integer.parseInt(value);
                } else if (key.equals("clusters.size")) {
                    cSize = Integer.parseInt(value);
                } else if (key.equals("clusters.method")) {
                    cMethod = value;
                } else if (key.equals("interval")) {
                    interval = Integer.parseInt(value);
                } else if (key.equals("vm.num")) {
                    vmNum = Integer.parseInt(value);
                } else if (key.equals("reduce.method")) {
                    rMethod = value;
                } else if (key.equals("ftc.method")) {
                    ftc_method = FTCluteringAlgorithm.valueOf(value);
                } else if (key.equals("ftc.monitor")) {
                    ftc_monitor = FTCMonitor.valueOf(value);
                } else if (key.equals("ftc.failure")) {
                    ftc_failure = FTCFailure.valueOf(value);
                } else if (key.equals("scheduler.method")) {
                    sch_method = SchedulingAlgorithm.valueOf(value);
                } else if (key.equals("planner.method")){
                    pln_method = PlanningAlgorithm.valueOf(value);
                } else if (key.equals("bandwidth")) {
                    bandwidth = Double.parseDouble(value);
                } else if (key.equals("deadline")) {
                	deadline = Long.parseLong(value);
                } else {
                    /**
                     * Set overheads per level(depth)
                     */
                    switch (key.charAt(0)) {
                        case 'd':
                            int depth_d = Integer.parseInt(key.substring(1));
                            if (depth_d >= 0) {
                                double delay = Double.parseDouble(value);
                                if (delay >= 0.0) {
                                    WEDelay.put(depth_d, delay);
                                } else {
                                    throw new Exception("Make sure workflow engine delay is >= 0.0");
                                }
                            } else {
                                throw new Exception("Not Supported");
                            }
                            break;
                        case 'q':
                            int depth_q = Integer.parseInt(key.substring(1));
                            if (depth_q >= 0) {
                                double delay = Double.parseDouble(value);
                                if (delay >= 0.0) {
                                    QueueDelay.put(depth_q, delay);
                                } else {
                                    throw new Exception("Make sure queue delay is >= 0.0");
                                }
                            } else {
                                throw new Exception("Not Supported");
                            }

                            break;
                        case 'c':
                            int depth_c = Integer.parseInt(key.substring(1));
                            if (depth_c >= 0) {
                                double delay = Double.parseDouble(value);
                                if (delay >= 0.0) {
                                    ClustDelay.put(depth_c, delay);
                                } else {
                                    throw new Exception("Make sure clustering delay is >= 0.0");
                                }
                            } else {
                                throw new Exception("Not Supported");
                            }

                            break;
                        case 'p':
                            int depth_p = Integer.parseInt(key.substring(1));
                            if (depth_p >= 0) {
                                double delay = Double.parseDouble(value);
                                if (delay >= 0.0) {
                                    PostDelay.put(depth_p, delay);
                                } else {
                                    throw new Exception("Make sure postscript delay is >= 0.0");
                                }
                            } else {
                                throw new Exception("Not Supported");
                            }

                            break;
                        case 'a':
                            int depth_a = Integer.parseInt(key.substring(1));
                            if (depth_a >= 0) {
                                double failure = Double.parseDouble(value);
                                if (failure >= 0.0 && failure <= 1.0) {
                                    failureMap.put(depth_a, failure);
                                } else {
                                    throw new Exception("Make sure task failure rate is between [0,1]");
                                }
                            } else {
                                throw new Exception("Not Supported");
                            }

                            break;
                        default:
                            throw new Exception("Not Supported:key=" + key + " value=" + value);



                    }
                }
            }


            if (dax != null) {
                daxPath = dax;
            }

            br.close();

            /**
             * Initialize a new OverheadParameters
             */
            op = new OverheadParameters(interval, WEDelay, QueueDelay, PostDelay,
                    ClustDelay, bandwidth);
            ClusteringMethod method = null;
            if (cMethod != null) {
                method = ClusteringMethod.valueOf(cMethod.toUpperCase());
            } else {
                /**
                 * By default it is no clustering
                 */
                method = ClusteringMethod.NONE;
            }
            if (clustering != null) {
                method = ClusteringMethod.valueOf(clustering.toUpperCase());
            }
            /**
             * Initialize a new ClusteringParameters
             */
            cp = new ClusteringParameters(cNum, cSize, method, code);
            /**
             * Be default, task failure rates are 0.0
             */
            if (!failureMap.containsKey((int) 0)) {
                failureMap.put(0, 0.0);
            }
            
            /**
             * If a user has specified planner.method, the scheduler.method should 
             * be set as STATIC_SCH to avoid unnecessary change during the runtime
             */
            if(!pln_method.equals(PlanningAlgorithm.INVALID)){
                if(sch_method != SchedulingAlgorithm.STATIC_SCH || sch_method != SchedulingAlgorithm.INVALID_SCH){
                    Log.printLine("Warning: your scheduler.method is reset to be STATIC_SCH");
                }
                sch_method = SchedulingAlgorithm.STATIC_SCH;
            }
            
            Parameters.init(ftc_method, ftc_monitor, ftc_failure,
                    failureMap, vmNum, daxPath, runtimePath,
                    datasizePath, op, cp, sch_method, pln_method,
                    rMethod, deadline);
            ReplicaCatalog.init(file_system);
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Error in config format");
            System.exit(1);
        }



    }

    public void printVersion() {
        Parameters.printVersion();
    }
}
