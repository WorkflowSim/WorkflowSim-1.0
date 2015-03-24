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
package org.workflowsim.examples.clustering.balancing;

import java.io.File;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.workflowsim.CondorVM;
import org.workflowsim.WorkflowDatacenter;
import org.workflowsim.Job;
import org.workflowsim.WorkflowEngine;
import org.workflowsim.WorkflowPlanner;
import org.workflowsim.examples.clustering.HorizontalClusteringExample1;
import org.workflowsim.utils.DistributionGenerator;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;

/**
 * This BalancedClusteringExample1 is using balanced horizontal clustering or
 * more specifically using horizontal runtime balancing.
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Dec 29, 2013
 */
public class BalancedClusteringExample1 extends HorizontalClusteringExample1 {

    ////////////////////////// STATIC METHODS ///////////////////////
    /**
     * Creates main() to run this example This example has only one datacenter
     * and one storage
     */
    public static void main(String[] args) {

        try {

            /**
             * TODO(chenww05): delete in the future
             *
             */
            String code = "i";
            String daxPath = "/Users/weiweich/NetBeansProjects/WorkflowSim-1.0/config/dax/Montage_100.xml";
            double c_delay = 0.0, q_delay = 0.0, e_delay = 0.0, p_delay = 0.0;
            int interval = 0;

            for (int i = 0; i < args.length; i++) {
                char key = args[i].charAt(1);
                switch (key) {
                    case 'c':
                        code = args[++i];
                        break;
                    case 'd':
                        daxPath = args[++i];
                        break;
                    case 'l':
                        c_delay = Double.parseDouble(args[++i]);
                        break;
                    case 'q':
                        q_delay = Double.parseDouble(args[++i]);
                        break;
                    case 'e':
                        e_delay = Double.parseDouble(args[++i]);
                        break;
                    case 'p':
                        p_delay = Double.parseDouble(args[++i]);
                        break;
                    case 'i':
                        interval = Integer.parseInt(args[++i]);
                        break;
                }
            }

            // First step: Initialize the WorkflowSim package. 
            /**
             * However, the exact number of vms may not necessarily be vmNum If
             * the data center or the host doesn't have sufficient resources the
             * exact vmNum would be smaller than that. Take care.
             */
            int vmNum = 20;//number of vms;
            File daxFile = new File(daxPath);
            if (!daxFile.exists()) {
                Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
                return;
            }

            /**
             * Since we are using MINMIN scheduling algorithm, the planning
             * algorithm should be INVALID such that the planner would not
             * override the result of the scheduler
             */
            Parameters.SchedulingAlgorithm sch_method = Parameters.SchedulingAlgorithm.DATA;
            Parameters.PlanningAlgorithm pln_method = Parameters.PlanningAlgorithm.INVALID;
            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.LOCAL;

            /**
             * clustering delay must be added, if you don't need it, you can set
             * all the clustering delay to be zero, but not null
             */
            Map<Integer, DistributionGenerator> clusteringDelay = new HashMap();
            Map<Integer, DistributionGenerator> queueDelay = new HashMap();
            Map<Integer, DistributionGenerator> postscriptDelay = new HashMap();
            Map<Integer, DistributionGenerator> engineDelay = new HashMap();
            /**
             * application has at most 11 horizontal levels
             */
            int maxLevel = 11;
            for (int level = 0; level < maxLevel; level++) {
                if (c_delay != 0.0) {
                    DistributionGenerator cluster_delay = new DistributionGenerator(DistributionGenerator.DistributionFamily.WEIBULL, c_delay, 1.0);
                    clusteringDelay.put(level, cluster_delay);
                }
                if (q_delay != 0.0) {
                    DistributionGenerator queue_delay = new DistributionGenerator(DistributionGenerator.DistributionFamily.WEIBULL, q_delay, 1.0);
                    queueDelay.put(level, queue_delay);
                }
                if (p_delay != 0.0) {
                    DistributionGenerator postscript_delay = new DistributionGenerator(DistributionGenerator.DistributionFamily.WEIBULL, p_delay, 1.0);
                    postscriptDelay.put(level, postscript_delay);
                }
                if (e_delay != 0.0) {
                    DistributionGenerator engine_delay = new DistributionGenerator(DistributionGenerator.DistributionFamily.WEIBULL, e_delay, 1.0);
                    engineDelay.put(level, engine_delay);
                }
            }

            OverheadParameters op = new OverheadParameters(interval, engineDelay, queueDelay, postscriptDelay, clusteringDelay, 0);

            /**
             * Balanced Clustering
             */
            ClusteringParameters.ClusteringMethod method = ClusteringParameters.ClusteringMethod.BALANCED;
            /**
             * r: Horizontal Runtime Balancing (HRB) d: Horizontal Distance
             * Balancing (HDB) i: Horizontal Impact Factor Balancing (HIFB) h:
             * Horizontal Random Balancing , the original horizontal clustering
             */
            ClusteringParameters cp = new ClusteringParameters(20, 0, method, code);

            /**
             * Initialize static parameters
             */
            Parameters.init(vmNum, daxPath, null,
                    null, op, cp, sch_method, pln_method,
                    null, 0);
            ReplicaCatalog.init(file_system);

            // before creating any entities.
            int num_user = 1;   // number of grid users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;  // mean trace events

            // Initialize the CloudSim library
            CloudSim.init(num_user, calendar, trace_flag);

            WorkflowDatacenter datacenter0 = createDatacenter("Datacenter_0");

            /**
             * Create a WorkflowPlanner with one schedulers.
             */
            WorkflowPlanner wfPlanner = new WorkflowPlanner("planner_0", 1);
            /**
             * Create a WorkflowEngine.
             */
            WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();
            /**
             * Create a list of VMs.The userId of a vm is basically the id of
             * the scheduler that controls this vm.
             */
            List<CondorVM> vmlist0 = createVM(wfEngine.getSchedulerId(0), Parameters.getVmNum());

            /**
             * Submits this list of vms to this WorkflowEngine.
             */
            wfEngine.submitVmList(vmlist0, 0);

            /**
             * Binds the data centers with the scheduler.
             */
            wfEngine.bindSchedulerDatacenter(datacenter0.getId(), 0);

            CloudSim.startSimulation();
            List<Job> outputList0 = wfEngine.getJobsReceivedList();
            CloudSim.stopSimulation();
            printJobList(outputList0);
        } catch (Exception e) {
            Log.printLine("The simulation has been terminated due to an unexpected error");
        }
    }
}
