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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.workflowsim.CondorVM;
import org.workflowsim.DatacenterExtended;
import org.workflowsim.DistributedClusterStorage;
import org.workflowsim.Job;
import org.workflowsim.WorkflowEngine;
import org.workflowsim.WorkflowPlanner;
import org.workflowsim.examples.clustering.HorizontalClusteringExample1;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;

/**
 * This BalancedClusteringExample1 is using balanced horizontal clustering or more specifically
 * using horizontal runtime balancing. 
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Dec 29, 2013
 */
public class BalancedClusteringExample1 extends HorizontalClusteringExample1 {

    ////////////////////////// STATIC METHODS ///////////////////////
    /**
     * Creates main() to run this example
     * This example has only one datacenter and one storage
     */
    public static void main(String[] args) {


       try {
           
           /**
            * delete in the future
            * 
            */
           String code = "i";
           String daxPath = "/Users/chenweiwei/Work/WorkflowSim-1.0/config/dax/Montage_100.xml";
           double intraBandwidth = 1.5e5;
           double c_delay = 0, q_delay = 0, e_delay = 0, p_delay = 0;
           int interval = 0;
           
           for(int i = 0; i < args.length; i ++){
               char key = args[i].charAt(1);
               switch(key){
                   case 'c':
                       code = args[++i];
                       break;
                   case 'd':
                       daxPath = args[++i];
                       break;
                   case 'b':
                       intraBandwidth = Double.parseDouble(args[++i]);
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
            /**
             * Should change this based on real physical path
             */
            //String daxPath = "/Users/chenweiwei/Research/balanced_clustering/generator/BharathiPaper/Fake_1.xml";
            if(daxPath == null){
                Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
                return;
            }
            File daxFile = new File(daxPath);
            if(!daxFile.exists()){
                Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
                return;
            }

            /**
             * Since we are using MINMIN scheduling algorithm, the planning algorithm should be INVALID 
             * such that the planner would not override the result of the scheduler
             */
            Parameters.SchedulingAlgorithm sch_method = Parameters.SchedulingAlgorithm.DATA;
            Parameters.PlanningAlgorithm pln_method = Parameters.PlanningAlgorithm.INVALID;
            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.LOCAL;

            /**
             * clustering delay must be added, if you don't need it, you can set all the clustering
             * delay to be zero, but not null
             */
            Map<Integer, Double> clusteringDelay = new HashMap();
            Map<Integer, Double> queueDelay = new HashMap();
            Map<Integer, Double> postscriptDelay = new HashMap();
            Map<Integer, Double> engineDelay = new HashMap();
            /**
             * application has at most 11 horizontal levels 
             */
            int maxLevel = 11;
            for (int level = 0; level < maxLevel; level++ ){
                clusteringDelay.put(level, c_delay);
                queueDelay.put(level, q_delay);
                postscriptDelay.put(level, p_delay);
                engineDelay.put(level, e_delay);
            }
            // Add clustering delay to the overhead parameters
            /**
             * Map<Integer, Double> wed_delay,
            Map<Integer, Double> queue_delay,
            Map<Integer, Double> post_delay,
            Map<Integer, Double> cluster_delay,
             */
            OverheadParameters op = new OverheadParameters(interval, engineDelay, queueDelay, postscriptDelay, clusteringDelay, 0);;
            
            /**
             * Balanced Clustering
             */
            ClusteringParameters.ClusteringMethod method = ClusteringParameters.ClusteringMethod.BALANCED;
            /**
             * r: Horizontal Runtime Balancing (HRB)
             * d: Horizontal Distance Balancing (HDB)
             * i: Horizontal Impact Factor Balancing (HIFB)
             * h: Horizontal Random Balancing , the original horizontal clustering
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

            DatacenterExtended datacenter0 = createDatacenter("Datacenter_0", intraBandwidth);

            /**
             * Create a WorkflowPlanner with one schedulers.
             */
            WorkflowPlanner wfPlanner = new WorkflowPlanner("planner_0", 1);
            /**
             * Create a WorkflowEngine.
             */
            WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();
            /**
             * Create a list of VMs.The userId of a vm is basically the id of the scheduler
             * that controls this vm. 
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

    /**
     * Creates a new Data Center object
     * @param name String name
     * @param intraBandwidth bandwidth
     * @return 
     */
    protected static DatacenterExtended createDatacenter(String name , double intraBandwidth) {

        // Here are the steps needed to create a PowerDatacenter:
        // 1. We need to create a list to store one or more
        //    Machines
        int vmNumber = 20;
        List<Host> hostList = new ArrayList<Host>();

        // 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
        //    create a list to store these PEs before creating
        //    a Machine.
        for (int i = 1; i <= vmNumber; i++) {
            List<Pe> peList1 = new ArrayList<Pe>();
            int mips = 2000;
            // 3. Create PEs and add these into the list.
            //for a quad-core machine, a list of 4 PEs is required:
            peList1.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
            peList1.add(new Pe(1, new PeProvisionerSimple(mips)));

            int hostId = 0;
            int ram = 2048; //host memory (MB)
            long storage = 1000000; //host storage
            int bw = 10000;
            hostList.add(
                    new Host(
                    hostId,
                    new RamProvisionerSimple(ram),
                    new BwProvisionerSimple(bw),
                    storage,
                    peList1,
                    new VmSchedulerTimeShared(peList1))); // This is our first machine
            hostId++;

        }

        // 5. Create a DatacenterCharacteristics object that stores the
        //    properties of a data center: architecture, OS, list of
        //    Machines, allocation policy: time- or space-shared, time zone
        //    and its price (G$/Pe time unit).
        String arch = "x86";      // system architecture
        String os = "Linux";          // operating system
        String vmm = "Xen";
        double time_zone = 10.0;         // time zone this resource located
        double cost = 3.0;              // the cost of using processing in this resource
        double costPerMem = 0.05;		// the cost of using memory in this resource
        double costPerStorage = 0.1;	// the cost of using storage in this resource
        double costPerBw = 0.1;			// the cost of using bw in this resource
        LinkedList<Storage> storageList = new LinkedList<Storage>();	//we are not adding SAN devices by now
        DatacenterExtended datacenter = null;


        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);


        // 6. Finally, we need to create a cluster storage object.
        /**
         * The bandwidth within a data center.
         */
        //double intraBandwidth = 1.5e3;// the number comes from the futuregrid site, you can specify your bw
        
        try {
            DistributedClusterStorage s1 = new DistributedClusterStorage(name, 1e12, vmNumber, intraBandwidth / 2);
            
            // The bandwidth from one vm to another vm
            for(int source = 0; source < vmNumber; source ++){
                for(int destination = 0; destination < vmNumber; destination ++){
                    if(source == destination){
                        continue;
                    }
                    s1.setBandwidth(source, destination, intraBandwidth);
                }
            }
            storageList.add(s1);
            datacenter = new DatacenterExtended(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
        }

        return datacenter;
    }

    
}
