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
package org.workflowsim.examples.depreciate;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
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
import org.workflowsim.ClusterStorage;
import org.workflowsim.CondorVM;
import org.workflowsim.DatacenterExtended;
import org.workflowsim.Job;
import org.workflowsim.WorkflowEngine;
import org.workflowsim.WorkflowPlanner;
import org.workflowsim.failure.FailureGenerator;
import org.workflowsim.failure.FailureMonitor;
import org.workflowsim.utils.ArgumentParser;
import org.workflowsim.utils.Parameters;

/**
 * This WorkflowSimExample creates a workflow planner, a workflow engine, and
 * two schedulers, two data centers and 20 vms. All the configuration of
 * CloudSim is done in WorkflowSimExamplex.java All the configuration of
 * WorkflowSim is done in the config.txt that must be specified in argument of
 * this WorkflowSimExample. The argument should have at least: "-p
 * path_to_config.txt"
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class WorkflowSimExample2 {

    private static List<CondorVM> createVM(int userId, int vms, int vmIdBase) {

        //Creates a container to store VMs. This list is passed to the broker later
        LinkedList<CondorVM> list = new LinkedList<CondorVM>();

        //VM Parameters
        long size = 10000; //image size (MB)
        int ram = 512; //vm memory (MB)
        int mips = 1000;
        long bw = 1000;
        int pesNumber = 1; //number of cpus
        String vmm = "Xen"; //VMM name

        //create VMs
        CondorVM[] vm = new CondorVM[vms];

        for (int i = 0; i < vms; i++) {
            double ratio = 1.0;
            vm[i] = new CondorVM(vmIdBase + i, userId, mips * ratio, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
            list.add(vm[i]);
        }

        return list;
    }

    ////////////////////////// STATIC METHODS ///////////////////////
    /**
     * Creates main() to run this example
     */
    public static void main(String[] args) {


        try {
            // First step: Initialize the CloudSim package. It should be called


            ArgumentParser option = new ArgumentParser(args);//init is done in option

            FailureMonitor.init();//it doesn't really matter?
            FailureGenerator.init();//must do it so as to initialize FTC

            // before creating any entities.
            int num_user = 1;   // number of grid users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;  // mean trace events

            /**
             * Here we overwrites the vmNum set in config.txt.
             */
            /**
             * However, the exact number of vms may not necessarily be vmNum If
             * the data center or the host doesn't have sufficient resources the
             * exact vmNum would be smaller than that. Take care.
             */
            int vmNum = 20;//number of vms;
            Parameters.setVmNum(vmNum);

            // Initialize the CloudSim library
            CloudSim.init(num_user, calendar, trace_flag);

            DatacenterExtended datacenter0 = createDatacenter("Datacenter_0");
            DatacenterExtended datacenter1 = createDatacenter("Datacenter_1");

            /**
             * Create a WorkflowPlanner with one scheduler.
             */
            WorkflowPlanner wfPlanner = new WorkflowPlanner("planner_0", 1);
            /**
             * Create a WorkflowEngine. Attach it to the workflow planner
             */
            WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();
            /**
             * Create two list of VMs. The trick is that make sure all vmId is unique so we need to 
             * index vm from a base (in this case Parameters.getVmNum/2 for the second vmlist1). 
             */
            List<CondorVM> vmlist0 = createVM(wfEngine.getSchedulerId(0), Parameters.getVmNum() / 2 , 0);
            List<CondorVM> vmlist1 = createVM(wfEngine.getSchedulerId(0), Parameters.getVmNum() / 2 , Parameters.getVmNum() / 2);

            /**
             * Submits these lists of vms to this WorkflowEngine.
             */
            wfEngine.submitVmList(vmlist0, 0);
            wfEngine.submitVmList(vmlist1, 0);

            /**
             * Binds the data centers with the scheduler id.
             * This scheduler controls two data centers. Make sure your data center is not very big otherwise
             * all the vms will be allocated to the first available data center
             * In the future, the vm allocation algorithm should be improved. 
             */
            wfEngine.bindSchedulerDatacenter(datacenter0.getId(), 0);
            wfEngine.bindSchedulerDatacenter(datacenter1.getId(), 0);

            CloudSim.startSimulation();


            List<Job> outputList0 = wfEngine.getJobsReceivedList();

            CloudSim.stopSimulation();

            printJobList(outputList0);

        } catch (Exception e) {
            Log.printLine("The simulation has been terminated due to an unexpected error");
        }
    }

    private static DatacenterExtended createDatacenter(String name) {

        // Here are the steps needed to create a PowerDatacenter:
        // 1. We need to create a list to store one or more
        //    Machines
        List<Host> hostList = new ArrayList<Host>();

        // 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
        //    create a list to store these PEs before creating
        //    a Machine.
        //
        // Here is the trick to use multiple data centers in one broker. Broker will first
        // allocate all vms to the first datacenter and if there is no enough resource then it will allocate 
        // the failed vms to the next available datacenter. The trick is make sure your datacenter is not 
        // very big so that the broker will distribute them. 
        // In a future work, vm scheduling algorithms should be done
        
        //
        for (int i = 1; i <= 3; i++) {
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
         * The bandwidth between data centers.
         */
        double interBandwidth = 1.5e7;// the number comes from the futuregrid site, you can specify your bw
        interBandwidth = Parameters.getOverheadParams().getBandwidth();
        /**
         * The bandwidth within a data center.
         */
        double intraBandwidth = interBandwidth;
        try {
            ClusterStorage s1 = new ClusterStorage(name, 1e12);
            if (name.equals("Datacenter_0")) {
                /**
                 * The bandwidth from Datacenter_0 to Datacenter_1.
                 */
                s1.setBandwidth("Datacenter_1", interBandwidth);

            } else if (name.equals("Datacenter_1")) {
                /**
                 * The bandwidth from Datacenter_1 to Datacenter_0.
                 */
                s1.setBandwidth("Datacenter_0", interBandwidth);

            }
            // The bandwidth within a data center
            s1.setBandwidth("local", intraBandwidth);
            // The bandwidth to the source site 
            s1.setBandwidth("source", interBandwidth);
            storageList.add(s1);
            datacenter = new DatacenterExtended(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
        }

        return datacenter;
    }

    /**
     * Prints the job objects
     *
     * @param list list of jobs
     */
    private static void printJobList(List<Job> list) {
        int size = list.size();
        Job job;

        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Cloudlet ID" + indent + "STATUS" + indent
                + "Data center ID" + indent + "VM ID" + indent + indent + "Time" + indent + "Start Time" + indent + "Finish Time" + indent + "Depth");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (int i = 0; i < size; i++) {
            job = list.get(i);
            Log.print(indent + job.getCloudletId() + indent + indent);

            if (job.getCloudletStatus() == Cloudlet.SUCCESS) {
                Log.print("SUCCESS");

                Log.printLine(indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                        + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
            } else if (job.getCloudletStatus() == Cloudlet.FAILED) {
                Log.print("FAILED");

                Log.printLine(indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                        + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
            }
        }

    }
}
