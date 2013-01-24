


package org.workflowsim.examples;

import org.workflowsim.ClusterStorage;
import org.workflowsim.WorkflowEngine;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.workflowsim.CondorVM;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.workflowsim.Job;
import org.workflowsim.WorkflowPlanner;
import org.workflowsim.DatacenterExtended;
import org.workflowsim.failure.FailureGenerator;
import org.workflowsim.failure.FailureMonitor;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ArgumentParser;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

/**
 *a workflow engines, one planner two schedulers
 */
public class WorkflowSimExample1 {


	private static List<CondorVM> createVM(int userId, int vms) {

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

		for(int i=0;i<vms;i++){
                    double ratio = (i%2 + 1);
                    //ratio /= 2;
                    if(i<2)
                    
                        ratio = 1;
                    
                    else 
                        ratio = 1;
                    
                    //Log.printLine(ratio);
                    vm[i] = new CondorVM(i, userId, mips * ratio, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
                    
			//for creating a VM with a space shared scheduling policy for cloudlets:
			//vm[i] = Vm(i, userId, mips, pesNumber, ram, bw, size, priority, vmm, new CloudletSchedulerSpaceShared());

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
                        
                    
                        ArgumentParser option  = new ArgumentParser(args);//init is done in option

                        FailureMonitor.init();//it doesn't really matter?
                        FailureGenerator.init();//must do it so as to initialize FTC
                        
			// before creating any entities.
			int num_user = 1;   // number of grid users
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false;  // mean trace events

			// Initialize the CloudSim library
			CloudSim.init(num_user, calendar, trace_flag);

			DatacenterExtended datacenter0 = createDatacenter("Datacenter_0");
                        DatacenterExtended datacenter1 = createDatacenter("Datacenter_1");
                        
                        
                        WorkflowPlanner planner = new WorkflowPlanner("planner_0", 2);
                        WorkflowEngine wfEngine = planner.getWorkflowEngine();
                        List<CondorVM> vmlist0 = createVM(wfEngine.getSchedulerId(0),Parameters.getVmNum()); 
                        //List<CondorVM> vmlist1 = createVM(wfEngine.getSchedulerId().get(1),14); 
                        
                        //wfEngine.submitVmList(vmlist0);
                        wfEngine.submitVmList(vmlist0, 0);
                        //wfEngine.submitVmList(vmlist1, 1);
                        
                        wfEngine.bindSchedulerDatacenter(datacenter0.getId(), 0);
                        wfEngine.bindSchedulerDatacenter(datacenter1.getId(), 1);
                        
                        CloudSim.startSimulation();


                        List<Job>outputList0 = wfEngine.getJobsReceivedList();
                        
			CloudSim.stopSimulation();

			printJobList(outputList0);
			datacenter0.printDebts();
                        
                        datacenter1.printDebts();



			
		}
		catch (Exception e)
		{
			Log.printLine("The simulation has been terminated due to an unexpected error");
		}
	}

	private static DatacenterExtended createDatacenter(String name){

		// Here are the steps needed to create a PowerDatacenter:
		// 1. We need to create a list to store one or more
		//    Machines
		List<Host> hostList = new ArrayList<Host>();

		// 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
		//    create a list to store these PEs before creating
		//    a Machine.
                for(int i=1;i<=28;i++)
                {
                    List<Pe> peList1 = new ArrayList<Pe>();
                    int mips = 2000;
                    // 3. Create PEs and add these into the list.
                    //for a quad-core machine, a list of 4 PEs is required:
                    peList1.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
                    peList1.add(new Pe(1, new PeProvisionerSimple(mips)));

                    int hostId=0;
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
    				new VmSchedulerTimeShared(peList1)
    			)
                            
                    ); // This is our first machine
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


		// 6. Finally, we need to create a PowerDatacenter object.
		
		try {
                        ClusterStorage s1 = new ClusterStorage(name, 1e12);
                        storageList.add(s1);
			datacenter = new DatacenterExtended(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
		} catch (Exception e) {
		}

		return datacenter;
	}


	/**
	 * Prints the Cloudlet objects
	 * @param list  list of Cloudlets
	 */
	private static void printJobList(List<Job> list) {
		int size = list.size();
		Job job;

		String indent = "    ";
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		Log.printLine("Cloudlet ID" + indent + "STATUS" + indent +
				"Data center ID" + indent + "VM ID" + indent + indent + "Time" + indent + "Start Time" + indent + "Finish Time" + indent + "Depth");

		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			job = list.get(i);
			Log.print(indent + job.getCloudletId() + indent + indent);

			if (job.getCloudletStatus() == Cloudlet.SUCCESS){
				Log.print("SUCCESS");

				Log.printLine( indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId() +
						indent + indent + indent + dft.format(job.getActualCPUTime()) +
						indent + indent + dft.format(job.getExecStartTime())+ indent + indent + indent + 
                                                dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
			}
                        else if (job.getCloudletStatus() == Cloudlet.FAILED){
				Log.print("FAILED");

				Log.printLine( indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId() +
						indent + indent + indent + dft.format(job.getActualCPUTime()) +
						indent + indent + dft.format(job.getExecStartTime())+ indent + indent + indent + 
                                                dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
			}
		}

	}
}
