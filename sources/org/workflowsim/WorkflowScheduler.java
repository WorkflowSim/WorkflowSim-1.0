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
package org.workflowsim;


import org.workflowsim.failure.FailureGenerator;
import org.workflowsim.scheduler.DefaultScheduler;
import org.workflowsim.scheduler.HEFTScheduler;
import org.workflowsim.scheduler.MCTScheduler;
import org.workflowsim.scheduler.MaxMinScheduler;
import org.workflowsim.scheduler.MinMinScheduler;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.Parameters.SCHMethod;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

/**
 * WorkflowScheduler represents a scheduler acting on behalf of a user. It hides VM management, as vm
 * creation, sumbission of jobs to this VMs and destruction of VMs.
 * 
 * @author Weiwei Chen
 */
public class WorkflowScheduler extends DatacenterBroker {

	//private List<Integer> datacenterIdsList;
        //private int datacenterId;
        
	/**
	 * Created a new WorkflowScheduler object.
	 * 
	 * @param name name to be associated with this entity (as required by Sim_entity class from
	 *            simjava package)
	 * @throws Exception the exception
	 * @pre name != null
	 * @post $none
	 */
        private int workflowEngineId;
	public WorkflowScheduler(String name) throws Exception {
		super(name);
                

	}
        
        public void bindSchedulerDatacenter(int datacenterId)
        {
            if(datacenterId <=0) 
            {
                Log.printLine("Error in data center id");
                return ;
            }
            this.datacenterIdsList.add( datacenterId);
        }

        public void setWorkflowEngineId(int workflowEngineId)
        {
            this.workflowEngineId = workflowEngineId;
        }
	
        @Override
	public void processEvent(SimEvent ev) {
		switch (ev.getTag()) {
		// Resource characteristics request
			case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
				processResourceCharacteristicsRequest(ev);
				break;
			// Resource characteristics answer
			case CloudSimTags.RESOURCE_CHARACTERISTICS:
				processResourceCharacteristics(ev);
				break;
			// VM Creation answer
			case CloudSimTags.VM_CREATE_ACK:
				processVmCreate(ev);
				break;
			// A finished cloudlet returned
                        case WorkflowSimTags.CLOUDLET_CHECK:
                            processCloudletReturn(ev);
                            break;
			case CloudSimTags.CLOUDLET_RETURN:
				//processCloudletCheck(ev);
                            
                            processCloudletReturn(ev);
				break;
			// if the simulation finishes
			case CloudSimTags.END_OF_SIMULATION:
				shutdownEntity();
				break;
                        case CloudSimTags.CLOUDLET_SUBMIT:
                                processCloudletSubmit(ev);
                                break;
                            
                        case WorkflowSimTags.CLOUDLET_UPDATE:
                            processCloudletUpdate(ev);
                            break;
			// other unknown tags are processed by this method
			default:
				processOtherEvent(ev);
				break;
		}
	}
        
        private DefaultScheduler getScheduler(SCHMethod name){
            DefaultScheduler  scheduler = null;
            //MAXMIN_SCH, MINMIN_SCH, ROUNDR_SCH, HEFT_SCH
            switch(name){
                case MINMIN_SCH:
                
                
                    scheduler = new MinMinScheduler();
                    break;
                case MAXMIN_SCH:
                
                    scheduler = new MaxMinScheduler();
                    break;
                case HEFT_SCH:
                
                    scheduler = new HEFTScheduler();
                    break;
                case MCT_SCH:
                
                    scheduler = new MCTScheduler();
                    break;
                case ROUNDR_SCH:
               
                    scheduler = new DefaultScheduler();
                    break;
                default: 
                    scheduler = new DefaultScheduler();
                    break;
                
                
            }
            
            return scheduler;
        }
       
        
        protected void processCloudletUpdate(SimEvent ev)
        {

            DefaultScheduler scheduler = getScheduler(Parameters.getSchedulerMode());
            scheduler.setCloudletList(getCloudletList());
            scheduler.setVmList(getVmsCreatedList());
            scheduler.run();
            List scheduledList = scheduler.getScheduledList();
            for(Iterator it = scheduledList.iterator(); it.hasNext();)
            {
                Cloudlet cloudlet = (Cloudlet)it.next();
                int vmId = cloudlet.getVmId();
                schedule(getVmsToDatacentersMap().get(vmId), Parameters.getOverheadParams().getQueueDelay(cloudlet), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
                
            }
            getCloudletList().removeAll(scheduledList);
            getCloudletSubmittedList().addAll(scheduledList);
            cloudletsSubmitted += scheduledList.size();
                        
        }
        
//        private void printJob(Cloudlet cl, String message){
//            Log.printLine("Scheduler " + message +" " + cl.getCloudletId());
//        }
        
        //submitted list is actually running list
	/**
	 * Process a cloudlet return event.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != $null
	 * @post $none
	 */
    @Override
	protected void processCloudletReturn(SimEvent ev) {
		Cloudlet cloudlet = (Cloudlet) ev.getData();
                //printJob(cloudlet, "returned");
                
                Job job = (Job)cloudlet;
                FailureGenerator.generate(job);
                
		getCloudletReceivedList().add(cloudlet);
                getCloudletSubmittedList().remove(cloudlet);
                //right
                CondorVM vm = (CondorVM)getVmsCreatedList().get(cloudlet.getVmId());
                //so that this resource is released
                vm.setState(WorkflowSimTags.VM_STATUS_IDLE);
                
                schedule(this.workflowEngineId, Parameters.getOverheadParams().getPostDelay(job), CloudSimTags.CLOUDLET_RETURN, cloudlet);
                
                cloudletsSubmitted--;
                //not really update right now, should wait 1 s until many jobs have returned
                
                schedule(this.getId(), 0.0, WorkflowSimTags.CLOUDLET_UPDATE);

	}

        protected void processCloudletCheck(SimEvent ev){

                
                
                

                
        }

    	@Override
	public void startEntity() {
		Log.printLine(getName() + " is starting...");
		// this resource should register to regional GIS.
		// However, if not specified, then register to system GIS (the
		// default CloudInformationService) entity.
		//int gisID = CloudSim.getEntityId(regionalCisName);
                int gisID = -1;
		if (gisID == -1) {
			gisID = CloudSim.getCloudInfoServiceEntityId();
		}

		// send the registration to GIS
		sendNow(gisID, CloudSimTags.REGISTER_RESOURCE, getId());
                //the below sentence is executed in workflow engine
                //schedule(getId(), 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST);

	}
        @Override
        public void shutdownEntity()
        {
            
            clearDatacenters();
            Log.printLine(getName() + " is shutting down...");
            
        }
	/**
	 * Submit cloudlets to the created VMs.
	 * Scheduling is here
	 * @pre $none
	 * @post $none
	 */
        //Used at some points
        //if all vms are created submitCloulets would be called
        @Override
	protected void submitCloudlets() {
            
            sendNow(this.workflowEngineId, CloudSimTags.CLOUDLET_SUBMIT, null);
	}
        //private int vmIndex = 0;
        private boolean processCloudletSubmitHasShown = false;
    	protected void processCloudletSubmit(SimEvent ev) {
		//Cloudlet cloudlet = (Cloudlet) ev.getData();
                List<Job> list = (List)ev.getData();
                getCloudletList().addAll(list);
                //should delay
                //processCloudletUpdate();
                sendNow(this.getId(), WorkflowSimTags.CLOUDLET_UPDATE);
                if(!processCloudletSubmitHasShown){
                    Log.printLine("Pay Attention that the actual vm size is " + getVmsCreatedList().size());
                    processCloudletSubmitHasShown = true;
                }
        }
    
    	/**
	 * Process a request for the characteristics of a PowerDatacenter.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != $null
	 * @post $none
	 */
    @Override
	protected void processResourceCharacteristicsRequest(SimEvent ev) {
                
		//setDatacenterIdsList(CloudSim.getCloudResourceList());
		setDatacenterCharacteristicsList(new HashMap<Integer, DatacenterCharacteristics>());

		Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloud Resource List received with "
				+ getDatacenterIdsList().size() + " resource(s)");

		for (Integer datacenterId : getDatacenterIdsList()) {
			sendNow(datacenterId, CloudSimTags.RESOURCE_CHARACTERISTICS, getId());
		}
	}

	
	
}
