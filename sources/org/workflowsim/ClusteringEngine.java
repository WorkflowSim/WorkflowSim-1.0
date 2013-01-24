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

import org.workflowsim.clusering.balancing.BalancedClustering;
import org.workflowsim.clusering.BlockClustering;
import org.workflowsim.clusering.BasicClustering;
import org.workflowsim.clusering.HorizontalClustering;
import org.workflowsim.clusering.VerticalClustering;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import java.util.Map;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;



/**
 * WorkflowPlanner supports dynamic planning in the future
 * 
 * @author Weiwei Chen
 *
 */
public class ClusteringEngine extends SimEntity {


	/** The cloudlet list. */
	protected List< Task> taskList;
        
        protected List<Job > jobList;

	/** The cloudlet submitted list. */
	protected List<? extends Task> taskSubmittedList;

	/** The cloudlet received list. */
	protected List<? extends Task> taskReceivedList;

	/** The cloudlets submitted. */
	protected int cloudletsSubmitted;

        protected  BasicClustering engine;
        

        
        private int workflowEngineId;
        // it could be a list
        private WorkflowEngine workflowEngine;
        
        //public static Map ReplicaCatalog;
        

        
       	/**
	 * Created a new DatacenterBroker object.
	 * 
	 * @param name name to be associated with this entity (as required by Sim_entity class from
	 *            simjava package)
	 * @throws Exception the exception
	 * @pre name != null
	 * @post $none
	 */

        
	public ClusteringEngine(String name, int schedulers /*, Map rc*/) throws Exception {
            super(name);
            setJobList(new ArrayList<Job>());
            setTaskList(new ArrayList<Task>());
            //???
            setTaskSubmittedList(new ArrayList<Task>());
            setTaskReceivedList(new ArrayList<Task>());

            //this.ReplicaCatalog = rc;
            cloudletsSubmitted = 0;
            this.workflowEngine = new WorkflowEngine(name+"_Engine_0", schedulers);
            this.workflowEngineId = this.workflowEngine.getId();
 
	}



        public int getWorkflowEngineId(){
            return this.workflowEngineId;
        }
        public WorkflowEngine getWorkflowEngine(){
            return this.workflowEngine;
        }


      
       
	/**
	 * This method is used to send to the broker the list of cloudlets.
	 * 
	 * @param list the list
	 * @pre list !=null
	 * @post $none
	 */
	public void submitTaskList(List<Task> list) {
		getTaskList().addAll(list);
	}


        protected void processClustering(){
            
            //DefaultClustering engine = new DefaultClustering();
            //this.engine = new DefaultClustering();
            //num, size
            ClusteringParameters params = Parameters.getClusteringParameters();
            
            
            switch(params.getClusteringMethod()){
                case HORIZONTAL:

                    if(params.getClustersNum()!=0){
                        this.engine = new HorizontalClustering( params.getClustersNum(), 0);
                    }
                    else if(params.getClustersSize()!=0){
                        this.engine = new HorizontalClustering( 0, params.getClustersSize());
                    }

                    break;
                case VERTICAL:
                    int depth = 1;
                    this.engine = new VerticalClustering( depth);
                    break;
                case BLOCK:
                    this.engine = new BlockClustering(params.getClustersNum(), params.getClustersSize());
                    break;
                case BALANCED:
                    this.engine = new BalancedClustering(params.getClustersNum());
                    break;
                default:
                    this.engine = new BasicClustering();
                    break;
                }
            engine.setTaskList(getTaskList());
            engine.run();
            setJobList(engine.getJobList());

            
            
        }
        private boolean isRealInputFile(List<org.cloudbus.cloudsim.File> list, org.cloudbus.cloudsim.File file){
            if(file.getType() == 1)//input file
            {

                for(org.cloudbus.cloudsim.File another: list){
                    
                    
                    if(another.getName().equals(file.getName()) 
                            && another.getType()==2){
                        return false;
                    }
                }
                return true;
            }
            return false;
        }
        protected void processDatastaging(){

            /*
             * here we assume that jobs are indexed in order because of clustering 
             * Even with default clustering is it still in order replaced
             * input file size and output file size don't matter
             */
            //this engine is clustering engine, not workflow engine
            List list = this.engine.getTaskFiles();
            //long size  = this.engine.getInputDataSize();
            //The duration of a job must be at least 100ms , a bug of cloudsim
            //minimum clock time, otherwise fail
//            Log.printLine("Data Stageing is not yet supported");
            Job job = new Job(getJobList().size() , 110/*, 0, 0*/);
            //job.setInputDataSize(size);
            //input real input files at the beginning
            
            
            List newList = new ArrayList<org.cloudbus.cloudsim.File>();
            for(Iterator it = list.iterator(); it.hasNext();){
                org.cloudbus.cloudsim.File file = (org.cloudbus.cloudsim.File)it.next();
                
                if(isRealInputFile(list, file)){
                    ReplicaCatalog.addStorageList(file.getName(), "source");
                    newList.add(file);
                }

                
                
            }
            job.setFileList(newList);
            job.setClassType(1);
            //stage-in is always first level job 
            //may have bugs when you have multiple sub-workflows
            job.setDepth(0);
            job.setPriority(0);
            //bug here
            //the first startegy sends all into the first one
            job.setUserId(getWorkflowEngine().getSchedulerId(0));
            
            
                        //add stage-in job
            for(Iterator it = getJobList().iterator(); it.hasNext();)
            {
                Job cJob = (Job)it.next();
                if(cJob.getParentList().isEmpty()){
                    //first level job
                    cJob.addParent(job);
                    job.addChild(cJob);
                }
                
            }
            //don't do it before the for loop otherwise there is a loop
            getJobList().add(job);
            //add stage-out job but I don't know how to do it now
        }
	/**
	 * Processes events available for this Broker.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != null
	 * @post $none
	 */
	@Override
	public void processEvent(SimEvent ev) {
            
		switch (ev.getTag()) {
		// Resource characteristics request
			// if the simulation finishes
                    case WorkflowSimTags.START_SIMULATION:
                        break;
                        case WorkflowSimTags.JOB_SUBMIT:
                                List list = (List)ev.getData();
                                setTaskList(list);

                                processClustering();

                                processDatastaging();

                                sendNow(this.workflowEngineId, WorkflowSimTags.JOB_SUBMIT, getJobList());
                                break;
			case CloudSimTags.END_OF_SIMULATION:
				shutdownEntity();
				break;
			// other unknown tags are processed by this method
			default:
				processOtherEvent(ev);
				break;
		}
	}

	/**
	 * Overrides this method when making a new and different type of Broker. This method is called
	 * by {@link #body()} for incoming unknown tags.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != null
	 * @post $none
	 */
	protected void processOtherEvent(SimEvent ev) {
		if (ev == null) {
			Log.printLine(getName() + ".processOtherEvent(): " + "Error - an event is null.");
			return;
		}

		Log.printLine(getName() + ".processOtherEvent(): "
				+ "Error - event unknown by this DatacenterBroker.");
	}

	/**
	 * Send an internal event communicating the end of the simulation.
	 * 
	 * @pre $none
	 * @post $none
	 */
	protected void finishExecution() {
		//sendNow(getId(), CloudSimTags.END_OF_SIMULATION);
	}

	/*
	 * (non-Javadoc)
	 * @see cloudsim.core.SimEntity#shutdownEntity()
	 */
	@Override
	public void shutdownEntity() {
		Log.printLine(getName() + " is shutting down...");
	}

	/*
	 * (non-Javadoc)
	 * @see cloudsim.core.SimEntity#startEntity()
	 */
	@Override
	public void startEntity() {
		Log.printLine(getName() + " is starting...");
		schedule(getId(), 0, WorkflowSimTags.START_SIMULATION);
	}


	/**
	 * Gets the cloudlet list.
	 * 
	 * @param <T> the generic type
	 * @return the cloudlet list
	 */
	@SuppressWarnings("unchecked")
	public List<Task> getTaskList() {
		return (List<Task>) taskList;
	}
        public List<Job> getJobList(){
            return jobList;
        }

	/**
	 * Sets the cloudlet list.
	 * 
	 * @param <T> the generic type
	 * @param cloudletList the new cloudlet list
	 */
	protected void setTaskList(List<Task> taskList) {
		this.taskList = taskList;
	}

        protected void setJobList(List<Job> jobList){
            this.jobList = jobList;
        }
	/**
	 * Gets the cloudlet submitted list.
	 * 
	 * @param <T> the generic type
	 * @return the cloudlet submitted list
	 */
	@SuppressWarnings("unchecked")
	public List<Task> getTaskSubmittedList() {
		return (List<Task>) taskSubmittedList;
	}

	/**
	 * Sets the cloudlet submitted list.
	 * 
	 * @param <T> the generic type
	 * @param cloudletSubmittedList the new cloudlet submitted list
	 */
	protected void setTaskSubmittedList(List<Task> taskSubmittedList) {
		this.taskSubmittedList = taskSubmittedList;
	}

	/**
	 * Gets the cloudlet received list.
	 * 
	 * @param <T> the generic type
	 * @return the cloudlet received list
	 */
	@SuppressWarnings("unchecked")
	public List<Task> getTaskReceivedList() {
		return (List<Task>) taskReceivedList;
	}

	/**
	 * Sets the cloudlet received list.
	 * 
	 * @param <T> the generic type
	 * @param cloudletReceivedList the new cloudlet received list
	 */
	protected void setTaskReceivedList(List<Task> taskReceivedList) {
		this.taskReceivedList = taskReceivedList;
	}


}
