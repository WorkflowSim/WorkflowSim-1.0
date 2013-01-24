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

import java.util.ArrayList;
import java.util.HashMap;
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
public class WorkflowPlanner extends SimEntity {


	/** The cloudlet list. */
	protected List< Task> taskList;
        
        
        protected WorkflowParser parser;
        
        
        private int clusteringEngineId;
        
        private ClusteringEngine clusteringEngine;
        
        //private int workflowEngineId;
        // it could be a list
        //private WorkflowEngine workflowEngine;
        
        //public static Map ReplicaCatalog;
        
        //public static Map FileName2File;
        
        
       	/**
	 * Created a new DatacenterBroker object.
	 * 
	 * @param name name to be associated with this entity (as required by Sim_entity class from
	 *            simjava package)
	 * @throws Exception the exception
	 * @pre name != null
	 * @post $none
	 */
        public WorkflowPlanner(String name) throws Exception{
            this(name, 1);
        }
        
	public WorkflowPlanner(String name, int schedulers) throws Exception {
            super(name);

            setTaskList(new ArrayList<Task>());


            //this.ReplicaCatalog     = new HashMap<String, List>();
            //this.FileName2File      = new HashMap<String, org.cloudbus.cloudsim.File>();

            this.clusteringEngine = new ClusteringEngine(name +  "_Merger_", schedulers);
            this.clusteringEngineId = this.clusteringEngine.getId();

            this.parser             = new WorkflowParser(getClusteringEngine().getWorkflowEngine().getSchedulerId(0));
            
            

                        

	}


        public int getClusteringEngineId(){
            return this.clusteringEngineId;
        }
        public ClusteringEngine getClusteringEngine(){
            return this.clusteringEngine;
        }
        public WorkflowParser getWorkflowParser(){
            return this.parser;
        }

      
        public int getWorkflowEngineId(){
            return getClusteringEngine().getWorkflowEngineId();
        }
        public WorkflowEngine getWorkflowEngine(){
            return getClusteringEngine().getWorkflowEngine();
        }
       
	/**
	 * This method is used to send to the broker the list of cloudlets.
	 * 
	 * @param list the list
	 * @pre list !=null
	 * @post $none
	 */
//	public void submitTaskList(List<Task> list) {
//		getTaskList().addAll(list);
//	}

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
                                getWorkflowParser().parse();
                                setTaskList(getWorkflowParser().getTaskList());
                                processImpactFactors(getTaskList());
                                sendNow(getClusteringEngineId(), WorkflowSimTags.JOB_SUBMIT, getTaskList());
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
        
        private void processImpactFactors(List<Task> taskList){
            ArrayList<Task> exits = new ArrayList();
            for(Task task: taskList){
                if(task.getChildList().isEmpty()){
                    exits.add(task);
                }
            }
            double avg = 1.0 / exits.size();
            for(Task task: exits){
                //set.setImpactFafctor(avg);
                addImpact(task, avg);
            }
        }
        private void addImpact(Task task, double impact){

            task.setImpact(task.getImpact() + impact);
            int size = task.getParentList().size();
            if(size > 0){
                double avg = impact / size;
                for(Task parent: task.getParentList()){
                    addImpact(parent, avg);
                }
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
       
	/**
	 * Sets the cloudlet list.
	 * 
	 * @param <T> the generic type
	 * @param cloudletList the new cloudlet list
	 */
	protected void setTaskList(List<Task> taskList) {
		this.taskList = taskList;
	}



}
