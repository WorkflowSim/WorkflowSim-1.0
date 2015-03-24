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
package org.workflowsim;

import java.util.ArrayList;
import java.util.List;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.workflowsim.clustering.BasicClustering;
import org.workflowsim.clustering.BlockClustering;
import org.workflowsim.clustering.HorizontalClustering;
import org.workflowsim.clustering.VerticalClustering;
import org.workflowsim.clustering.balancing.BalancedClustering;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.Parameters.ClassType;
import org.workflowsim.utils.ReplicaCatalog;

/**
 * ClusteringEngine is an optional component of WorkflowSim and it merges tasks
 * into jobs
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 *
 */
public final class ClusteringEngine extends SimEntity {

    /**
     * The task list
     */
    protected List< Task> taskList;
    /**
     * The job list
     */
    protected List<Job> jobList;
    /**
     * The task submitted list.
     */
    protected List<? extends Task> taskSubmittedList;
    /**
     * The task received list.
     */
    protected List<? extends Task> taskReceivedList;
    /**
     * The number of tasks submitted.
     */
    protected int cloudletsSubmitted;
    /**
     * The clustering engine to use
     */
    protected BasicClustering engine;
    /**
     * The WorkflowEngineId of the WorkflowEngine
     */
    private final int workflowEngineId;
    /**
     * The WorkflowEngine used in this ClusteringEngine
     */
    private final WorkflowEngine workflowEngine;

    /**
     * Created a new ClusteringEngine object.
     *
     * @param name name to be associated with this entity (as required by
     * Sim_entity class from simjava package)
     * @param schedulers the number of schedulers in this ClusteringEngine
     * @throws Exception the exception
     * @pre name != null
     * @post $none
     */
    public ClusteringEngine(String name, int schedulers) throws Exception {
        super(name);
        setJobList(new ArrayList<>());
        setTaskList(new ArrayList<>());
        setTaskSubmittedList(new ArrayList<>());
        setTaskReceivedList(new ArrayList<>());

        cloudletsSubmitted = 0;
        this.workflowEngine = new WorkflowEngine(name + "_Engine_0", schedulers);
        this.workflowEngineId = this.workflowEngine.getId();
    }

    /**
     * returns the WorkflowEngineId
     *
     * @return workflow engine id
     */
    public int getWorkflowEngineId() {
        return this.workflowEngineId;
    }

    /**
     * returns the WorkflowEngine
     *
     * @return workflow engine
     */
    public WorkflowEngine getWorkflowEngine() {
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

    /**
     * Processes events available for this ClusteringEngine.
     *
     * @pre ev != null
     * @post $none
     */
    protected void processClustering() {

        /**
         * The parameters from configuration file
         */
        ClusteringParameters params = Parameters.getClusteringParameters();
        switch (params.getClusteringMethod()) {
            /**
             * Perform Horizontal Clustering
             */
            case HORIZONTAL:
                // if clusters.num is set in configuration file
                if (params.getClustersNum() != 0) {
                    this.engine = new HorizontalClustering(params.getClustersNum(), 0);
                } // if clusters.size is set in configuration file
                else if (params.getClustersSize() != 0) {
                    this.engine = new HorizontalClustering(0, params.getClustersSize());
                }
                break;
            /**
             * Perform Vertical Clustering
             */
            case VERTICAL:
                int depth = 1;
                this.engine = new VerticalClustering(depth);
                break;
            /**
             * Perform Block Clustering
             */
            case BLOCK:
                this.engine = new BlockClustering(params.getClustersNum(), params.getClustersSize());
                break;
            /**
             * Perform Balanced Clustering
             */
            case BALANCED:
                this.engine = new BalancedClustering(params.getClustersNum());
                break;
            /**
             * By default, it does no clustering
             */
            default:
                this.engine = new BasicClustering();
                break;
        }
        engine.setTaskList(getTaskList());
        engine.run();
        setJobList(engine.getJobList());
    }

    /**
     * Adds data stage-in jobs to the job list
     */
    protected void processDatastaging() {

        /**
         * All the files of this workflow, it is saved in the workflow engine
         */
        List<FileItem> list = this.engine.getTaskFiles();
        /**
         * A bug of cloudsim, you cannot set the length of a cloudlet to be
         * smaller than 110 otherwise it will fail The reason why we set the id
         * of this job to be getJobList().size() is so that the job id is the
         * next available id
         */
        Job job = new Job(getJobList().size(), 110);

        /**
         * This is a very simple implementation of stage-in job, in which we Add
         * all the files to be the input of this stage-in job so that
         * WorkflowSim will transfers them when this job is executed
         */
        List<FileItem> fileList = new ArrayList<>();
        for (FileItem file : list) {
            /**
             * To avoid duplicate files
             */
            if (file.isRealInputFile(list)) {
                ReplicaCatalog.addFileToStorage(file.getName(), Parameters.SOURCE);
                fileList.add(file);
            }
        }
        job.setFileList(fileList);
        job.setClassType(ClassType.STAGE_IN.value);

        /**
         * stage-in is always first level job
         */
        job.setDepth(0);
        job.setPriority(0);

        /**
         * A very simple strategy if you have multiple schedulers and
         * sub-workflows just use the first scheduler
         */
        job.setUserId(getWorkflowEngine().getSchedulerId(0));

        /**
         * add stage-in job
         */
        for (Job cJob : getJobList()) {
            /**
             * first level jobs
             */
            if (cJob.getParentList().isEmpty()) {
                cJob.addParent(job);
                job.addChild(cJob);
            }
        }
        getJobList().add(job);
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
            case WorkflowSimTags.START_SIMULATION:
                break;
            case WorkflowSimTags.JOB_SUBMIT:
                List list = (List) ev.getData();
                setTaskList(list);
                /**
                 * It doesn't mean we must do clustering here because by default
                 * the processClustering() does nothing unless in the
                 * configuration file we have specified to use clustering
                 */
                processClustering();
                /**
                 * Add stage-in jobs Currently we just add a job that has
                 * minimum runtime but inputs all input data at the beginning of
                 * the workflow execution
                 */
                processDatastaging();
                sendNow(this.workflowEngineId, WorkflowSimTags.JOB_SUBMIT, getJobList());
                break;
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            default:
                processOtherEvent(ev);
                break;
        }
    }

    /**
     * Overrides this method when making a new and different type of Broker.
     * This method is called by {@link #body()} for incoming unknown tags.
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
     * Gets the task list.
     *
     * @return the task list
     */
    @SuppressWarnings("unchecked")
    public List<Task> getTaskList() {
        return (List<Task>) taskList;
    }

    /**
     * Gets the job list.
     *
     * @return the job list
     */
    public List<Job> getJobList() {
        return jobList;
    }

    /**
     * Sets the task list.
     *
     * @param taskList the new task list
     */
    protected void setTaskList(List<Task> taskList) {
        this.taskList = taskList;
    }

    /**
     * Sets the job list.
     *
     * @param jobList the new job list
     */
    protected void setJobList(List<Job> jobList) {
        this.jobList = jobList;
    }

    /**
     * Gets the tasks submitted list.
     *
     * @return the task submitted list
     */
    @SuppressWarnings("unchecked")
    public List<Task> getTaskSubmittedList() {
        return (List<Task>) taskSubmittedList;
    }

    /**
     * Sets the tasks submitted list.
     *
     * @param taskSubmittedList the new task submitted list
     */
    protected void setTaskSubmittedList(List<Task> taskSubmittedList) {
        this.taskSubmittedList = taskSubmittedList;
    }

    /**
     * Gets the task received list.
     *
     * @return the task received list
     */
    @SuppressWarnings("unchecked")
    public List<Task> getTaskReceivedList() {
        return (List<Task>) taskReceivedList;
    }

    /**
     * Sets the task received list.
     *
     * @param taskReceivedList the new cloudlet received list
     */
    protected void setTaskReceivedList(List<Task> taskReceivedList) {
        this.taskReceivedList = taskReceivedList;
    }
}
