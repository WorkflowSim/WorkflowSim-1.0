/**
 *  Copyright 2012-2013 University Of Southern California
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
import java.util.List;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;

/**
 * Task is an extention to Cloudlet in CloudSim. It supports the implementation
 * of dependencies between tasks, which includes a list of parent tasks and a 
 * list of child tasks that it has. In WorkflowSim, the Workflow Engine assure
 * that a task is released to the scheduler (ready to run) when all of its 
 * parent tasks have completed successfully
 * 
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class Task extends Cloudlet{
    
    /*
     * The list of parent tasks. 
     */
    private List<Task>parentList;
    /*
     * The list of child tasks. 
     */
    private List<Task>childList;
    /*
     * The list of all files (input data and ouput data)
     */
    private List<org.cloudbus.cloudsim.File>fileList;
    /*
     * The priority used for research. Not used in current version. 
     */
    private int priority;
    /*
     * The depth of this task. Depth of a task is defined as the furthest path 
     * from the root task to this task. It is set during the workflow parsing 
     * stage. 
     */
    private int depth;
    /*
     * The impact of a task. It is used in research. 
     */
    private double impact;

    /*
     * The type of a task. 
     */
    private String type;
    
    /**
     * Allocates a new Task object. The task length should be greater than or equal to 1. 
     * 
     * @param taskId the unique ID of this Task
     * @param taskLength the length or size (in MI) of this task to be executed in a
     *            PowerDatacenter
     * @pre taskId >= 0
     * @pre taskLength >= 0.0
     * @post $none
     */
    
    public Task(
                    final int taskId,
                    final long taskLength
                ) 
    {
        /**
         * We do not use cloudletFileSize and cloudletOutputSize here. We have added a list to task 
         * and thus we don't need a cloudletFileSize or cloudletOutputSize here
         * The utilizationModelCpu, utilizationModelRam, and utilizationModelBw are just set to be 
         * the default mode. You can change it for your own purpose. 
         */
        super(taskId, taskLength, 1, 0, 0,  new UtilizationModelFull(), new UtilizationModelFull(), new UtilizationModelFull());


        this.childList      = new ArrayList<Task>();
        this.parentList     = new ArrayList<Task>();
        this.fileList       = new ArrayList<org.cloudbus.cloudsim.File>();
        this.impact         = 0.0;
    }

    /**
     * Sets the type of the task
     * 
     * @param type the type
     * @return $none
     */
    public void setType(String type){
        this.type = type;
    }
    
    /**
    * Gets the type of the task
    * 
    * @return the type of the task
    * @pre $none
    * @post $none
    */
    public String getType(){
        return type;
    }
    
    /**
     * Sets the priority of the task
     * 
     * @param priority the priority
     * @return $none
     */
    public void setPriority(int priority)
    {
        this.priority = priority;
    }
    
    /**
     * Sets the depth of the task
     * 
     * @param depth the depth
     * @return $none
     */
    public void setDepth(int depth){
        this.depth = depth;
    }
    
    /**
    * Gets the priority of the task
    * 
    * @return the priority of the task
    * @pre $none
    * @post $none
    */
    public int getPriority(){
        
        return this.priority;
    }
    
    /**
    * Gets the depth of the task
    * 
    * @return the depth of the task
    * @pre $none
    * @post $none
    */
    public int getDepth(){
        return this.depth;
    }
    
    /**
    * Gets the child list of the task
    * 
    * @return the list of the children
    * @pre $none
    * @post $none
    */
    public List<Task> getChildList()
    {
        return this.childList;
    }
    
    /**
     * Sets the child list of the task
     * 
     * @param list, child list of the task
     * @return $none
     */
    public void setChildList(List list){
        this.childList = list;
    }
    
    /**
     * Sets the parent list of the task
     * 
     * @param list, parent list of the task
     * @return $none
     */
    public void setParentList(List list){
        this.parentList = list;
    }
    
    /**
     * Adds the list to existing child list
     * 
     * @param list, the child list to be added
     * @return $none
     */
    public void addChildList(List list){
        this.childList.addAll(list);
    }
    
    /**
     * Adds the list to existing parent list
     * 
     * @param list, the parent list to be added
     * @return $none
     */
    public void addParentList(List list){
        this.parentList.addAll(list);
    }
    
    /**
    * Gets the list of the parent tasks
    * 
    * @return the list of the parents
    * @pre $none
    * @post $none
    */
    public List<Task> getParentList()
    {
        return this.parentList;
    }
    
    /**
     * Adds a task to existing child list
     * 
     * @param task, the child task to be added
     * @return $none
     */
    public void addChild(Task task){
        this.childList.add(task);
    }
    
    /**
     * Adds a task to existing parent list
     * 
     * @param task, the parent task to be added
     * @return $none
     */
    public void addParent(Task task){
        this.parentList.add(task);
    }
    
    /**
    * Gets the list of the files
    * 
    * @return the list of files
    * @pre $none
    * @post $none
    */
    public List getFileList(){
        return this.fileList;
    }
    
    /**
     * Adds a file to existing file list
     * 
     * @param file, the file to be added
     * @return $none
     */
    public void addFile(org.cloudbus.cloudsim.File file){
        this.fileList.add(file);
    }
    
    /**
     * Sets a file list
     * 
     * @param list, the file list
     * @return $none
     */
    public void setFileList(List<org.cloudbus.cloudsim.File> list)
    {
        this.fileList = list;
    }
    
    /**
     * Sets the impact factor
     * 
     * @param impact, the impact factor
     * @return $none
     */
    public void setImpact(double impact){
        this.impact = impact;
    }
    
    /**
    * Gets the impact of the task
    * 
    * @return the impact of the task
    * @pre $none
    * @post $none
    */
    public double getImpact(){
        return this.impact;
    }
}
