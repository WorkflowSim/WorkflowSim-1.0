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
package org.workflowsim.clusering;

import org.workflowsim.Task;
import java.util.ArrayList;
import java.util.Iterator;

/**
 *
 * @author Weiwei Chen
 */
public class TaskSet {
    private ArrayList<Task> taskList;
    
    private ArrayList<TaskSet> parentList;
    
    private ArrayList<TaskSet> childList;
    
    public boolean hasChecked;
    
    private double impactFactor;
    
    public TaskSet(){
        this.taskList       = new ArrayList<Task>();
        this.parentList     = new ArrayList<TaskSet>();
        this.childList      = new ArrayList<TaskSet>();
        this.hasChecked     = false;
        this.impactFactor   = 0.0;
        
    
    }
    
    public double getImpactFactor(){
        return this.impactFactor;
    }
    public void setImpactFafctor(double factor){
        this.impactFactor = factor;
    }
    public ArrayList<TaskSet> getParentList(){
        return this.parentList;
    }
    public ArrayList<TaskSet> getChildList(){
        return this.childList;
    }
    
    public ArrayList<Task> getTaskList(){
        return this.taskList;
    }
    public void addTask(Task task){
        this.taskList.add(task);
    }
    public void addTask(ArrayList<Task> list){
        this.taskList.addAll(list);
    }
    
    public long getJobRuntime(){
        long runtime = 0;
        for(Task task: taskList){
            runtime += task.getCloudletLength();
        }
        return runtime;
    }
}
