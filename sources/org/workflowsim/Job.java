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
import java.util.List;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;

/**
 *
 * @author Weiwei Chen
 */
public class Job extends Task{
    
    private List<Task> taskList;
    
    /*
     * The overhead between tasks (not yet utilized)
     */
    private double taskOverhead;
    //dependency list
    //private List<Job>parentList;
    //private List<Job>childList;
    
    public Job(
                    final int cloudletId,
                    final long cloudletLength/*,
                    final long cloudletFileSize,
                    final long cloudletOutputSize*/
                ) 
    {

        super(cloudletId, cloudletLength/*, cloudletFileSize, cloudletOutputSize*/);
        //super(cloudletId, cloudletLength, 1, cloudletFileSize, cloudletOutputSize, new UtilizationModelFull(), new UtilizationModelFull(), new UtilizationModelFull());
        this.taskList       = new ArrayList<Task>();
        this.taskOverhead   = 0.0;
     //   this.childList      = new ArrayList<Job>();
     //   this.parentList     = new ArrayList<Job>();
    }

    public List<Task> getTaskList()
    {
        //System.out.println("It is not safe to do it, please update other parameters");
        return this.taskList;
    }
    //Operators for childlist and parentlist
    public void setTaskList(List list)
    {
        this.taskList = list;
    }
    public void addTaskList(List list){
        this.taskList.addAll(list);
    }
    
    public List getParentList(){
        
        return super.getParentList();
    }
    

        
}
