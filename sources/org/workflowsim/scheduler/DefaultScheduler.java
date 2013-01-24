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
package org.workflowsim.scheduler;

import org.workflowsim.CondorVM;
import org.workflowsim.WorkflowSimTags;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Vm;

/**
 * The default scheduler is a FCFS algorithm
 * @author Weiwei Chen
 */
public class DefaultScheduler implements SchedulerInterface{
    
    private List <? extends Cloudlet> cloudletList;
    
    private List <? extends Vm> vmList;
    
    private List < Cloudlet> scheduledList;
    
    public DefaultScheduler(){
        this.scheduledList = new ArrayList();
    }
    
    @Override
    public void setCloudletList(List list){
        this.cloudletList = list;
    }
    
    @Override
    public void setVmList(List list){
        this.vmList =new ArrayList(list);
    }
    
    @Override
    public List getCloudletList(){
        return this.cloudletList;
    }
    
    @Override
    public List getVmList(){
        return this.vmList;
    }   
    
    @Override
    public void run(){
        //Round Robin
        
        
        for(Iterator it = getCloudletList().iterator();it.hasNext();)
        {
            Cloudlet cloudlet = (Cloudlet)it.next();
            boolean stillHasVm = false;
            for(Iterator itc = getVmList().iterator(); itc.hasNext();)
            {

                CondorVM vm = (CondorVM) itc.next();
                if(vm.getState()==WorkflowSimTags.VM_STATUS_IDLE)
                {
                    stillHasVm = true;
                    vm.setState(WorkflowSimTags.VM_STATUS_BUSY);
                    cloudlet.setVmId(vm.getId());
                    this.scheduledList.add(cloudlet);
                    break;
                }
            }
            //no vm available 
            if(!stillHasVm){
                break;
            }

        }
    }
    @Override
    public List getScheduledList()
    {
        return this.scheduledList;
    }
}
