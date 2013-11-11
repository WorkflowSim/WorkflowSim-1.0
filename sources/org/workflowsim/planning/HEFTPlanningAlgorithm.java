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
package org.workflowsim.planning;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.Vm;
import org.workflowsim.CondorVM;
import org.workflowsim.Task;
import org.workflowsim.utils.Parameters;

/**
 * The HEFT planning algorithm. This algo does not consider data transfer.
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Nov 10, 2013
 */
public class HEFTPlanningAlgorithm extends BasePlanningAlgorithm {

    /**
     * The main function
     */
    @Override
    public void run() {

        double [][] bandwidths = Parameters.getBandwidths();
        int vmNum = getVmList().size();
        int taskNum = getTaskList().size();
        double [] availableTime = new double[vmNum];
        //cloudlet id starts from 1
        double [][] earliestStartTime = new double[taskNum + 1][vmNum];
        double [][] earliestFinishTime = new double[taskNum + 1][vmNum];
        int [] allocation = new int[taskNum + 1];
        
        List<Task> taskList = new ArrayList<Task>(getTaskList());
        List<Task> readyList = new ArrayList<Task>();
        while(!taskList.isEmpty()){
            readyList.clear();
            for(Task task : taskList){
                boolean ready = true;
                for(Task parent: task.getParentList()){
                    if(taskList.contains(parent)){
                        ready = false;
                        break;
                    }
                }
                if(ready){
                    readyList.add(task);
                }
            }
            taskList.removeAll(readyList);
            //schedule readylist
            for(Task task: readyList){
                long [] fileSizes = new long[task.getParentList().size()];
                int parentIndex = 0;
                for(Task parent: task.getParentList()){
                    long fileSize = 0;
                    for(Iterator fileIter = task.getFileList().iterator(); fileIter.hasNext();){
                        File file = (File)fileIter.next();
                        if(file.getType()==1){
                            for(Iterator fileIter2 = parent.getFileList().iterator();fileIter2.hasNext();){
                                File file2 = (File)fileIter2.next();
                                if(file2.getType() == 2 && file2.getName().equals(file.getName()))
                                {
                                    fileSize += file.getSize();
                                }
                            }
                        }
                    }
                    fileSizes[parentIndex] = fileSize;
                    parentIndex ++;
                }     
                
                double minTime = Double.MAX_VALUE;
                int minTimeIndex = 0;
                
                for(int vmIndex = 0; vmIndex < getVmList().size(); vmIndex++){
                    Vm vm = (Vm)getVmList().get(vmIndex);
                    double startTime = availableTime[vm.getId()];
                    parentIndex = 0;
                    for(Task parent: task.getParentList()){
                        int allocatedVmId = allocation[parent.getCloudletId()];
                        double actualFinishTime = earliestFinishTime[parent.getCloudletId()][allocatedVmId];
                        double communicationTime = fileSizes[parentIndex] / bandwidths[allocatedVmId][vm.getId()];
                        
                        if(actualFinishTime + communicationTime > startTime){
                            startTime = actualFinishTime + communicationTime;
                        }
                        parentIndex ++;
                    }
                    earliestStartTime[task.getCloudletId()][vm.getId()] = startTime;
                    double runtime = task.getCloudletLength() / vm.getMips();
                    earliestFinishTime[task.getCloudletId()][vm.getId()] = runtime + startTime;
                    
                    if(runtime + startTime < minTime){
                        minTime = runtime + startTime;
                        minTimeIndex = vmIndex;
                    }
                }
                
                allocation[task.getCloudletId()] = minTimeIndex;//we do not really need it use task.getVmId
                task.setVmId(minTimeIndex);
                availableTime[minTimeIndex] = minTime;
            }
        }
        
    }


}
