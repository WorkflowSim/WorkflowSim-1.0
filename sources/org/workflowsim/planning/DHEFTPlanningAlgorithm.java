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
import java.util.List;
import org.cloudbus.cloudsim.Vm;
import org.workflowsim.FileItem;
import org.workflowsim.Task;
import org.workflowsim.utils.Parameters;

/**
 * The Distributed HEFT planning algorithm. The difference compared to HEFT:
 * 
 * 1. We are able to specify the bandwidth between each pair of vms in the
 * bandwidths of Parameters. 
 * 2. Instead of using the average communication cost in HEFT, we also aim to 
 * optimize the communication cost
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Nov 10, 2013
 */
public class DHEFTPlanningAlgorithm extends BasePlanningAlgorithm {

    /**
     * The main function
     */
    @Override
    public void run() {

        List<Vm> vmList = getVmList();
        double [][] bandwidths = new double[vmList.size()][vmList.size()];
        
        for(int i = 0; i < vmList.size(); i++){
            for(int j = i ; j < vmList.size(); j++){
                bandwidths[i][j] = bandwidths [j][i] = Math.min(vmList.get(i).getBw(), vmList.get(j).getBw());
            }
        }
        for (Object vmObject : getVmList()) {
            Vm vm =  (Vm)vmObject;
            vm.getBw();
        }
        
        int vmNum = getVmList().size();
        int taskNum = getTaskList().size();
        double [] availableTime = new double[vmNum];
        //cloudlet id starts from 1
        double [][] earliestStartTime = new double[taskNum + 1][vmNum];
        double [][] earliestFinishTime = new double[taskNum + 1][vmNum];
        int [] allocation = new int[taskNum + 1];
        
        List<Task> taskList = new ArrayList(getTaskList());
        List<Task> readyList = new ArrayList<>();
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
                    for(FileItem file : task.getFileList()){
                        if(file.getType()==Parameters.FileType.INPUT){
                            for(FileItem file2 : parent.getFileList()){
                                if(file2.getType() == Parameters.FileType.OUTPUT && file2.getName().equals(file.getName()))
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
