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
package org.workflowsim.scheduling;

import java.util.Iterator;
import java.util.List;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.CondorVM;
import org.workflowsim.Job;
import org.workflowsim.WorkflowSimTags;
import org.workflowsim.utils.Parameters.FileType;
import org.workflowsim.utils.ReplicaCatalog;

/**
 * Data aware algorithm. Schedule a job to a vm that has most input data it requires. 
 * It only works for a local environment. 
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class DataAwareSchedulingAlgorithm extends BaseSchedulingAlgorithm {

    public DataAwareSchedulingAlgorithm() {
        super();
    }

    @Override
    public void run() {

        
        int size = getCloudletList().size();

        for (int i = 0; i < size; i++) {

            Cloudlet cloudlet = (Cloudlet) getCloudletList().get(i);

            int vmSize = getVmList().size();
            CondorVM closestVm = null;//(CondorVM)getVmList().get(0);
            double minTime = Double.MAX_VALUE;
            for (int j = 0; j < vmSize; j++) {
                CondorVM vm = (CondorVM) getVmList().get(j);
                if (vm.getState() == WorkflowSimTags.VM_STATUS_IDLE) {
                   
                    
                    Job job = (Job)cloudlet;
                    double time = dataTransferTime(job.getFileList(), cloudlet, vm.getId());
                    if(time < minTime){
                        minTime = time;
                        closestVm = vm;
                    }
                    
                }
            }

            if(closestVm!=null){
                closestVm.setState(WorkflowSimTags.VM_STATUS_BUSY);
                cloudlet.setVmId(closestVm.getId());
                getScheduledList().add(cloudlet);
            
            
            
            Log.printLine("Schedules " + cloudlet.getCloudletId() + " with "
                    + cloudlet.getCloudletLength() + " to VM " + closestVm.getId() 
                    +" with " + closestVm.getCurrentRequestedTotalMips() + " and data is " + minTime);
            }



        }

    }
    
       /**
     * If a input file has an output file it does not need stage-in For
     * workflows, we have a rule that a file is written once and read many
     * times, thus if a file is an output file it means it is generated within
     * this job and then used by another task within the same job (or other jobs
     * maybe) This is useful when we perform horizontal clustering
     *
     * @param list, the list of all files
     * @param file, the file to be examined
     * @pre $none
     * @post $none
     */
    private boolean isRealInputFile(List<File> list, File file) {
        if (file.getType() == FileType.INPUT.value)//input file
        {
            for (File another : list) {
                if (another.getName().equals(file.getName())
                        /**
                         * if another file is output file
                         */
                        && another.getType() == FileType.OUTPUT.value) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    /*
     * Stage in for a single job (both stage-in job and compute job)
     * @param requiredFiles, all files to be stage-in
     * @param cl, the job to be processed
     * @pre  $none
     * @post $none
     */

    protected double dataTransferTime(List<File> requiredFiles, Cloudlet cl, int vmId)  {
        double time = 0.0;

        Iterator<File> iter = requiredFiles.iterator();
        while (iter.hasNext()) {

            File file = iter.next();

            //The input file is not an output File 
            if (isRealInputFile(requiredFiles, file)) {
                List siteList = ReplicaCatalog.getStorageList(file.getName());
                if (siteList.isEmpty()) {
                    
                }

                boolean hasFile = false;
                for (Iterator it = siteList.iterator(); it.hasNext();) {
                    //site is where one replica of this data is located at
                    String site = (String) it.next();

                    if(site.equals(Integer.toString(vmId))){
                        hasFile = true;
                        break;
                    }

                }
                if(!hasFile){
                    time += file.getSize() ;
                }

            }
        }

        return time;
    }
}
