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
import org.cloudbus.cloudsim.Cloudlet;
import org.workflowsim.CondorVM;
import org.workflowsim.WorkflowSimTags;

/**
 * The FCFS algorithm. 
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class FCFSSchedulingAlgorithm extends BaseSchedulingAlgorithm {

    /**
     * The main function
     */
    @Override
    public void run() {


        for (Iterator it = getCloudletList().iterator(); it.hasNext();) {
            Cloudlet cloudlet = (Cloudlet) it.next();
            boolean stillHasVm = false;
            for (Iterator itc = getVmList().iterator(); itc.hasNext();) {

                CondorVM vm = (CondorVM) itc.next();
                if (vm.getState() == WorkflowSimTags.VM_STATUS_IDLE) {
                    stillHasVm = true;
                    vm.setState(WorkflowSimTags.VM_STATUS_BUSY);
                    cloudlet.setVmId(vm.getId());
                    getScheduledList().add(cloudlet);
                    break;
                }
            }
            //no vm available 
            if (!stillHasVm) {
                break;
            }
        }
    }
}
