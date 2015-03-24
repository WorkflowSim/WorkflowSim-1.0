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

import java.util.HashMap;
import java.util.Map;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.CondorVM;
import org.workflowsim.WorkflowSimTags;

/**
 * Static algorithm. Do not schedule at all and reply on Workflow Planner to set
 * the mapping relationship. But StaticSchedulingAlgorithm would check whether a
 * job has been assigned a VM in this stage (in case your implementation of
 * planning algorithm forgets it)
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Jun 17, 2013
 */
public class StaticSchedulingAlgorithm extends BaseSchedulingAlgorithm {

    public StaticSchedulingAlgorithm() {
        super();
    }

    @Override
    public void run() throws Exception {

        Map<Integer, CondorVM> mId2Vm = new HashMap<>();

        for (int i = 0; i < getVmList().size(); i++) {
            CondorVM vm = (CondorVM) getVmList().get(i);
            if (vm != null) {
                mId2Vm.put(vm.getId(), vm);
            }
        }

        int size = getCloudletList().size();

        for (int i = 0; i < size; i++) {
            Cloudlet cloudlet = (Cloudlet) getCloudletList().get(i);
            /**
             * Make sure cloudlet is matched to a VM. It should be done in the
             * Workflow Planner. If not, throws an exception because
             * StaticSchedulingAlgorithm itself does not do the mapping.
             */
            if (cloudlet.getVmId() < 0 || !mId2Vm.containsKey(cloudlet.getVmId())) {
                Log.printLine("Cloudlet " + cloudlet.getCloudletId() + " is not matched."
                        + "It is possible a stage-in job");
                cloudlet.setVmId(0);

            }
            CondorVM vm = mId2Vm.get(cloudlet.getVmId());
            if (vm.getState() == WorkflowSimTags.VM_STATUS_IDLE) {
                vm.setState(WorkflowSimTags.VM_STATUS_BUSY);
                getScheduledList().add(cloudlet);
                Log.printLine("Schedules " + cloudlet.getCloudletId() + " with "
                        + cloudlet.getCloudletLength() + " to VM " + cloudlet.getVmId());
            }
        }
    }
}
