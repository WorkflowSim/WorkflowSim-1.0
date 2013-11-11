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

import java.util.Iterator;
import java.util.Random;
import org.workflowsim.CondorVM;
import org.workflowsim.Task;
import org.workflowsim.utils.Parameters;

/**
 * The Random planning algorithm. This is just for demo. It is not useful in practice.
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Jun 17, 2013
 */
public class RandomPlanningAlgorithm extends BasePlanningAlgorithm {

    /**
     * The main function
     */
    @Override
    public void run() {

        Random random = new Random(System.currentTimeMillis());
        for (Iterator it = getTaskList().iterator(); it.hasNext();) {
            Task task = (Task) it.next();
            double duration = task.getCloudletLength() / 1000;
            
            for(int i = 0; i < task.getParentList().size(); i++ ){
                Task parent = task.getParentList().get(i);
            }
            
            
            for(int i = 0; i < task.getChildList().size(); i++ ){
                Task child = task.getChildList().get(i);
            }
            
            int vmNum = getVmList().size();
            /**
             * Randomly choose a vm
             */
            
            int vmId = random.nextInt(vmNum);
            
            CondorVM vm = (CondorVM) getVmList().get(vmId);
            //This shows the cpu capability of a vm
            double mips = vm.getMips();
            
            task.setVmId(vm.getId());
                    
            long deadline = Parameters.getDeadline();

        }
    }


}
