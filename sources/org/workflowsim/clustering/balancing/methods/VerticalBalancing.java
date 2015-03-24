/*
 * 
 *  Copyright 2012-2013 University Of Southern California
 * 
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 * 
 */
package org.workflowsim.clustering.balancing.methods;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.workflowsim.clustering.TaskSet;

/**
 * VerticalBalancing is the same vc as in org.workflowsim.clustering. Rewrite here so as
 * to test it again with other balancing methods
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class VerticalBalancing extends BalancingMethod {

    /**
     * Initialize a VerticalBalancing object
     * @param levelMap the level map
     * @param taskMap the task map
     * @param clusterNum the clusters.num
     */
    public VerticalBalancing(Map levelMap, Map taskMap, int clusterNum) {
        super(levelMap, taskMap, clusterNum);
    }

    /**
     * The main function
     */
    @Override
    public void run() {
        Collection<TaskSet> sets = getTaskMap().values();
        for (TaskSet set : sets) {
            if (!set.hasChecked) {
                set.hasChecked = true;
            }
            //check if you can merge it with its child
            List<TaskSet> list = set.getChildList();
            if (list.size() == 1) {
                //
                TaskSet child = list.get(0);
                List pList = child.getParentList();
                if (pList.size() == 1) {
                    //add parent to child (don't do it reversely)
                    addTaskSet2TaskSet(set, child);
                }
            }
        }
        //within each method
        cleanTaskSetChecked();
    }
}
