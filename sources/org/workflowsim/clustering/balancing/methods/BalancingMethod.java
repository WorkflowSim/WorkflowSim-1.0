/*
 * 
 *   Copyright 2012-2013 University Of Southern California
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.workflowsim.Task;
import org.workflowsim.clustering.TaskSet;

/**
 * BlancingMethod is the super class of all balancing methods
 * 
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class BalancingMethod {

    /** Task Map */
    private final Map<Task, TaskSet> taskMap;
    
    /**
     * TaskSet per level.
     */
    private final Map<Integer, List<TaskSet>> levelMap;
    /** Number of clustered jobs per level. */
    private final int clusterNum;

    /**
     * Initialize a BalancingMethod
     * @param levelMap, taskSets per level
     * @param taskMap, map from task to taskSet
     * @param clusterNum number of clustered jobs
     */
    public BalancingMethod(Map levelMap, Map taskMap, int clusterNum) {
        this.taskMap = taskMap;
        this.levelMap = levelMap;
        this.clusterNum = clusterNum;
    }

    /**
     * Gets the task map
     * @return task map
     */
    public Map<Task, TaskSet> getTaskMap() {
        return this.taskMap;
    }

    /**
     * Gets the level map
     * @return level map
     */
    public Map<Integer, List<TaskSet>> getLevelMap() {
        return this.levelMap;
    }

    /**
     * Gets clusters.num
     * @return clusters.num
     */
    public int getClusterNum() {
        return this.clusterNum;
    }
    
    /**
     * Add all the tasks in tail to head and then clean tail. Can be reused with 
     * verticalClustering()
     * @param tail
     * @param head 
     */
    public void addTaskSet2TaskSet(TaskSet tail, TaskSet head) {
        head.addTask(tail.getTaskList());
        head.getParentList().remove(tail);
        //update manually, beautifully, I like it here
        for (Task task : tail.getTaskList()) {
            getTaskMap().put(task, head);
        }
        /*
         * At the same level you can do so, but for vc it doens't, 
         * while usually for vc we don't need to calculate impact
         */
        head.setImpactFafctor(head.getImpactFactor() + tail.getImpactFactor());
        for (TaskSet taskSet : tail.getParentList()) {
            taskSet.getChildList().remove(tail);
            if (!taskSet.getChildList().contains(head)) {
                taskSet.getChildList().add(head);
            }
            if (!head.getParentList().contains(taskSet)) {
                head.getParentList().add(taskSet);
            }
        }

        for (TaskSet taskSet : tail.getChildList()) {
            taskSet.getParentList().remove(tail);
            if (!taskSet.getParentList().contains(head)) {
                taskSet.getParentList().add(head);
            }
            if (!head.getChildList().contains(taskSet)) {
                head.getChildList().add(taskSet);
            }
        }
        tail.getTaskList().clear();
        tail.getChildList().clear();
        tail.getParentList().clear();
    }
    /**
     * BalancingMethod is the super class and it should not be used.
     */
    public void run() {
        throw (new RuntimeException("Should not use this function"));
    }

    /**
     * Clean all the check points
     */
    public void cleanTaskSetChecked() {
        Collection sets = getTaskMap().values();
        for (Iterator it = sets.iterator(); it.hasNext();) {
            TaskSet set = (TaskSet) it.next();
            set.hasChecked = false;
        }
    }
}
