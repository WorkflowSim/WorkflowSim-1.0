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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.workflowsim.Task;
import org.workflowsim.clustering.TaskSet;

/**
 * HorizontalRuntimeBalancing is a method that merges task so as to balance job runtime
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class HorizontalRuntimeBalancing extends BalancingMethod {

    /**
     * Initialize a HorizontalRuntimeBalancing object
     * @param levelMap the level map
     * @param taskMap the task map
     * @param clusterNum the clustes.num
     */
    public HorizontalRuntimeBalancing(Map levelMap, Map taskMap, int clusterNum) {
        super(levelMap, taskMap, clusterNum);
    }

    /**
     * The main function
     */
    @Override
    public void run() {
        Map<Integer, List<TaskSet>> map = getLevelMap();
        for (List<TaskSet> taskList : map.values()) {
            /**The reason why we don shuffle is very complicated. */
            long seed = System.nanoTime();
            Collections.shuffle(taskList, new Random(seed));
            seed = System.nanoTime();
            Collections.shuffle(taskList, new Random(seed));

            if (taskList.size() > getClusterNum()) {
                List<TaskSet> jobList = new ArrayList<>();
                for (int i = 0; i < getClusterNum(); i++) {
                    jobList.add(new TaskSet());
                }
                sortListDecreasing(taskList);
                for (TaskSet set : taskList) {
                    //MinHeap is required 
                    sortListIncreasing(jobList);
                    TaskSet job = (TaskSet) jobList.get(0);
                    job.addTask(set.getTaskList());
                    //update dependency
                    for (Task task : set.getTaskList()) {
                        getTaskMap().put(task, job);//this is enough
                    }

                }

                taskList.clear();//you sure?
            } else {
                //do nothing since 
            }

        }
    }
    /**
     * Sort taskSets based on their runtime
     * @param taskList taskSets to be sorted
     */
    private void sortListIncreasing(List<TaskSet> taskList) {
        Collections.sort(taskList, new Comparator<TaskSet>() {
            @Override
            public int compare(TaskSet t1, TaskSet t2) {
                //Decreasing order
                return (int) (t1.getJobRuntime() - t2.getJobRuntime());
            }
        });

    }

    /**
     * Sort taskSets based on their runtime
     * @param taskList taskSets to be sorted
     */
    private void sortListDecreasing(List<TaskSet> taskList) {
        Collections.sort(taskList, new Comparator<TaskSet>() {
            @Override
            public int compare(TaskSet t1, TaskSet t2) {
                //Decreasing order
                return (int) (t2.getJobRuntime() - t1.getJobRuntime());
            }
        });

    }
}
