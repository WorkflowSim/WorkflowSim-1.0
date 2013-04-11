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
import java.util.Iterator;
import java.util.Map;
import org.workflowsim.Task;
import org.workflowsim.clustering.TaskSet;

/**
 * HorizontalImpactBalancing is a method that merges tasks that have similar impact
 * factors. 
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class HorizontalImpactBalancing extends BalancingMethod {

    /**
     * Initialize a HorizontalImpactBalancing object
     * @param levelMap the level map
     * @param taskMap the task map
     * @param clusterNum the clusters.num
     */
    public HorizontalImpactBalancing(Map levelMap, Map taskMap, int clusterNum) {
        super(levelMap, taskMap, clusterNum);
    }

    /**
     * The main function
     */
    @Override
    public void run() {
        Map<Integer, ArrayList<TaskSet>> map = getLevelMap();
        for (Iterator it = map.values().iterator(); it.hasNext();) {
            ArrayList<TaskSet> taskList = (ArrayList) it.next();
            process(taskList);

        }

    }

    protected TaskSet getCandidateTastSet(ArrayList<TaskSet> taskList, TaskSet checkSet) {
        long min = taskList.get(0).getJobRuntime();
        for (TaskSet set : taskList) {
            if (set.getJobRuntime() == min && checkSet.getImpactFactor() == set.getImpactFactor()) {
                return set;
            }
        }
        return taskList.get(0);

    }

    /**
     * Sort taskSet based on their impact factors and then merge similar taskSet together
     * @param taskList 
     */
    public void process(ArrayList<TaskSet> taskList) {

        if (taskList.size() > getClusterNum()) {
            ArrayList<TaskSet> jobList = new ArrayList<TaskSet>();
            for (int i = 0; i < getClusterNum(); i++) {
                jobList.add(new TaskSet());
            }
            sortListDecreasing(taskList);
            for (TaskSet set : taskList) {
                sortListIncreasing(jobList);
                TaskSet job = getCandidateTastSet(jobList, set);
                addTaskSet2TaskSet(set, job);
                job.addTask(set.getTaskList());
                job.setImpactFafctor(set.getImpactFactor());
                //update dependency
                for (Task task : set.getTaskList()) {
                    getTaskMap().put(task, job);//this is enough
                    //impact factor is not updated
                }

            }

            taskList.clear();//you sure?
        } else {
            //do nothing since 
        }


    }

    /**
     * Sort taskSet in an ascending order of impact factor
     * @param taskList taskSets to be sorted
     */
    private void sortListIncreasing(ArrayList taskList) {
        Collections.sort(taskList, new Comparator<TaskSet>() {
            public int compare(TaskSet t1, TaskSet t2) {
                //Decreasing order
                return (int) (t1.getJobRuntime() - t2.getJobRuntime());

            }
        });

    }
    /**
     * Sort taskSet in a descending order of impact factor
     * @param taskList taskSets to be sorted
     */
    private void sortListDecreasing(ArrayList taskList) {
        Collections.sort(taskList, new Comparator<TaskSet>() {
            public int compare(TaskSet t1, TaskSet t2) {
                //Decreasing order
                return (int) (t2.getJobRuntime() - t1.getJobRuntime());

            }
        });

    }

}
