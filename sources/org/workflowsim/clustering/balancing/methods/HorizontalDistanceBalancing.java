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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import org.workflowsim.clustering.TaskSet;

/**
 *  HorizontalDistanceBalancing is a method that merges tasks based on distance metric
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class HorizontalDistanceBalancing extends HorizontalImpactBalancing {

    /**
     * Initialize a HorizontalDistanceBalancing object
     * @param levelMap the level map
     * @param taskMap the task map
     * @param clusterNum the clusters.num
     */
    public HorizontalDistanceBalancing(Map levelMap, Map taskMap, int clusterNum) {
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

    /**
     * Gets the potential candidate taskSets to merge
     * @param taskList
     * @param checkSet
     * @return 
     */
    @Override
    protected TaskSet getCandidateTastSet(ArrayList<TaskSet> taskList, TaskSet checkSet) {
        long min = taskList.get(0).getJobRuntime();
        int dis = Integer.MAX_VALUE;
        TaskSet task = null;
        for (TaskSet set : taskList) {
            if (set.getJobRuntime() == min) {
                int distance = calDistance(checkSet, set);
                if (distance < dis) {
                    dis = distance;
                    task = set;
                }
            }
        }

        if (task != null) {
            return task;
        } else {
            return taskList.get(0);
        }


    }

    /**
     * Calculate the distance between two taskSet
     * one assumption here taskA and taskB are at the same level 
     * because it is horizontal clustering
     * does not work with arbitary workflows
     * @param taskA
     * @param taskB
     * @return 
     */
    private int calDistance(TaskSet taskA, TaskSet taskB) {
        if (taskA == null || taskB == null || taskA == taskB) {
            return 0;
        }
        LinkedList<TaskSet> listA = new LinkedList<TaskSet>();
        LinkedList<TaskSet> listB = new LinkedList<TaskSet>();
        int distance = 0;
        listA.add(taskA);
        listB.add(taskB);

        if (taskA.getTaskList().isEmpty() || taskB.getTaskList().isEmpty()) {
            return 0;
        }
        do {

            LinkedList<TaskSet> copyA = (LinkedList) listA.clone();
            listA.clear();
            for (TaskSet set : copyA) {
                for (TaskSet child : set.getChildList()) {
                    if (!listA.contains(child)) {
                        listA.add(child);
                    }
                }
            }
            LinkedList<TaskSet> copyB = (LinkedList) listB.clone();
            listB.clear();
            for (TaskSet set : copyB) {
                for (TaskSet child : set.getChildList()) {
                    if (!listB.contains(child)) {
                        listB.add(child);
                    }
                }
            }

            for (TaskSet set : listA) {
                if (listB.contains(set)) {
                    return distance * 2;
                }
            }

            distance++;

        } while (!listA.isEmpty() && !listB.isEmpty());

        return distance * 2;
    }
}
