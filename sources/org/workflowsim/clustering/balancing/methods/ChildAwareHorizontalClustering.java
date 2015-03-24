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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.workflowsim.clustering.AbstractArrayList;
import org.workflowsim.clustering.TaskSet;

/**
 * ChildAwareHorizontalClsutering is a method that merges a task and its children
 * 
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class ChildAwareHorizontalClustering extends BalancingMethod {

    /**
     * Initialize a ChildAwareHorizontalClustering object
     * @param levelMap the level map
     * @param taskMap the task map
     * @param clusterNum the clusters.num
     */
    public ChildAwareHorizontalClustering(Map levelMap, Map taskMap, int clusterNum) {
        super(levelMap, taskMap, clusterNum);
    }

    /**
     * The main function
     */
    @Override
    public void run() {
        Map<Integer, List<TaskSet>> map = getLevelMap();
        Map<List<TaskSet>, AbstractArrayList> tmpMap = new HashMap();
        for (Map.Entry entry : map.entrySet()) {
            int depth = (Integer) entry.getKey();
            ArrayList<TaskSet> list = (ArrayList) entry.getValue();
            AbstractArrayList abList = new AbstractArrayList(list, depth);
            tmpMap.put(list, abList);
        }
        List<AbstractArrayList> abList = new ArrayList(tmpMap.values());
        sortMap(abList);
        for (AbstractArrayList list : abList) {
            if (!list.hasChecked) {
                boolean hasClustered = CHBcheckLevel(list.getArrayList());
                //Log.printLine("Depth:"+list.getDepth());
                if (hasClustered) {
                    list.hasChecked = true;
                    //check its parent levels
                    int depth = list.getDepth();

                    int i = depth + 1;
                    while (map.containsKey(i)) {
                        List<TaskSet> tsList = map.get(i);
                        CHBcheckLevel(tsList);
                        AbstractArrayList tsAbList = tmpMap.get(tsList);
                        tsAbList.hasChecked = true;
                        i++;
                    }
                }
            }
        }
        //within each method
        cleanTaskSetChecked();
    }

    /**
     * Sort taskSets based on their size
     * @param list taskSets to be sorted
     */
    private void sortMap(List<AbstractArrayList> list) {
        Collections.sort(list, new Comparator<AbstractArrayList>() {
            @Override
            public int compare(AbstractArrayList l1, AbstractArrayList l2) {

                return (int) (l2.getArrayList().size() - l1.getArrayList().size());
            }
        });

    }

    /**
     * Process taskSets level by level
     * @param taskList to be processed
     * @return 
     */
    private boolean CHBcheckLevel(List<TaskSet> taskList) {
        boolean hasClustered = false;
        for (TaskSet setA : taskList) {
            setA.hasChecked = false;//for safety
        }
        for (int i = 0; i < taskList.size(); i++) {
            TaskSet setA =  taskList.get(i);
            if (!setA.hasChecked) {
                for (int j = i + 1; j < taskList.size(); j++) {
                    TaskSet setB = taskList.get(j);
                    if (!setB.hasChecked) {

                        TaskSet kid = CHBhasOneParent(setA, setB);
                        if (kid != null) {
                            if (true) {//this condition is that the runtime is fine
                                setA.hasChecked = true;//it does not matter
                                setB.hasChecked = true;
                                addTaskSet2TaskSet(setA, setB);
                                hasClustered = true;
                            }
                        }
                    }
                }
            }
        }
        return hasClustered;
    }

    /**
     * Checks whether two taskSets have only the same child
     * @param setA the first taskSet to compare
     * @param setB the second taskSet to compare
     * @return whether they have the same child alone
     */
    private TaskSet CHBhasOnlyChild(TaskSet setA, TaskSet setB) {

        if (setA.getChildList().size() == 1 && setB.getChildList().size() == 1) {
            TaskSet kidA = setA.getChildList().get(0);
            TaskSet kidB = setB.getChildList().get(0);
            if (kidA.equals(kidB)) {
                return kidA;
            }
        }
        return null;
    }

    /**
     * Checks whether two taskSets have only the same parent
     * @param setA the first taskSet to compare
     * @param setB the second taskSet to compare
     * @return whether they have the same parent alone
     */
    private TaskSet CHBhasOnlyParent(TaskSet setA, TaskSet setB) {

        if (setA.getParentList().size() == 1 && setB.getParentList().size() == 1) {
            TaskSet kidA = setA.getParentList().get(0);
            TaskSet kidB = setB.getParentList().get(0);
            if (kidA.equals(kidB)) {
                return kidA;
            }
        }
        return null;
    }
    
    /**
     * Checks whether two taskSets have one common parent
     * @param setA the first taskSet to compare
     * @param setB the second taskSet to compare
     * @return whether they have the same parent 
     */
    private TaskSet CHBhasOneParent(TaskSet setA, TaskSet setB) {
        for (TaskSet parentA : setA.getParentList()) {
            for (TaskSet parentB : setB.getParentList()) {
                if (parentA.equals(parentB)) {
                    return parentA;
                }
            }
        }
        return null;
    }
}
