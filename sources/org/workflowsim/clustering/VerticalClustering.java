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
package org.workflowsim.clustering;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import org.workflowsim.Task;
import org.workflowsim.utils.Parameters;

/**
 * VerticalClustering merges tasks at the same pipeline
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class VerticalClustering extends BasicClustering {

    /* The maximum depth to explore. */
    private final int mDepth;
    /* The checkpoint map. */
    private final Map<Integer, Boolean> mHasChecked;

    /**
     * Initialize a VeriticalClustering object
     *
     * @param depth depth
     */
    public VerticalClustering(int depth) {
        super();
        this.mDepth = depth;
        this.mHasChecked = new HashMap<>();

    }

    /**
     * Sets the checkpoint of a task
     *
     * @param index id of a task
     */
    private void setCheck(int index) {

        if (mHasChecked.containsKey(index)) {
            mHasChecked.remove(index);
        }
        mHasChecked.put(index, true);

    }

    /**
     * Gets the checkpoint of a task
     *
     * @param index id of a task
     * @return
     */
    private boolean getCheck(int index) {

        if (mHasChecked.containsKey(index)) {
            return mHasChecked.get(index);
        }
        return false;
    }

    /**
     * The main function
     */
    @Override
    public void run() {
        if (mDepth > 0) {
            /**
             * Specially for Montage workflow since it has duplicate edges
             */
            if (Parameters.getReduceMethod().equals("montage")) {
                removeDuplicateMontage();
            }
            Task root = super.addRoot();
            Task node;
            List<Task> taskList = new ArrayList<>();
            Stack<Task> stack = new Stack<>();
            stack.push(root);
            while (!stack.empty()) {
                node = (Task) stack.pop();
                if (!getCheck(node.getCloudletId())) {
                    setCheck(node.getCloudletId());

                    int pNum = node.getParentList().size();
                    int cNum = node.getChildList().size();

                    for (Task cNode : node.getChildList()) {
                        stack.push(cNode);
                    }

                    if (pNum == 0) {
                        //root skip it
                    } else if (pNum > 1) {
                        if (cNum > 1 || cNum == 0) {

                            if (!taskList.isEmpty()) {
                                addTasks2Job(taskList);
                                taskList.clear();
                            }
                            taskList.add(node);
                            addTasks2Job(taskList);
                            taskList.clear();

                        } else {//cNum==1
                            //cut and add new

                            if (!taskList.isEmpty()) {
                                addTasks2Job(taskList);
                                taskList.clear();
                            }
                            if (!taskList.contains(node)) {
                                taskList.add(node);
                            }
                        }
                    } else {//pNum == 1
                        if (cNum > 1 || cNum == 0) {
                            //This is different to the case of pNum > 1
                            taskList.add(node);
                            addTasks2Job(taskList);
                            taskList.clear();
                        } else {
                            //This is also different to the case of pNum > 1
                            if (!taskList.contains(node)) {
                                taskList.add(node);
                            }
                        }
                    }

                } else {
                    if (!taskList.isEmpty()) {
                        addTasks2Job(taskList);
                        taskList.clear();
                    }
                }
            }
        }
        mHasChecked.clear();
        super.clean();        
        updateDependencies();
        addClustDelay();
    }

    /**
     * Remove duplicate just for Montage Set in reducer.method
     */
    public void removeDuplicateMontage() {

        List jobList = this.getTaskList();
        for (Object jobList1 : jobList) {
            Task node = (Task) jobList1;
            String name = node.getType();
            switch (name) {
                case "mBackground":
                    //remove all of its parents of mProjectPP
                    
                    for (int j = 0; j < node.getParentList().size(); j++) {
                        
                        Task parent = (Task) node.getParentList().get(j);
                        if (parent.getType().equals("mProjectPP")) {
                            j--;
                            node.getParentList().remove(parent);
                            parent.getChildList().remove(node);
                        }
                    }   break;
                case "mAdd":
                    for (int j = 0; j < node.getParentList().size(); j++) {
                        
                        Task parent = (Task) node.getParentList().get(j);
                        String pName = parent.getType();
                        if (pName.equals("mBackground") || pName.equals("mShrink")) {
                            j--;
                            node.getParentList().remove(parent);
                            parent.getChildList().remove(node);
                    }
                }   break;
            }
        }
    }
}
