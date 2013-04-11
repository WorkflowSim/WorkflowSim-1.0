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

import org.workflowsim.Task;
import org.workflowsim.clustering.balancing.methods.BalancingMethod;
import org.workflowsim.clustering.TaskSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import org.cloudbus.cloudsim.Log;

/**
 *
 * @author Weiwei Chen
 */
public class HorizontalImpactBalancing extends BalancingMethod {

    public HorizontalImpactBalancing(Map levelMap, Map taskMap, int clusterNum) {
        super(levelMap, taskMap, clusterNum);
    }

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


        //return null;
    }

    public void process(ArrayList<TaskSet> taskList) {

        if (taskList.size() > getClusterNum()) {
            ArrayList<TaskSet> jobList = new ArrayList<TaskSet>();
            for (int i = 0; i < getClusterNum(); i++) {
                jobList.add(new TaskSet());
            }
            sortListDecreasing(taskList);
            for (TaskSet set : taskList) {
                //MinHeap is required 
                sortListIncreasing(jobList);
                //TaskSet job = (TaskSet)jobList.get(0);
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

    private void sortListIncreasing(ArrayList taskList) {
        Collections.sort(taskList, new Comparator<TaskSet>() {
            public int compare(TaskSet t1, TaskSet t2) {
                //Decreasing order
//                if(t1.getImpactFactor() == t2.getImpactFactor()){
                return (int) (t1.getJobRuntime() - t2.getJobRuntime());
//                }else{
//                   return (int)(t1.getImpactFactor()- t2.getImpactFactor());
//                }
            }
        });

    }

    private void sortListDecreasing(ArrayList taskList) {
        Collections.sort(taskList, new Comparator<TaskSet>() {
            public int compare(TaskSet t1, TaskSet t2) {
                //Decreasing order
//                if(t1.getImpactFactor() == t2.getImpactFactor()){
                return (int) (t2.getJobRuntime() - t1.getJobRuntime());
//                }else{
//                   return (int)(t2.getImpactFactor()- t1.getImpactFactor());
//                }

            }
        });

    }

    public void process2(ArrayList<TaskSet> setList) {

        do {

            int size = setList.size();
            int count = size - getClusterNum();
            if (count <= 0) {
                return;
            }
            for (int i = 0; i < setList.size(); i++) {
                TaskSet set = setList.get(i);
                Log.printLine("Value " + set.getImpactFactor());
            }
            double min = Double.MAX_VALUE;
            for (int i = 0; i < setList.size(); i++) {
                TaskSet set = setList.get(i);
                if (set.getImpactFactor() < min) {
                    min = set.getImpactFactor();
                }
            }
            ArrayList<TaskSet> unList = new ArrayList();
            for (int i = 0; i < setList.size(); i++) {
                TaskSet set = setList.get(i);
                if (set.getImpactFactor() == min) {
                    unList.add(set);
                }
            }
            int num = this.getClusterNum();
            double avg = 1.0 / num;
            if (min >= avg) {
                return;
            }

            int avg_size = (int) (avg / min);
            if (size - getClusterNum() - size / avg_size <= 0) {
                return;
            }
            int index = 0;
            TaskSet cur_set = null;
            for (int i = 0; i < unList.size(); i++) {
                TaskSet set = unList.get(i);
                if (index == 0) {
                    index++;
                    cur_set = set;
                } else {
                    index++;
                    if (index == avg_size) {
                        index = 0;
                        setList.remove(cur_set);
                    }
                    addTaskSet2TaskSet(set, cur_set);
                    setList.remove(set);
                }
            }
            if (cur_set != null) {
                setList.remove(cur_set);
            }

            /*
             *find a small one 
             */
//            int min_i = 0;
//            int min_j = 0;
//            double min_diff = Double.MAX_VALUE;
//            boolean flag = false;
//            for(int i =0; i < setList.size(); i ++){
//                TaskSet setA = setList.get(i);
//                for(int j = i + 1; j < setList.size(); j++){
//                    TaskSet setB = setList.get(j);
//                    if(setA.getImpactFactor() == setB.getImpactFactor()){
//                        min_i = i;
//                        min_j = j;
//                        flag = true;
//                        break;
//                    }
//                    else{
//                        if(Math.abs(setA.getImpactFactor() - setB.getImpactFactor())<min_diff){
//                            min_diff = Math.abs(setA.getImpactFactor() - setB.getImpactFactor());
//                            min_i = i;
//                            min_j = j;
//                        }
//                    }
//                }
//                if(flag){
//                    break;
//                }
//            }
//            TaskSet setA = setList.get(min_i);
//            TaskSet setB = setList.get(min_j);
//            addTaskSet2TaskSet(setA, setB);
//            setList.remove(setA);
        } while (true);
    }
}
