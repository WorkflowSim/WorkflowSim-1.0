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
        Map<Integer, List<TaskSet>> map = getLevelMap();
        for (List<TaskSet> taskList : map.values()) {
            process(taskList);
        }

    }

    /**
     * Sort taskSet based on their impact factors and then merge similar taskSet together
     * @param taskList 
     */
    public void process(List<TaskSet> taskList) {

        if (taskList.size() > getClusterNum()) {
            List<TaskSet> jobList = new ArrayList<>();
            for (int i = 0; i < getClusterNum(); i++) {
                jobList.add(new TaskSet());
            }
            int clusters_size = taskList.size() / getClusterNum();
            if(clusters_size * getClusterNum() < taskList.size()){
                clusters_size ++;
            }
            sortListDecreasing(taskList);
            for (TaskSet set : taskList) {
                //sortListIncreasing(jobList);
                //Log.printLine(set.getJobRuntime());
                TaskSet job = null;
                try{
                    job = getCandidateTastSet(jobList, set, clusters_size);
                }catch(Exception e) {
                    e.printStackTrace();
                }
                addTaskSet2TaskSet(set, job);
                job.addTask(set.getTaskList());
                job.setImpactFafctor(set.getImpactFactor());
                //update dependency
                for (Task task : set.getTaskList()) {
                    getTaskMap().put(task, job);//this is enough
                }
            }
            taskList.clear();
        } 
    }

    /**
     * Sort taskSet in an ascending order of impact factor
     * @param taskList taskSets to be sorted
     */
    private void sortListIncreasing(List taskList) {
        Collections.sort(taskList, new Comparator<TaskSet>() {
            @Override
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
    private void sortListDecreasing(List taskList) {
        
        Collections.sort(taskList, new Comparator<TaskSet>() {
        @Override
        public int compare(TaskSet t1, TaskSet t2) {
            //Decreasing order
            if(Math.abs(t2.getImpactFactor() - t1.getImpactFactor()) > 1.0e-8){
                if(t1.getImpactFactor() > t2.getImpactFactor()){
                    return 1;
                }else if(t1.getImpactFactor() < t2.getImpactFactor()){
                    return -1;
                }else{
                    return 0;
                }                        
            }
            else{
                if (t1.getJobRuntime() > t2.getJobRuntime()){
                    return 1;
                }else if (t1.getJobRuntime() < t2.getJobRuntime()) {
                    return -1;
                }else{
                    return 0;
                }
            }

        }
        });
    
    }
    
    private List<TaskSet> getNextPotentialTaskSets(List<TaskSet> taskList, 
                                            TaskSet checkSet, int clusters_size){

        Map<Double, List<TaskSet>> map = new HashMap<>();
        
        for (TaskSet set : taskList) {
                double factor = set.getImpactFactor();

                if(!map.containsKey(factor)){
                    map.put(factor, new ArrayList<>());
                }
                List<TaskSet> list = map.get(factor);
                if(!list.contains(set)){
                    list.add(set);
                }
        }
        List<TaskSet> returnList = new ArrayList<> ();
        List<TaskSet> mapSet = map.get(checkSet.getImpactFactor());
        if(mapSet!=null && !mapSet.isEmpty()){
            for(TaskSet set: mapSet){
                if(set.getTaskList().size() < clusters_size){
                    returnList.add(set);
                }
            }
        }
        
        if(returnList.isEmpty()){
            List<TaskSet> zeros = map.get(0.0);
            if(zeros!=null && !zeros.isEmpty())
            {
                returnList.addAll(zeros);
            }
        }
        
        if(returnList.isEmpty()){
            returnList.clear();//?
            for (TaskSet set : taskList) {
                if(set.getTaskList().isEmpty()){
                    returnList.add(set);
                    return returnList;
                }
            }
            map.remove(checkSet.getImpactFactor());
            //no empty available
            while(returnList.isEmpty() ){
                
                List<Double> keys = new ArrayList(map.keySet());
                double min = Double.MAX_VALUE;
                
                double min_i = -1;
                for(double i: keys){
                    double distance = Math.abs(i - checkSet.getImpactFactor());
                    if (distance < min){
                        min = distance;
                        min_i = i;
                    }
                }
                if(min_i>=0){
                    for(TaskSet set: map.get(min_i)){
                        if(set.getTaskList().size() < clusters_size){
                            returnList.add(set);
                        }
                    }
                }else{
                    return null;
                }
                map.remove(min_i);
            }
        }
        return returnList;            

    }
    
    
    /**
     * Gets the potential candidate taskSets to merge
     * @param taskList
     * @param checkSet
     * @param clusters_size
     * @return 
     */
    protected TaskSet getCandidateTastSet(List<TaskSet> taskList, 
                                            TaskSet checkSet, 
                                            int clusters_size) {
        
        
        
        List<TaskSet> potential = null;
        try{
            potential=getNextPotentialTaskSets(taskList, checkSet,  clusters_size);
        }catch (Exception e)
        {
            e.printStackTrace();
        }
        TaskSet task = null;
        long max = Long.MIN_VALUE;
        for(TaskSet set: potential){
            if(set.getJobRuntime() > max){
                max = set.getJobRuntime();
                task = set;
            }
        }

        if (task != null) {
            return task;
        } else {
            return taskList.get(0);
        }
    }
}
