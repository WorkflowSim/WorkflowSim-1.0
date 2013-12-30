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

//    protected TaskSet getCandidateTastSet(ArrayList<TaskSet> taskList, TaskSet checkSet) {
//        long min = taskList.get(0).getJobRuntime();
//        for (TaskSet set : taskList) {
//            if (set.getJobRuntime() == min && checkSet.getImpactFactor() == set.getImpactFactor()) {
//                return set;
//            }
//        }
//        return taskList.get(0);
//
//    }

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
            int clusters_size = taskList.size() / getClusterNum();
            if(clusters_size * getClusterNum() < taskList.size()){
                clusters_size ++;
            }
            sortListDecreasing(taskList);
            //preprocessing(taskList, jobList);
            
            

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
                    //impact factor is not updated
                }

            }

//            Log.printLine("level");
//            for(int i = 0; i < taskList.size();i++){
//                TaskSet set = (TaskSet) (taskList.get(i));
//                Log.printLine("TaskSet ");
//                for(int j = 0; j < set.getTaskList().size(); j++){
//                    Task task = set.getTaskList().get(j);
//                    Log.printLine("Task: " + task.getImpact());
//                }
//            }
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
    
    
//    private ArrayList preprocessing(ArrayList<TaskSet> taskList, ArrayList<TaskSet> jobList){
//        int size = taskList.size();
//        int [] record  = new int[size];
//        for(int i = 0; i < size; i++){
//            record[i]= -1;
//        }
//        int index_record = 0;
//        
//        int[][] distances = new int[size][size];
//        
//
//        for(int i = 0; i < size; i++){
//            for(int j = 0; j <i; j++){
//                TaskSet setA = (TaskSet)taskList.get(i);
//                TaskSet setB = (TaskSet)taskList.get(j);
//                int distance ;//= calDistance(setA, setB);
//                
//                distances[i][j] = distance;
//                
//
//                
//                
//
//            }
//        }
//        int job_index = 0;
//        //boolean [] popped = new boolean[size];
//        ArrayList idList;// = sortDistanceIncreasing(distances, size, jobList.size());
//        for(int i = 0; i < idList.size(); i++){
//            int max_i = (Integer)idList.get(i);
//                
//                record[index_record] = max_i;
//                index_record ++;
//                TaskSet set = (TaskSet)taskList.get(max_i);
//                TaskSet job = jobList.get(job_index);
//                addTaskSet2TaskSet(set, job);
//                job.addTask(set.getTaskList());
//                job.setImpactFafctor(set.getImpactFactor());
//                //update dependency
//                for (Task task : set.getTaskList()) {
//                    getTaskMap().put(task, job);//this is enough
//                    //impact factor is not updated
//                }
//                job_index ++;
//                if(job_index == jobList.size()){
//                    break;
//                }
//            
//        }
//
//
//        /**
//         * Actually not really necessary because record[i] is already empty
//         */
//        Arrays.sort(record);
//        for(int i = size -1 ;i>=0 && record[i]>=0;i--){
//            taskList.remove(record[i]);
//            
//            
//        }
//        
//        return taskList;
//        
//    }
//favor empty set
    private ArrayList<TaskSet> getNextPotentialTaskSets(ArrayList<TaskSet> taskList, 
                                            TaskSet checkSet, int clusters_size){

        HashMap map = new HashMap<Double, ArrayList>();
        
        for (TaskSet set : taskList) {
                double factor = set.getImpactFactor();

                if(!map.containsKey(factor)){
                    map.put(factor, new ArrayList<TaskSet>());
                }
                ArrayList<TaskSet> list = (ArrayList)map.get(factor);
                if(!list.contains(set)){
                    list.add(set);
                }
        }
        ArrayList returnList = new ArrayList<TaskSet> ();
        ArrayList<TaskSet> mapSet = (ArrayList<TaskSet>)map.get(checkSet.getImpactFactor());
        if(mapSet!=null && !mapSet.isEmpty()){
            for(TaskSet set: mapSet){
                if(set.getTaskList().size() < clusters_size){
                    returnList.add(set);
                }
            }
        }
        
        if(returnList.isEmpty()){
            ArrayList<TaskSet> zeros = (ArrayList<TaskSet>)map.get(0.0);
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
                
                ArrayList<Double> keys = new ArrayList(map.keySet());
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
                    for(TaskSet set: (ArrayList<TaskSet>)map.get(min_i)){
                        if(set.getTaskList().size() < clusters_size){
                            returnList.add(set);
                        }
                    }
                }else{
                    return null;
                }
                map.remove(min_i);
            }
            
            return returnList;
            
        }else{
            
            return returnList;
        }
        

    }
    
    
    /**
     * Gets the potential candidate taskSets to merge
     * @param taskList
     * @param checkSet
     * @return 
     */
    protected TaskSet getCandidateTastSet(ArrayList<TaskSet> taskList, 
                                            TaskSet checkSet, 
                                            int clusters_size) {
        
        
        
        ArrayList<TaskSet> potential = null;
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
