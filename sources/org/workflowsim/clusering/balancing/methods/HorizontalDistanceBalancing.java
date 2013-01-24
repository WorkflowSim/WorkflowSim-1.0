/*
 * 
 *   Copyright 2007-2008 University Of Southern California
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
package org.workflowsim.clusering.balancing.methods;

import org.workflowsim.clusering.TaskSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import org.cloudbus.cloudsim.Log;

/**
 *
 * @author Weiwei Chen
 */
public class HorizontalDistanceBalancing extends HorizontalImpactBalancing{
    
    public HorizontalDistanceBalancing(Map levelMap, Map taskMap, int clusterNum){
        super(levelMap, taskMap, clusterNum);
    }
    @Override
    public void run(){
        Map<Integer,ArrayList<TaskSet>> map = getLevelMap();
        for(Iterator it = map.values().iterator();it.hasNext();){
            ArrayList<TaskSet> taskList = (ArrayList)it.next();
            process(taskList);
            //disMap.clear();
        }
        
    }
    @Override
    protected TaskSet getCandidateTastSet(ArrayList<TaskSet> taskList, TaskSet checkSet){
        long min = taskList.get(0).getJobRuntime();
        int dis = Integer.MAX_VALUE;
        TaskSet task = null;
        for(TaskSet set:taskList){
            if(set.getJobRuntime() == min){
                int distance = calDistance(checkSet, set);
                if(distance < dis){
                    dis = distance;
                    task = set;
                }
            }
        }
        
        if(task!=null){
            return task;
        }else{
            return taskList.get(0);
        }
      
        
        //return null;
    }
    /*
     * one assumption here taskA and taskB are at the same level 
     * because it is horizontal clustering
     * does not work arbitary workflows
     */
    private int calDistance(TaskSet taskA, TaskSet taskB){
        if(taskA == null || taskB == null || taskA == taskB)
        {
            return 0;
        }
        LinkedList<TaskSet> listA = new LinkedList<TaskSet>();
        LinkedList<TaskSet> listB = new LinkedList<TaskSet>();
        int distance = 0;
        listA.add(taskA);
        listB.add(taskB);

        if(taskA.getTaskList().isEmpty()||taskB.getTaskList().isEmpty()){
            return 0;
        }
        do{
            
            LinkedList<TaskSet> copyA = (LinkedList)listA.clone();
            listA.clear();
            for(TaskSet set: copyA){
                for(TaskSet child: set.getChildList()){
                    if(!listA.contains(child)){
                        listA.add(child);
                    }
                }
            }
            LinkedList<TaskSet> copyB = (LinkedList)listB.clone();
            listB.clear();
            for(TaskSet set: copyB){
                for(TaskSet child: set.getChildList()){
                    if(!listB.contains(child)){
                        listB.add(child);
                    }
                }
            }
            
            for(TaskSet set: listA){
                if(listB.contains(set)){
                    return distance * 2;
                }
            }
            
            distance ++;
            
        }while(!listA.isEmpty()&&!listB.isEmpty());
        
        return distance * 2;
    }

}
