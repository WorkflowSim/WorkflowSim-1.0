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
package org.workflowsim.clustering.balancing.methods;

import org.workflowsim.clustering.balancing.methods.BalancingMethod;
import org.workflowsim.clustering.AbstractArrayList;
import org.workflowsim.clustering.TaskSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Weiwei Chen
 */
public class ChildAwareHorizontalClustering extends BalancingMethod{
    
    public ChildAwareHorizontalClustering(Map levelMap, Map taskMap, int clusterNum){
        super(levelMap, taskMap, clusterNum);
    }
    @Override
    public void run(){
        Map<Integer,ArrayList<TaskSet>> map = getLevelMap();
        Map<ArrayList<TaskSet>, AbstractArrayList> tmpMap = new HashMap();
        for(Map.Entry entry: map.entrySet()){
            int depth = (Integer)entry.getKey();
            ArrayList<TaskSet> list = (ArrayList)entry.getValue();
            AbstractArrayList abList = new AbstractArrayList(list, depth);
            tmpMap.put(list, abList);            
        }
        ArrayList<AbstractArrayList> abList = new ArrayList(tmpMap.values());
        sortMap(abList);
        for(AbstractArrayList list : abList){
            if(!list.hasChecked){
                boolean hasClustered = CHBcheckLevel(list.getArrayList());
                //Log.printLine("Depth:"+list.getDepth());
                if(hasClustered){
                    list.hasChecked = true;
                    //check its parent levels
                    int depth = list.getDepth();
//                    for(int i = depth - 1; i >0 ; i --){//i=depth may work but too intensive
//                        ArrayList<TaskSet> tsList = (ArrayList)map.get(i);
//                        CHBcheckLevel(tsList);
//                        AbstractArrayList tsAbList = (AbstractArrayList)tmpMap.get(tsList);
//                        tsAbList.hasChecked = true;
//                    }
                    int i = depth + 1;
                    while(map.containsKey(i)){
                        ArrayList<TaskSet> tsList = (ArrayList)map.get(i);
                        CHBcheckLevel(tsList);
                        AbstractArrayList tsAbList = (AbstractArrayList)tmpMap.get(tsList);
                        tsAbList.hasChecked = true;
                        i ++;
                    }
                }
            }
        }
        //within each method
        cleanTaskSetChecked();
    }
    private void sortMap(ArrayList<AbstractArrayList> list){
        Collections.sort(list, new Comparator<AbstractArrayList>(){
            public int compare(AbstractArrayList l1, AbstractArrayList l2){
                
                return (int)(l2.getArrayList().size() - l1.getArrayList().size());
            }
        });
    
    }
     private boolean CHBcheckLevel(ArrayList taskList){
        boolean hasClustered = false;
        for(int i = 0 ; i < taskList.size(); i ++){
            TaskSet setA = (TaskSet)taskList.get(i);
            setA.hasChecked = false;//for safety
        }
        for(int i = 0 ; i < taskList.size(); i ++){
            TaskSet setA = (TaskSet)taskList.get(i);
            if(!setA.hasChecked){

                for(int j = i+1; j < taskList.size(); j++){
                    TaskSet setB = (TaskSet)taskList.get(j);
                    if(!setB.hasChecked){
                        //TaskSet kid = CHBhasOnlyChild(setA, setB);
                        //TaskSet kid = CHBhasOnlyParent(setA, setB);
                        TaskSet kid = CHBhasOneParent(setA, setB);
                        if(kid!=null){
                            if(true){//this condition is that the runtime is fine
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
      private TaskSet CHBhasOnlyChild(TaskSet setA, TaskSet setB){
        
        if(setA.getChildList().size()==1 && setB.getChildList().size()==1){
            TaskSet kidA = setA.getChildList().get(0);
            TaskSet kidB = setB.getChildList().get(0);
            if(kidA.equals(kidB)){
                return kidA;
            }
        }
        return null;
    }
    private TaskSet CHBhasOnlyParent(TaskSet setA, TaskSet setB){
        
        if(setA.getParentList().size()==1 && setB.getParentList().size()==1){
            TaskSet kidA = setA.getParentList().get(0);
            TaskSet kidB = setB.getParentList().get(0);
            if(kidA.equals(kidB)){
                return kidA;
            }
        }
        return null;
    }
    private TaskSet CHBhasOneParent(TaskSet setA, TaskSet setB){
        for(TaskSet parentA: setA.getParentList()){
            for (TaskSet parentB: setB.getParentList()){
                if(parentA.equals(parentB)){
                    return parentA;
                }
            }
        }
        return null;
    }

   
}
