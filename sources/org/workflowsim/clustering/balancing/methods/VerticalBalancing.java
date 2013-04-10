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
import org.workflowsim.clustering.TaskSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * @author chenweiwei
 */
public class VerticalBalancing extends BalancingMethod{
    
    public VerticalBalancing(Map levelMap, Map taskMap, int clusterNum){
        super(levelMap, taskMap, clusterNum);
    }
    @Override
    public void run(){
        Collection sets = getTaskMap().values();
        for(Iterator it = sets.iterator();it.hasNext();){
            TaskSet set = (TaskSet)it.next();
            if(!set.hasChecked){
                set.hasChecked = true;
            }
            //check if you can merge it with its child
            ArrayList list = set.getChildList();
            if(list.size()==1){
                //
                TaskSet child = (TaskSet)list.get(0);
                ArrayList pList = child.getParentList();
                if(pList.size()==1){
                    //add parent to child (don't do it reversely)
                    addTaskSet2TaskSet(set, child);
                    
                }
            }
            
        }
        //within each method
        cleanTaskSetChecked();
    }
    
}
