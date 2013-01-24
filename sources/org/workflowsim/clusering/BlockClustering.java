/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.workflowsim.clusering;

import org.workflowsim.Task;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.cloudbus.cloudsim.Log;

/**
 *
 * @author Weiwei Chen
 */
public class BlockClustering extends BasicClustering{
    
    
    private int clusterNum;
    private int clusterSize;
    private Map mHasChecked;
    private Map mDepth2Task;
    

    public BlockClustering(int cNum, int cSize){
        super();
        clusterNum = cNum;
        clusterSize = cSize;
        this.mHasChecked = new HashMap<Integer, Boolean>();
        this.mDepth2Task = new HashMap<Integer, Map>();

    }
    
    private void setCheck(int index){
        
        if(mHasChecked.containsKey(index)){
            mHasChecked.remove(index);
        }
        mHasChecked.put(index, true);
        
                
    }
    private boolean getCheck(int index){

        if(mHasChecked.containsKey(index)){
            return (Boolean)mHasChecked.get(index);
        }
        return false;
    }
    
    @Override
    public void run()
 
    {
            
        // level by level
        if(clusterNum >0 || clusterSize>0){
            for(Iterator it = getTaskList().iterator();it.hasNext();)
            {
                Task task = (Task)it.next();
                int depth = task.getDepth();
                if(!mDepth2Task.containsKey(depth)){
                    mDepth2Task.put(depth, new ArrayList<Task>());
                }
                ArrayList list = (ArrayList)mDepth2Task.get(depth);
                if (!list.contains(task)){
                    list.add(task);
                }

            }
        }
           
        
        if(clusterNum > 0){
            bundleClustering();
        }else if(clusterSize >0){
            collapseClustering();
        }
        
        mHasChecked.clear();
        super.clean();

        updateDependencies();
        addClustDelay();
    }
    
        
    private List searchList(List taskList){
        ArrayList sucList = new ArrayList<Task>();
        for(Iterator it = taskList.iterator(); it.hasNext();){
            Task task =  (Task)it.next();
            if(getCheck(task.getCloudletId())){
                //do not process this task it's been checked
            }else{
                setCheck(task.getCloudletId());
                sucList.add(task);
                //add all of its successors
                Task node = task;
                while(node!=null){
                    int pNum = node.getParentList().size();
                    int cNum = node.getChildList().size();
                    if(cNum==1 ){
                        Task child = (Task)node.getChildList().get(0);
                        if(!getCheck(child.getCloudletId()) && child.getParentList().size() == 1){
                            setCheck(child.getCloudletId());
                            sucList.add(child);
                            node = child;
                        }else{
                            node = null;
                        }
                        
                    }else{
                        node = null;
                        //cNum == 0, doesn't have to add
                        //cNum > 0 might be a cross dependency issue
                    }
                }
            
                
                
            }
            
            
            
        }
        //dangerous
        //taskList.clear();
        return sucList;
    }
    
        
    private void bundleClustering(){
        
        for (Iterator it = mDepth2Task.entrySet().iterator();it.hasNext();){
            Map.Entry pairs = (Map.Entry<Integer, ArrayList>)it.next();
            ArrayList list = (ArrayList)pairs.getValue();
            int num = list.size();
            int avg_a = num / this.clusterNum ;
            int avg_b = avg_a;
            if(avg_a * this.clusterNum < num) {
                avg_b ++;
            }
            
            int mid = num - this.clusterNum * avg_a;
            if (avg_a <=0 )avg_a = 1;
            if (avg_b <= 0)avg_b = 1;
            int start = 0, end = -1;
            for(int i = 0; i < this.clusterNum; i ++){
                start = end + 1;
                if(i < mid){
                    //use avg_b
                    end = start + avg_b - 1;
                }else{
                    //use avg_a
                    end = start + avg_a - 1;
                    
                }
                
                
                if (end >= num)
                    end = num - 1;
                if(end < start)
                    break;
                
                addTasks2Job(searchList(list.subList(start, end + 1)));
            }
            
        }

    }
        
    private void collapseClustering(){
        for (Iterator it = mDepth2Task.entrySet().iterator();it.hasNext();){
            Map.Entry pairs = (Map.Entry<Integer, ArrayList>)it.next();
            ArrayList list = (ArrayList)pairs.getValue();
            int num = list.size();
            int avg = this.clusterSize ;

            int start = 0;
            int end = 0;
            int i = 0;
            do{
                start = i * avg;
                end = start + avg - 1;
                i ++ ;
                if (end >= num) end = num - 1;
                addTasks2Job(searchList(list.subList(start, end + 1)));
            }while(end < num - 1);

        }
    }
    
    
}
