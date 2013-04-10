/**
 *  Copyright 2012-2013 University Of Southern California
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
package org.workflowsim.clusering.balancing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.Task;
import org.workflowsim.clusering.BasicClustering;
import org.workflowsim.clusering.TaskSet;
import org.workflowsim.clusering.balancing.methods.ChildAwareHorizontalClustering;
import org.workflowsim.clusering.balancing.methods.HorizontalDistanceBalancing;
import org.workflowsim.clusering.balancing.methods.HorizontalImpactBalancing;
import org.workflowsim.clusering.balancing.methods.HorizontalRuntimeBalancing;
import org.workflowsim.clusering.balancing.methods.VerticalBalancing;
import org.workflowsim.clusering.balancing.metrics.DistanceVariance;
import org.workflowsim.clusering.balancing.metrics.HorizontalRuntimeVariance;
import org.workflowsim.clusering.balancing.metrics.ImpactFactorVariance;
import org.workflowsim.clusering.balancing.metrics.PipelineRuntimeVariance;
import org.workflowsim.utils.Parameters;

/**
 * BalancedClustering is a clustering method that aims balancing task runtime and 
 * data dependency. All BalancedClustering methods should extend it
 * 
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class BalancedClustering extends BasicClustering{

    
    /** Number of clustered jobs per level. */
    private int clusterNum;

    /** Map from task to taskSet. */
    private Map<Task, TaskSet> mTask2TaskSet;
    
    /** Map from taskSet to its depth. */
    private Map mTaskSet2Depth;
    
    /**
     * Initialize a BalancedClustering method
     * 
     * @param clusterNum clusters.num
     */
    public BalancedClustering(int clusterNum){
        super();
        this.clusterNum = clusterNum;
        this.mTask2TaskSet = new HashMap<Task, TaskSet>();
        mTaskSet2Depth = new HashMap<TaskSet, Integer>();

    }
    
    /**
     * Clean the checked flag of a taskset
     * @return 
     */
    public void cleanTaskSetChecked(){
        Collection sets = mTask2TaskSet.values();
        for(Iterator it = sets.iterator();it.hasNext();){
            TaskSet set = (TaskSet)it.next();
            set.hasChecked = false;
        }
    }
   
    
   
    
    
    
    
    private void addImpact(TaskSet set, double impact){
        /*
         * follow the path from set
         */
        set.setImpactFafctor(set.getImpactFactor() + impact);
        int size = set.getParentList().size();
        if(size > 0){
            double avg = impact / size;
            for(TaskSet parent: set.getParentList()){
                addImpact(parent, avg);
            }
        }
    }
    
    public void printMetrics(){
        Map<Integer,ArrayList<TaskSet>> map = getCurrentTaskSetAtLevels();
        for(TaskSet set: mTask2TaskSet.values()){
            set.setImpactFafctor(0.0);
        }
        
        int maxDepth = 0;
        for(Entry entry: map.entrySet()){
            int depth = (Integer)entry.getKey();
            if(depth > maxDepth){
                maxDepth = depth;
            }
        }
        ArrayList<TaskSet> exits = map.get(maxDepth);
        double avg = 1.0 / exits.size();
        for(TaskSet set: exits){
            //set.setImpactFafctor(avg);
            addImpact(set, avg);
        }

        for(Entry entry: map.entrySet()){
            int depth = (Integer)entry.getKey();
            ArrayList<TaskSet> list = (ArrayList)entry.getValue();
            double hrv = new HorizontalRuntimeVariance().getMetric(list);
                    //getHorizontalRuntimeVariance(list);
            
            //Log.printLine("Depth; " + depth);
            double ifv  = new ImpactFactorVariance().getMetric(list);
                    //getDependencyVariance(list);
            
            double prv = new PipelineRuntimeVariance().getMetric(list);
                    //getPipelineRuntimeVariance(list);
            
            double dv = new DistanceVariance().getMetric(list);
            Log.printLine("HRV " + depth + " " + list.size() 
                    + " " + hrv + "\nIFV " + depth + " "
                    + list.size() +" " + ifv + "\nPRV " +depth
                    + " " + list.size() + " " + prv + "\nDV " + depth+ " " + list.size() + " "+dv );
            
        }
    }
    
    public Map<Integer,ArrayList<TaskSet>> getCurrentTaskSetAtLevels(){
        //makesure it is updated 
        
        //makesure Taskset.hasChecked is false
        Map map = new HashMap<Integer, ArrayList<TaskSet>>();
        Collection sets = mTask2TaskSet.values();
        for(Iterator it = sets.iterator();it.hasNext();){
            TaskSet set = (TaskSet)it.next();
            if(!set.hasChecked){
                set.hasChecked = true;
                int depth = getDepth(set);
                if(!map.containsKey(depth)){
                    map.put(depth, new ArrayList<TaskSet>());
                }
                ArrayList list = (ArrayList)map.get(depth);
                list.add(set);
                
            }
            
        }
        mTaskSet2Depth.clear();
        //must do
        cleanTaskSetChecked();
        return map;
    }
    
    private int getDepth(TaskSet set){
        if(mTaskSet2Depth.containsKey(set)){
            return (Integer)mTaskSet2Depth.get(set);
        }else{
            int depth = 0;
            for(Iterator it = set.getParentList().iterator(); it.hasNext();){
                TaskSet parent = (TaskSet)it.next();
                int curDepth = getDepth(parent);
                if(curDepth > depth)
                {
                    depth = curDepth;
                }
            }
            depth ++;
            mTaskSet2Depth.put(set, depth );
            return depth;
        
        }
        
    }
    private boolean check(Task ancessor, Task set){
        if(ancessor == null || set == null){
            return false;
        }
        if(ancessor == set){
            return true;
        }
        //for(Task parent: set.getParentList()){
        for(Iterator it = set.getParentList().iterator(); it.hasNext();){
            Task parent = (Task)it.next();
            if(check(ancessor, parent)){
                return true;
            }else{
                //parent.hasChecked = true;
            }
        }
        return false;
    }
    private Map<Task, Task> mRecover = new HashMap<Task, Task>();
    private void remove(){

        for(Task set: this.getTaskList()){
                if(set.getChildList().size()>=2){
                    for(int i = 0; i < set.getChildList().size(); i ++){
                        Task children = (Task)set.getChildList().get(i);
                        for(int j = i + 1; j < set.getChildList().size();j++){
                            Task another = (Task)set.getChildList().get(j);
                            //check() takes much time
                            // avoid unnecessary checks
                            if(children.getDepth() >  another.getDepth()){
                                if(check(another, children)){
                                    //remove i
                                    set.getChildList().remove(children);
                                    children.getParentList().remove(set);
                                    i -- ;
                                    mRecover.put(set, children);
                                    //cleanTaskSetChecked();
                                    break;
                                }else{
                                    //cleanTaskSetChecked();
                                }
                            }
                            if(another.getDepth()>children.getDepth()){
                                if(check(children, another)){
                                    set.getChildList().remove(another);
                                    another.getParentList().remove(set);
                                    i -- ;
                                    mRecover.put(set, another);
                                    //cleanTaskSetChecked();
                                    break;
                                }else{
                                    //cleanTaskSetChecked();
                                }
                            }
                        }
                        
                    }
                }
            
        }
    }
    private void recover(){
        for(Iterator it = mRecover.entrySet().iterator();it.hasNext();){
            Entry entry = (Entry)it.next();
            Task set = (Task)entry.getKey();
            Task children = (Task)entry.getValue();
            set.getChildList().add(children);
            children.getParentList().add(set);
        }
    }
    
    @Override
    public void run()
 
    {

        //First step, initialization, (naive clustering)
        
        if(clusterNum >0){
            for(Iterator it = getTaskList().iterator();it.hasNext();)
            {
                Task task = (Task)it.next();
                TaskSet set = new TaskSet();
                set.addTask(task);
                mTask2TaskSet.put(task, set);
                
            }
        }

        remove();
        updateTaskSetDependencies();
        
        printMetrics();
        String code = Parameters.getClusteringParameters().getCode();
        Map<Integer,ArrayList<TaskSet>> map = getCurrentTaskSetAtLevels();
        if(code!=null){
            for(char c:code.toCharArray()){
                
                switch (c){
                    case 'v':

                        //verticalClustering();
                        VerticalBalancing v = new VerticalBalancing(map, this.mTask2TaskSet, this.clusterNum) ;
                        v.run();
                        break;
                    case 'c':
                        //childAwareHorizontalClustering();
                        ChildAwareHorizontalClustering ch = 
                                new ChildAwareHorizontalClustering(map, this.mTask2TaskSet, this.clusterNum);
                        ch.run();
                        updateTaskSetDependencies();
                        break;
                    case 'r':
                        //horizontalRuntimeBalancing();
                        HorizontalRuntimeBalancing r = 
                                new HorizontalRuntimeBalancing(map, this.mTask2TaskSet, this.clusterNum);
                        r.run();
                        updateTaskSetDependencies();
                        break;
                    case 'i':
                        HorizontalImpactBalancing i = 
                                new HorizontalImpactBalancing(map, this.mTask2TaskSet, this.clusterNum);
                        i.run();
                        break;
                    case 'd':
                        HorizontalDistanceBalancing d = 
                                new HorizontalDistanceBalancing(map, this.mTask2TaskSet, this.clusterNum);
                        d.run();
                        break;
                    default:
                        break;
                }
            }
            printMetrics();
        }
        
        
        

        printOut();
        
        

        Collection sets = mTask2TaskSet.values();
        for(Iterator it = sets.iterator();it.hasNext();){
            TaskSet set = (TaskSet)it.next();
            if(!set.hasChecked){
                set.hasChecked = true;
                addTasks2Job(set.getTaskList());
            }
        }
        //a good habit
        cleanTaskSetChecked();
        
        
        updateDependencies();
        addClustDelay();
        
        recover();
    }

    private void printOut(){
        Collection sets = mTask2TaskSet.values();
        for(Iterator it = sets.iterator();it.hasNext();){
            TaskSet set = (TaskSet)it.next();
            if(!set.hasChecked){
                set.hasChecked = true;
            
                Log.printLine("Job");
                for(Task task: set.getTaskList()){
                    Log.printLine("Task " + task.getCloudletId() + " " + task.getImpact() + " "+ task.getCloudletLength());
                }
            }
            
        }
        //within each method
        cleanTaskSetChecked();
    }
    


    private void updateTaskSetDependencies(){
        
        Collection sets = mTask2TaskSet.values();
        for(Iterator it = sets.iterator();it.hasNext();){
            TaskSet set = (TaskSet)it.next();
            if(!set.hasChecked){
                set.hasChecked = true;
                set.getChildList().clear();
                set.getParentList().clear();
                for(Task task: set.getTaskList()){
                    for(Iterator tIt= task.getParentList().iterator(); tIt.hasNext();){
                        Task parent = (Task)tIt.next();
                        TaskSet parentSet = (TaskSet)mTask2TaskSet.get(parent);
                        if(!set.getParentList().contains(parentSet)&&set!=parentSet){
                            set.getParentList().add(parentSet);
                        }
                        
                    }
                    for(Iterator tIt= task.getChildList().iterator(); tIt.hasNext();){
                        Task child = (Task)tIt.next();
                        TaskSet childSet = (TaskSet)mTask2TaskSet.get(child);
                        if(!set.getChildList().contains(childSet)&&set!=childSet){
                            set.getChildList().add(childSet);
                        }
                        
                    }
                }
            }
        }
        //within each method
        cleanTaskSetChecked();
    }
    
}
