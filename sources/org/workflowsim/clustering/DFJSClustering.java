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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.workflowsim.Task;

/**
 * DFJSClustering is based on Muthuvelu,'s 2005 paper:
 * A Dynamic Job Grouping-Based Scheduling for Deploying Applications with Fine-Grained Tasks on Global Grids
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Dec 31, 2013
 */
public class DFJSClustering extends BasicClustering {


    /**
     * The granularity size used in Muthuvelu's paper
     */
    private double granularity;
    /**
     * The map from depth to tasks at that depth.
     */
    private Map mDepth2Task;

    /**
     * Initialize a DFJSClustering with granularity
     * @param granularity 
     */
    public DFJSClustering(double granularity) {
        super();
        this.granularity = granularity;
        this.mDepth2Task = new HashMap<Integer, Map>();

    }

    /**
     * The main function
     */
    @Override
    public void run() {
        
        for (Iterator it = getTaskList().iterator(); it.hasNext();) {
            Task task = (Task) it.next();
            int depth = task.getDepth();
            if (!mDepth2Task.containsKey(depth)) {
                mDepth2Task.put(depth, new ArrayList<Task>());
            }
            ArrayList list = (ArrayList) mDepth2Task.get(depth);
            if (!list.contains(task)) {
                list.add(task);
            }

        }
        
        /**
         * if granularity is set.
         */
        if (this.granularity > 0) {
            doDFJSClustering();
        }

        updateDependencies();
        addClustDelay();
    }

    /**
     * Merges tasks based on resource capacity (granularity)
     */
    private void doDFJSClustering() {

        for (Iterator it = mDepth2Task.entrySet().iterator(); it.hasNext();) {
            Map.Entry pairs = (Map.Entry<Integer, ArrayList>) it.next();
            ArrayList list = (ArrayList) pairs.getValue();

            long seed = System.nanoTime();
            Collections.shuffle(list, new Random(seed));
            seed = System.nanoTime();
            Collections.shuffle(list, new Random(seed));
            int num = list.size();
            
            List taskList = new ArrayList();
            double sum = 0.0;
            for(int i = 0; i < num; i ++){
                Task task = (Task)list.get(i);
                double runtime = task.getCloudletLength() / 1000;
                if(sum + runtime > this.granularity ){
                    if(taskList.isEmpty()){
                        taskList.add(task);
                        addTasks2Job(taskList);
                        taskList = new ArrayList();
                        sum = 0.0;
                    }else{
                        addTasks2Job(taskList);
                        taskList = new ArrayList();
                        sum = 0.0;
                        taskList.add(task);
                    }
                }else{
                    taskList.add(task);
                    sum += runtime;
                }  
            }
            if(!taskList.isEmpty()){
                addTasks2Job(taskList);
            }
        }
    }

}
