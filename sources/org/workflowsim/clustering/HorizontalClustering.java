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
import org.workflowsim.Job;
import org.workflowsim.Task;

/**
 * HorizontalClustering merges task at the same horizontal level
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class HorizontalClustering extends BasicClustering {

    /**
     * The number of clustered jobs per level.
     */
    private final int clusterNum;
    /**
     * The number of tassk in a job.
     */
    private final int clusterSize;
    /**
     * The map from depth to tasks at that depth.
     */
    private final Map<Integer, List> mDepth2Task;

    /**
     * Initialize a HorizontalClustering Either clusterNum or clusterSize should
     * be set
     *
     * @param clusterNum clusters.num
     * @param clusterSize clusters.size
     */
    public HorizontalClustering(int clusterNum, int clusterSize) {
        super();
        this.clusterNum = clusterNum;
        this.clusterSize = clusterSize;
        this.mDepth2Task = new HashMap<>();

    }

    /**
     * The main function
     */
    @Override
    public void run() {
        if (clusterNum > 0 || clusterSize > 0) {
            for (Iterator it = getTaskList().iterator(); it.hasNext();) {
                Task task = (Task) it.next();
                int depth = task.getDepth();
                if (!mDepth2Task.containsKey(depth)) {
                    mDepth2Task.put(depth, new ArrayList<>());
                }
                List list = mDepth2Task.get(depth);
                if (!list.contains(task)) {
                    list.add(task);
                }

            }
        }
        /**
         * if clusters.num is set.
         */
        if (clusterNum > 0) {
            bundleClustering();
            /**
             * else if clusters.size is set.
             */
        } else if (clusterSize > 0) {
            collapseClustering();
        }

        updateDependencies();
        addClustDelay();
    }

    /**
     * Merges tasks into a fixed number of jobs.
     */
    private void bundleClustering() {

        for (Map.Entry<Integer, List> pairs : mDepth2Task.entrySet()) {
            List list = pairs.getValue();

            long seed = System.nanoTime();
            Collections.shuffle(list, new Random(seed));
            seed = System.nanoTime();
            Collections.shuffle(list, new Random(seed));

            int num = list.size();
            int avg_a = num / this.clusterNum;
            int avg_b = avg_a;
            if (avg_a * this.clusterNum < num) {
                avg_b++;
            }

            int mid = num - this.clusterNum * avg_a;
            if (avg_a <= 0) {
                avg_a = 1;
            }
            if (avg_b <= 0) {
                avg_b = 1;
            }
            int start = 0, end = -1;
            for (int i = 0; i < this.clusterNum; i++) {
                start = end + 1;
                if (i < mid) {
                    //use avg_b
                    end = start + avg_b - 1;
                } else {
                    //use avg_a
                    end = start + avg_a - 1;

                }


                if (end >= num) {
                    end = num - 1;
                }
                if (end < start) {
                    break;
                }
                addTasks2Job(list.subList(start, end + 1));
            }


        }
    }

    /**
     * Merges a fixed number of tasks into a job
     */
    private void collapseClustering() {
        for (Map.Entry<Integer, List> pairs : mDepth2Task.entrySet()) {
            List list = pairs.getValue();

            long seed = System.nanoTime();
            Collections.shuffle(list, new Random(seed));
            seed = System.nanoTime();
            Collections.shuffle(list, new Random(seed));

            int num = list.size();
            int avg = this.clusterSize;

            int start = 0;
            int end = 0;
            int i = 0;
            do {
                start = i * avg;
                end = start + avg - 1;
                i++;
                if (end >= num) {
                    end = num - 1;
                }
                Job job = addTasks2Job(list.subList(start, end + 1));
            } while (end < num - 1);

        }
    }
}
