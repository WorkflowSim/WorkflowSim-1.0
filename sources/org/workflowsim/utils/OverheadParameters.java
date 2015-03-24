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
package org.workflowsim.utils;

import java.util.List;
import java.util.Map;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.Job;

/**
 * This class includes all parameters that involve overheads
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class OverheadParameters {

    /**
     * The interval of workflow engine delay
     */
    private final int WED_INTERVAL;
    /**
     * The bandwidth
     */
    private final double bandwidth;
    /**
     * The list of workflow engine delay key = level value = delay
     */
    private final Map<Integer, DistributionGenerator> WED_DELAY;
    /**
     * The list of queue delay key = level value = delay
     */
    private final Map<Integer, DistributionGenerator> QUEUE_DELAY;
    /**
     * The list of postscript delay key = level value = delay
     */
    private final Map<Integer, DistributionGenerator> POST_DELAY;
    /**
     * The list of clustering delay key = level value = delay
     */
    private final Map<Integer, DistributionGenerator> CLUST_DELAY;

    /**
     * Created a new OverheadParameters object.
     *
     * @param wed_interval, the interval of workflow engine
     * @param wed_delay, the list of workflow engine delay
     * @param queue_delay, the list of queue delay
     * @param post_delay, the list of postscript delay
     * @param cluster_delay, the list of clustering delay
     * @param bandwidth, the bandwidth
     * @pre $none
     * @post $none
     */
    public OverheadParameters(int wed_interval,
            Map<Integer, DistributionGenerator> wed_delay,
            Map<Integer, DistributionGenerator> queue_delay,
            Map<Integer, DistributionGenerator> post_delay,
            Map<Integer, DistributionGenerator> cluster_delay,
            double bandwidth) {
        this.WED_INTERVAL = wed_interval;
        this.WED_DELAY = wed_delay;
        this.QUEUE_DELAY = queue_delay;
        this.POST_DELAY = post_delay;
        this.CLUST_DELAY = cluster_delay;
        this.bandwidth = bandwidth;

    }

    /**
     * Gets the bandwidth
     *
     * @return the bandwidth
     * @pre $none
     * @post $none
     */
    public double getBandwidth() {
        return this.bandwidth;
    }

    /**
     * Gets the interval
     *
     * @return the interval
     * @pre $none
     * @post $none
     */
    public int getWEDInterval() {
        return this.WED_INTERVAL;
    }

    /**
     * Gets the queue delay
     *
     * @return the queue delay
     * @pre $none
     * @post $none
     */
    public Map<Integer, DistributionGenerator> getQueueDelay() {
        return this.QUEUE_DELAY;
    }

    /**
     * Gets the postscript delay
     *
     * @return the postscript delay
     * @pre $none
     * @post $none
     */
    public Map<Integer, DistributionGenerator> getPostDelay() {
        return this.POST_DELAY;
    }

    /**
     * Gets the workflow engine delay
     *
     * @return the workflow engine delay
     * @pre $none
     * @post $none
     */
    public Map<Integer, DistributionGenerator> getWEDDelay() {
        return this.WED_DELAY;
    }

    /**
     * Gets the clustering delay
     *
     * @return the clustering delay
     * @pre $none
     * @post $none
     */
    public Map<Integer, DistributionGenerator> getClustDelay() {
        return this.CLUST_DELAY;
    }

    /**
     * Gets the clustering delay for a particular job based on the depth(level)
     *
     * @param cl, the job
     * @return the clustering delay
     * @pre $none
     * @post $none
     */
    public double getClustDelay(Cloudlet cl) {
        double delay = 0.0;
        if(this.CLUST_DELAY == null){
            return delay;
        }
        if (cl != null) {
            Job job = (Job) cl;

            if (this.CLUST_DELAY.containsKey(job.getDepth())) {
                delay = this.CLUST_DELAY.get(job.getDepth()).getNextSample();
            } else if (this.CLUST_DELAY.containsKey(0)) {
                delay = this.CLUST_DELAY.get(0).getNextSample();
            } else {
                delay = 0.0;
            }


        } else {
            Log.printLine("Not yet supported");
        }
        return delay;
    }

    /**
     * Gets the queue delay for a particular job based on the depth(level)
     *
     * @param cl, the job
     * @return the queue delay
     * @pre $none
     * @post $none
     */
    public double getQueueDelay(Cloudlet cl) {
        double delay = 0.0;

        if(this.QUEUE_DELAY == null){
            return delay;
        }
        if (cl != null) {
            Job job = (Job) cl;

            if (this.QUEUE_DELAY.containsKey(job.getDepth())) {
                delay = this.QUEUE_DELAY.get(job.getDepth()).getNextSample();
            } else if (this.QUEUE_DELAY.containsKey(0)) {
                delay = this.QUEUE_DELAY.get(0).getNextSample();
            } else {
                delay = 0.0;
            }


        } else {
            Log.printLine("Not yet supported");
        }
        return delay;
    }

    /**
     * Gets the postscript delay for a particular job based on the depth(level)
     *
     * @param job
     * @return the postscript delay
     * @pre $none
     * @post $none
     */
    public double getPostDelay(Job job) {
        double delay = 0.0;

        if(this.POST_DELAY == null){
            return delay;
        }
        if (job != null) {

            if (this.POST_DELAY.containsKey(job.getDepth())) {
                delay = this.POST_DELAY.get(job.getDepth()).getNextSample();
            } else if (this.POST_DELAY.containsKey(0)) {
                //the default one
                delay = this.POST_DELAY.get(0).getNextSample();
            } else {
                delay = 0.0;
            }

        } else {
            Log.printLine("Not yet supported");
        }
        return delay;
    }

    /**
     * Gets the workflow engine delay for a particular job based on the
     * depth(level)
     *
     * @param list
     * @return the workflow engine delay
     * @pre $none
     * @post $none
     */
    public double getWEDDelay(List list) {
        double delay = 0.0;

        if(this.WED_DELAY == null){
            return delay;
        }
        if (!list.isEmpty()) {
            Job job = (Job) list.get(0);
            if (this.WED_DELAY.containsKey(job.getDepth())) {
                delay = this.WED_DELAY.get(job.getDepth()).getNextSample();
            } else if (this.WED_DELAY.containsKey(0)) {
                delay = this.WED_DELAY.get(0).getNextSample();
            } else {
                delay = 0.0;
            }

        } else {
            //actuall set it to be 0.0;
            //Log.printLine("Not yet supported");
        }
        return delay;
    }
}
