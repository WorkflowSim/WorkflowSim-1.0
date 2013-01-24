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
package org.workflowsim.utils;

import org.workflowsim.Job;
import java.util.List;
import java.util.Map;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;

/**
 *
 * @author Weiwei Chen
 */
public class OverheadParameters {
    
    
    private  int WED_INTERVAL;
    
    private double bandwidth;
    
    private double random;
    
    private double random1;
    
    private Map<Integer, Double> WED_DELAY;
    
    private Map<Integer, Double> QUEUE_DELAY;
    
    private Map<Integer, Double> POST_DELAY;

    private Map<Integer, Double> CLUST_DELAY;
    
    public OverheadParameters(int wed_interval, 
            Map<Integer, Double> wed_delay, 
            Map<Integer, Double> queue_delay, 
            Map<Integer, Double> post_delay,
            Map<Integer, Double> cluster_delay, 
            double bandwidth, double random, 
            double random1){
        this.WED_INTERVAL   = wed_interval;
        this.WED_DELAY      = wed_delay;
        this.QUEUE_DELAY    = queue_delay;
        this.POST_DELAY     = post_delay;
        this.CLUST_DELAY    = cluster_delay;
        this.bandwidth      = bandwidth;
        this.random         = random;
        this.random1        = random1;
    }
    
    public double getRandom(){
        return this.random;
    }
    
    public double getRandom1(){
        return this.random1;
    }
    public double getBandwidth(){
        return this.bandwidth;
    }
    
    public int getWEDInterval(){
        return this.WED_INTERVAL;
    }

    public Map<Integer, Double> getQueueDelay(){
        return this.QUEUE_DELAY;
    }
    public Map<Integer, Double> getPostDelay(){
        return this.POST_DELAY;
    }
    public Map<Integer, Double> getWEDDelay(){
        return this.WED_DELAY;
    }
    public Map<Integer, Double> getClustDelay(){
        return this.CLUST_DELAY;
    }
    
    public double getClustDelay(Cloudlet cl)
    {
        double delay = 0.0;

        if(cl!=null){
            Job job = (Job)cl;

            if(this.CLUST_DELAY.containsKey(job.getDepth())){
                delay = (Double)this.CLUST_DELAY.get(job.getDepth());
            }else if(this.CLUST_DELAY.containsKey(0)){
                delay = (Double)this.CLUST_DELAY.get(0);
            }else{
                delay = 0.0;
            }


        }else
        {
            Log.printLine("Not yet supported");
        }
        return delay;
    }  
    
    public double getQueueDelay(Cloudlet cl)
    {
        double delay = 0.0;

        if(cl!=null){
            Job job = (Job)cl;

            if(this.QUEUE_DELAY.containsKey(job.getDepth())){
                delay = (Double)this.QUEUE_DELAY.get(job.getDepth());
            }else if(this.QUEUE_DELAY.containsKey(0)){
                delay = (Double)this.QUEUE_DELAY.get(0);
            }else{
                delay = 0.0;
            }


        }else
        {
            Log.printLine("Not yet supported");
        }
        return delay;
    }        
    public double getPostDelay(Job job)
    {
        double delay = 0.0;

        if(job!=null){

            if(this.POST_DELAY.containsKey(job.getDepth())){
                delay = (Double)this.POST_DELAY.get(job.getDepth());
            }
            else if(this.POST_DELAY.containsKey(0)) {
                //the default one
                delay = (Double)this.POST_DELAY.get(0);
            }else{
                delay = 0.0;
            }

        }else
        {
            Log.printLine("Not yet supported");
        }
        return delay;
    }       
    public double getWEDDelay(List list)
        {
            double delay = 0.0;
  
            if(!list.isEmpty()){
                Job job = (Job)list.get(0);
                if(this.WED_DELAY.containsKey(job.getDepth())){
                    delay = (Double)this.WED_DELAY.get(job.getDepth());
                }
                else if(this.WED_DELAY.containsKey(0)) {
                    delay = (Double)this.WED_DELAY.get(0);
                }else{
                    delay = 0.0;
                }

            }else
            {
                //actuall set it to be 0.0;
                //Log.printLine("Not yet supported");
            }
            return delay;
        }
}
