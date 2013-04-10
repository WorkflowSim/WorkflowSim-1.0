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
package org.workflowsim.clustering.balancing.metrics;

import org.workflowsim.clustering.balancing.metrics.BalancingMetric;
import org.workflowsim.clustering.TaskSet;
import java.util.ArrayList;

/**
 *
 * @author Weiwei Chen
 */
public class ImpactFactorVariance implements BalancingMetric{
    @Override
    public double getMetric(ArrayList<TaskSet> list){
         if(list == null || list.size() <= 1){
            return 0.0;
        }
        double sum = 0;
        for(TaskSet task: list){
            sum += task.getImpactFactor();
            
        }
        double mean = sum / list.size();
        //Log.printLine("sum: " + sum );
        sum = 0.0;
        for(TaskSet task: list){
            double var = task.getImpactFactor();
            sum += Math.pow(var-mean, 2);
        }
        return Math.sqrt(sum/list.size());
    }
    
}
