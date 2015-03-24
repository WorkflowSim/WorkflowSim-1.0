/*
 * 
 *  Copyright 2012-2013 University Of Southern California
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

import java.util.List;
import org.workflowsim.clustering.TaskSet;

/**
 * HorizontalRuntimeVariance is the standard deviation of the runtime
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class HorizontalRuntimeVariance implements BalancingMetric {

    /**
     * Returns the standard deviation of runtime
     * @param list taskSets to be checked
     * @return the standard deviation
     */
    @Override
    public double getMetric(List<TaskSet> list) {
        if (list == null || list.size() <= 1) {
            return 0.0;
        }
        long sum = 0;
        for (TaskSet task : list) {
            sum += task.getJobRuntime();
        }
        long mean = sum / list.size();
        sum = 0;
        for (TaskSet task : list) {
            long var = task.getJobRuntime();
            sum += Math.pow((double) (var - mean), 2);
        }
        return Math.sqrt((double) (sum / list.size())) / mean;
    }
}
