/*
 * 
 *   Copyright 2013-2014 University Of Southern California
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
package org.workflowsim.examples;

import org.cloudbus.cloudsim.Log;
import org.workflowsim.examples.clustering.*;
import org.workflowsim.examples.clustering.balancing.*;
import org.workflowsim.examples.failure.*;
import org.workflowsim.examples.failure.clustering.*;
import org.workflowsim.examples.planning.*;
import org.workflowsim.examples.scheduling.*;
import org.workflowsim.examples.cost.*;

/**
 * Test all the workflow examples
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Mar 3, 2014
 */
public class WorkflowSimAllExamplesTester {

    /**
     *
     * Test all the workflow examples and check their results
     */
    public static void main(String[] args) {
        try {
            /**
             * Basic Examples
             */
            WorkflowSimBasicExample1.main(args);
            WorkflowSimCostExample1.main(args);
            WorkflowSimCostExample2.main(args);
            DynamicWorkloadExample1.main(args);
            WorkflowSimMultipleClusterExample1.main(args);

            /*
             * Horizontal Clustering Examples
             */
            HorizontalClusteringExample1.main(args);
            HorizontalClusteringExample2.main(args);
            HorizontalClusteringExample3.main(args);
            VerticalClusteringExample1.main(args);

            /**
             * Balanced Clustering Examples
             */
            BalancedClusteringExample1.main(args);

            /**
             * Fault Tolerant Scheduling Examples
             */
            FaultTolerantSchedulingExample1.main(args);

            /**
             * Fault Tolerant Clustering Examples
             */
            FaultTolerantClusteringExample1.main(args);
            FaultTolerantClusteringExample2.main(args);
            FaultTolerantClusteringExample3.main(args);
            FaultTolerantClusteringExample4.main(args);
            FaultTolerantClusteringExample5.main(args);
            FaultTolerantClusteringExample6.main(args);

            /**
             * Planning Algorithms
             */
            DHEFTPlanningAlgorithmExample1.main(args);
            HEFTPlanningAlgorithmExample1.main(args);

            /**
             * Scheduling Algorithms
             */
            DataAwareSchedulingAlgorithmExample.main(args);
            FCFSSchedulingAlgorithmExample.main(args);
            MAXMINSchedulingAlgorithmExample.main(args);
            MCTSchedulingAlgorithmExample.main(args);
            MINMINSchedulingAlgorithmExample.main(args);
        } catch (Exception e) {
            Log.printLine("ERROR: please check your workflow examples");
            e.printStackTrace();
        }
    }
}
