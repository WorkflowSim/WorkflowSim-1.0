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
package org.workflowsim.examples.failure.clustering;

import java.io.File;
import java.text.DecimalFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.workflowsim.CondorVM;
import org.workflowsim.WorkflowDatacenter;
import org.workflowsim.Job;
import org.workflowsim.WorkflowEngine;
import org.workflowsim.WorkflowPlanner;
import org.workflowsim.failure.FailureGenerator;
import org.workflowsim.failure.FailureMonitor;
import org.workflowsim.failure.FailureParameters;
import org.workflowsim.utils.DistributionGenerator;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.PeriodicalDistributionGenerator;
import org.workflowsim.utils.PeriodicalSignal;
import org.workflowsim.utils.ReplicaCatalog;

/**
 * This FaultTolerantClusteringExample3 uses Dynamic Reclustering to address the
 * fault tolerance problem in task clustering
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Dec 31, 2013
 */
public class FaultTolerantClusteringExample7 extends FaultTolerantClusteringExample1 {

    ////////////////////////// STATIC METHODS ///////////////////////
    /**
     * Creates main() to run this example This example has only one datacenter
     * and one storage
     */
    public static void main(String[] args) {

        try {
            // First step: Initialize the WorkflowSim package. 

            /**
             * However, the exact number of vms may not necessarily be vmNum If
             * the data center or the host doesn't have sufficient resources the
             * exact vmNum would be smaller than that. Take care.
             */
            int vmNum = 20;//number of vms;
            /**
             * Should change this based on real physical path
             */
            //String daxPath = "/Users/weiweich/NetBeansProjects/WorkflowSim-1.0/config/dax/Montage_1000.xml";
            //String daxPath = "/Users/chenweiwei/Research/balanced_clustering/data/scan/SIPHT.n.1000.9.dax";
            //String daxPath = "/Users/chenweiwei/Research/balanced_clustering/data/scan-1/LIGO.n.800.8.dax";
            //String daxPath ="/Users/chenweiwei/Research/balanced_clustering/data/scan-1/GENOME.d.702049732.5.dax";
            String daxPath = "/Users/chenweiwei/Research/balanced_clustering/data/scan-1/CYBERSHAKE.n.700.10.dax";
            //String daxPath = "/Users/chenweiwei/Research/balanced_clustering/generator/BharathiPaper/Montage_300.xml";
            //This controls k if q_shape is large it is good
            double q_scale = 50, q_weight = 3, q_shape = 3;
            double t_scale = 10;//default is 1.0
            String clustering = "DR";
            double theta = 500, theta_weight = 30 * 0.1;
            double period = 10000;
            double upperbound = 500;
            double lowerbound = 50;
            double portion = 0.1;//default is 0.5

            for (int i = 0; i < args.length; i++) {
                char key = args[i].charAt(1);
                switch (key) {
                    case 'c':
                        clustering = args[++i];
                        break;
                    case 'd':
                        daxPath = args[++i];
                        break;
                    case 'b':
                        t_scale = Double.parseDouble(args[++i]);
                        break;
                    case 'w':
                        q_weight = Double.parseDouble(args[++i]);
                        break;
                    case 'q':
                        q_scale = Double.parseDouble(args[++i]);
                        break;
                    case 's':
                        q_shape = Double.parseDouble(args[++i]);
                        break;
                    case 'p':
                        theta = Double.parseDouble(args[++i]);
                        break;
                    case 't':
                        theta_weight = Double.parseDouble(args[++i]);
                        break;
                    case 'e':
                        period = Double.parseDouble(args[++i]);
                        break;
                    case 'n':
                        portion = Double.parseDouble(args[++i]);
                        break;
                }
            }
            t_scale /= 10;
            if (daxPath == null) {
                Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
                return;
            }
            File daxFile = new File(daxPath);
            if (!daxFile.exists()) {
                Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
                return;
            }
            /**
             * Runtime Parameters
             */
            Parameters.setRuntimeScale(t_scale);

            /*
             *  Fault Tolerant Parameters
             */
            /**
             * MONITOR_JOB classifies failures based on the level of jobs;
             * MONITOR_VM classifies failures based on the vm id; MOINTOR_ALL
             * does not do any classification; MONITOR_NONE does not record any
             * failiure.
             */
            FailureParameters.FTCMonitor ftc_monitor = FailureParameters.FTCMonitor.MONITOR_VM_JOB;
            /**
             * Similar to FTCMonitor, FTCFailure controls the way how we
             * generate failures.
             */
            FailureParameters.FTCFailure ftc_failure = FailureParameters.FTCFailure.FAILURE_VM_JOB;
            /**
             * In this example, we have horizontal clustering and we use Dynamic
             * Reclustering.
             */
            PeriodicalSignal signal = new PeriodicalSignal(period, upperbound, lowerbound, portion);

            /**
             * Clustering Parameters
             */
            ClusteringParameters.ClusteringMethod method = ClusteringParameters.ClusteringMethod.HORIZONTAL;

            FailureParameters.FTCluteringAlgorithm ftc_method = FailureParameters.FTCluteringAlgorithm.FTCLUSTERING_NOOP;
            switch (clustering) {
                case "SR":
                    ftc_method = FailureParameters.FTCluteringAlgorithm.FTCLUSTERING_SR;
                    break;
                case "DR":
                    ftc_method = FailureParameters.FTCluteringAlgorithm.FTCLUSTERING_DR;
                    break;
                case "NOOP":
                    ftc_method = FailureParameters.FTCluteringAlgorithm.FTCLUSTERING_NOOP;
                    break;
                case "DC":
                    ftc_method = FailureParameters.FTCluteringAlgorithm.FTCLUSTERING_DC;
                    break;
                case "VR":
                    ftc_method = FailureParameters.FTCluteringAlgorithm.FTCLUSTERING_VERTICAL;
                    method = ClusteringParameters.ClusteringMethod.VERTICAL;
                    break;
            }
            ClusteringParameters cp = new ClusteringParameters(vmNum, 0, method, null);

            /**
             * Task failure rate for each level
             *
             */
            int maxLevel = 11; //most workflows we use has a maximum of 11 levels

            PeriodicalDistributionGenerator[][] failureGenerators = new PeriodicalDistributionGenerator[vmNum][maxLevel];
            //Don't make it smaller than 10 seconds, it has too many failures
            PeriodicalDistributionGenerator generator = new PeriodicalDistributionGenerator(DistributionGenerator.DistributionFamily.WEIBULL,
                    theta, 0.78, theta_weight, theta_weight * theta, 0.78, signal);

            for (int level = 0; level < maxLevel; level++) {
                /*
                 * 
                 * For simplicity, set the task failure rate of each level to be 0.1. Which means 10%
                 * of submitted tasks will fail. It doesn't have to be the same task 
                 * failure rate at each level. 
                 */
                for (int vmId = 0; vmId < vmNum; vmId++) {
                    failureGenerators[vmId][level] = generator;
                }
            }

            /**
             * Since we are using MINMIN scheduling algorithm, the planning
             * algorithm should be INVALID such that the planner would not
             * override the result of the scheduler
             */
            Parameters.SchedulingAlgorithm sch_method = Parameters.SchedulingAlgorithm.MINMIN;
            Parameters.PlanningAlgorithm pln_method = Parameters.PlanningAlgorithm.INVALID;
            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED;

            /**
             * overheads
             */
            Map<Integer, DistributionGenerator> clusteringDelay = new HashMap();
            Map<Integer, DistributionGenerator> queueDelay = new HashMap();
            Map<Integer, DistributionGenerator> postscriptDelay = new HashMap();
            Map<Integer, DistributionGenerator> engineDelay = new HashMap();
            /**
             * application has at most 11 horizontal levels
             */
            DistributionGenerator queue_delay = new DistributionGenerator(
                    DistributionGenerator.DistributionFamily.GAMMA, q_scale, q_shape,
                    q_weight, q_weight * q_scale, q_shape);
            for (int level = 0; level < maxLevel; level++) {
                queueDelay.put(level, queue_delay);
            }
            OverheadParameters op = new OverheadParameters(0, engineDelay, queueDelay, postscriptDelay, clusteringDelay, 0);

            /**
             * Initialize static parameters
             */
            FailureParameters.init(ftc_method, ftc_monitor, ftc_failure, failureGenerators);
            Parameters.init(vmNum, daxPath, null,
                    null, op, cp, sch_method, pln_method,
                    null, 0);
            ReplicaCatalog.init(file_system);

            FailureMonitor.init();
            FailureGenerator.init();

            // before creating any entities.
            int num_user = 1;   // number of grid users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;  // mean trace events

            // Initialize the CloudSim library
            CloudSim.init(num_user, calendar, trace_flag);

            WorkflowDatacenter datacenter0 = createDatacenter("Datacenter_0");

            /**
             * Create a WorkflowPlanner with one schedulers.
             */
            WorkflowPlanner wfPlanner = new WorkflowPlanner("planner_0", 1);
            /**
             * Create a WorkflowEngine.
             */
            WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();
            /**
             * Create a list of VMs.The userId of a vm is basically the id of
             * the scheduler that controls this vm.
             */
            List<CondorVM> vmlist0 = createVM(wfEngine.getSchedulerId(0), Parameters.getVmNum());

            /**
             * Submits this list of vms to this WorkflowEngine.
             */
            wfEngine.submitVmList(vmlist0, 0);

            /**
             * Binds the data centers with the scheduler.
             */
            wfEngine.bindSchedulerDatacenter(datacenter0.getId(), 0);

            CloudSim.startSimulation();
            List<Job> outputList0 = wfEngine.getJobsReceivedList();
            CloudSim.stopSimulation();
            printJobList2(outputList0);
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("The simulation has been terminated due to an unexpected error");
        }
    }

    /**
     * Prints the job objects
     *
     * @param list list of jobs
     */
    protected static double printJobList2(List<Job> list) {
        int size = list.size();
        double makespan = 0;
        Job job;

        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Cloudlet ID" + indent + "STATUS" + indent
                + "Data center ID" + indent + "VM ID" + indent + indent + "Time" + indent + "Start Time" + indent + "Finish Time" + indent + "Depth");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (int i = 0; i < size; i++) {
            job = list.get(i);
            Log.print(indent + job.getCloudletId() + indent + indent);

            if (job.getFinishTime() > makespan) {
                makespan = job.getFinishTime();
            }

            if (job.getCloudletStatus() == Cloudlet.SUCCESS) {
                Log.print("SUCCESS");
                Log.printLine(indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                        + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
            } else if (job.getCloudletStatus() == Cloudlet.FAILED) {
                Log.print("FAILED");
                Log.printLine(indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                        + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
            }
        }
        return makespan;
    }
}
