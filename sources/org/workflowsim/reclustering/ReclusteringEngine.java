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
package org.workflowsim.reclustering;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.Job;
import org.workflowsim.Task;
import org.workflowsim.failure.FailureMonitor;
import org.workflowsim.failure.FailureParameters;
import org.workflowsim.failure.FailureRecord;
import org.workflowsim.utils.Parameters;

/**
 *
 * A ReclusteringEngine creates a new clustered job after a job fails
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 *
 */
public class ReclusteringEngine {

    /**
     * Create a new job
     *
     * @param id, the job id
     * @param job, the failed job
     * @param length, the length of this job
     * @param taskList, the task list
     * @return a new job
     */
    private static Job createJob(int id, Job job, long length, List taskList, boolean updateDep) {
        try {
            Job newJob = new Job(id, length);
            newJob.setUserId(job.getUserId());
            newJob.setVmId(-1);
            newJob.setCloudletStatus(Cloudlet.CREATED);

            newJob.setTaskList(taskList);
            newJob.setDepth(job.getDepth());
            if (updateDep) {
                newJob.setChildList(job.getChildList());
                newJob.setParentList(job.getParentList());
                for (Iterator it = job.getChildList().iterator(); it.hasNext();) {
                    Job cJob = (Job) it.next();
                    cJob.addParent(newJob);
                }
            }
            return newJob;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;

    }

    /**
     * Process job recreation based on different reclustering algorithm
     *
     * @param job, job
     * @param id, job id
     * @return a list of new jobs
     */
    public static List<Job> process(Job job, int id) {        
        List jobList = new ArrayList();

        try {

            switch (FailureParameters.getFTCluteringAlgorithm()) {
                case FTCLUSTERING_NOOP:

                    jobList.add(createJob(id, job, job.getCloudletLength(), job.getTaskList(), true));
                    //job submttted doesn't have to be considered
                    break;
                /**
                 * Dynamic clustering.
                 */
                case FTCLUSTERING_DC:
                    jobList = DCReclustering(jobList, job, id, job.getTaskList());
                    break;
                /**
                 * Selective reclustering.
                 */
                case FTCLUSTERING_SR:
                    jobList = SRReclustering(jobList, job, id);

                    break;
                /**
                 * Dynamic reclustering.
                 */
                case FTCLUSTERING_DR:
                    jobList = DRReclustering(jobList, job, id, job.getTaskList());
                    break;
                /**
                 * Block reclustering.
                 */
                case FTCLUSTERING_BLOCK:
                    jobList = BlockReclustering(jobList, job, id);
                    break;
                /**
                 * Binary reclustering.
                 */
                case FTCLUSTERING_VERTICAL:
                    jobList = VerticalReclustering(jobList, job, id);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobList;
    }

    /**
     * Partition the list of tasks based on their depth
     *
     * @param list, the list to process
     * @return a map with key equals to depth
     */
    private static Map<Integer, List<Task>> getDepthMap(List<Task> list) {
        Map<Integer, List<Task>> map = new HashMap<>();

        for (Task task : list) {
            int depth = task.getDepth();
            if (!map.containsKey(depth)) {
                map.put(depth, new ArrayList<>());
            }
            List<Task> dl = map.get(depth);
            if (!dl.contains(task)) {
                dl.add(task);
            }
        }

        return map;
    }
    //DR + level aware

    /**
     * Used in DR and level aware
     *
     * @param map
     * @return the minimum key in Map
     */
    private static int getMin(Map<Integer, List<Task>> map) {
        if (map != null && !map.isEmpty()) {
            int min = Integer.MAX_VALUE;
            for (int value : map.keySet()) {
                if (value < min) {
                    min = value;
                }
            }
            return min;
        }
        return -1;
    }

    /**
     * Check whether this list has failed task
     *
     * @param list
     * @return boolean whether this list has failed task
     */
    private static boolean checkFailed(List<Task> list) {
        boolean all = false;
        for (Task task : list) {
            if (task.getCloudletStatus() == Cloudlet.FAILED) {
                all = true;
                break;
            }
        }
        return all;
    }

    /**
     * Vertical Reclustering
     *
     * @param jobList, job list
     * @param job, job
     * @param id, job id
     * @return
     */
    private static List<Job> VerticalReclustering(List<Job> jobList, Job job, int id) {
        Map<Integer, List<Task>> map = getDepthMap(job.getTaskList());
        
        /**
         * If it has just one level
         */
        if (map.size() == 1) {

            jobList = DCReclustering(jobList, job, id, job.getTaskList());

            return jobList;
        }
        int min = getMin(map);
        int max = min + map.size() - 1;
        int mid = (min + max) / 2;
        List listUp = new ArrayList<>();
        List listDown = new ArrayList<>();
        for (int i = min; i < min + map.size(); i++) {
            List<Task> list = map.get(i);
            if (i <= mid) {
                listUp.addAll(list);
            } else {
                listDown.addAll(list);
            }
        }
        List<Job> newUpList = DCReclustering(new ArrayList(), job, id, listUp);
        id += newUpList.size();
        jobList.addAll(newUpList);
        jobList.addAll(DCReclustering(new ArrayList(), job, id, listDown));
        return jobList;

    }

    /**
     * Block Reclustering
     *
     * @param jobList, job list
     * @param job, job
     * @param id, job id
     * @return
     */
    private static List BlockReclustering(List jobList, Job job, int id) {
        Map map = getDepthMap(job.getTaskList());
        if (map.size() == 1) {
            jobList = DRReclustering(jobList, job, id, job.getTaskList());
            return jobList;
        }
        //do that to every list
        int min = getMin(map);
        for (int i = min; i < min + map.size(); i++) {

            List list = (List) map.get(i);

            if (checkFailed(list)) {
                //should be separate
                List newList = DRReclustering(new ArrayList(), job, id, list);
                id = newList.size() + id;//?
                jobList.addAll(newList);
            } else {
                //do nothing
            }
        }


        return jobList;
    }

    /**
     * Dynamic Reclustering
     *
     * @param jobList, job list
     * @param job, job
     * @param id, job id
     * @param allTaskList, all the task List
     * @return
     */
    private static List<Job> DCReclustering(List<Job> jobList, Job job, int id, List<Task> allTaskList) {

        Task firstTask = allTaskList.get(0);
        //Definition of FailureRecord(long length, int tasks, int depth, int all, int vm, int job, int workflow)
        /**
         * @TODO do not know why it is here
         */
        double taskLength = (double) firstTask.getCloudletLength() / 1000 + Parameters.getOverheadParams().getClustDelay(job) / getDividend(job.getDepth());
        FailureRecord record = new FailureRecord(taskLength, 0, job.getDepth(), allTaskList.size(), 0, 0, job.getUserId());
        record.delayLength = getCumulativeDelay(job.getDepth());
        int suggestedK = FailureMonitor.getClusteringFactor(record);

        if (suggestedK == 0) {
            //not really k=0, just too big
            jobList.add(createJob(id, job, job.getCloudletLength(), allTaskList, true));
        } else {

            int actualK = 0;
            List<Task> taskList = new ArrayList<>();
            List<Job> retryJobs = new ArrayList<>();
            long length = 0;
            Job newJob = createJob(id, job, 0, null, false);
            for (Task allTaskList1 : allTaskList) {
                Task task = (Task) allTaskList1;
                if (actualK < suggestedK) {
                    actualK++;
                    taskList.add(task);
                    length += task.getCloudletLength();
                } else {
                    newJob.setTaskList(taskList);
                    taskList = new ArrayList<>();
                    newJob.setCloudletLength(length);
                    length = 0;
                    retryJobs.add(newJob);
                    id++;
                    newJob = createJob(id, job, 0, null, false);
                    actualK = 0;//really a f*k bug
                }
            }
            
            if (!taskList.isEmpty()) {
                newJob.setTaskList(taskList);
                newJob.setCloudletLength(length);
                retryJobs.add(newJob);
            }
            updateDependencies (job, retryJobs);
            jobList.addAll(retryJobs);

        }
        return jobList;

    }

    /**
     * Selective Reclustering
     *
     * @param jobList, job list
     * @param job, job
     * @param id, job id
     * @return
     */
    private static List<Job> SRReclustering(List<Job> jobList, Job job, int id) {
        List<Task> newTaskList = new ArrayList<>();
        long length = 0;
        for (Task task : job.getTaskList()) {
            if (task.getCloudletStatus() == Cloudlet.FAILED) {
                newTaskList.add(task);
                length += task.getCloudletLength();
            } else {
            }
        }
        jobList.add(createJob(id, job, length, newTaskList, true));
        return jobList;
    }

    /**
     * Get the dividend
     *
     * @param depth
     * @return
     */
    private static int getDividend(int depth) {
        int dividend = 1;
        switch (depth) {
            case 1:
                dividend = 78;
                break;
            case 2:
                dividend = 229;
                break;
            case 5:
                dividend = 64;
                break;
            default:
                Log.printLine("Eroor");
                break;
        }
        return dividend;
    }

    /**
     * Sum up all the delay for one job
     * @param depth the depth
     * @return cumulative delay
     */
    private static double getCumulativeDelay(int depth){
        double delay = 0.0;
        if(Parameters.getOverheadParams().getQueueDelay()!=null && 
                Parameters.getOverheadParams().getQueueDelay().containsKey(depth)){
            delay += Parameters.getOverheadParams().getQueueDelay().get(depth).getMLEMean();
        }
        if(Parameters.getOverheadParams().getWEDDelay()!=null &&
                Parameters.getOverheadParams().getWEDDelay().containsKey(depth)){
            delay += Parameters.getOverheadParams().getWEDDelay().get(depth).getMLEMean();
        }
        if(Parameters.getOverheadParams().getPostDelay()!=null &&
                Parameters.getOverheadParams().getPostDelay().containsKey(depth)){
            delay += Parameters.getOverheadParams().getPostDelay().get(depth).getMLEMean();
        }
        return delay;
    }
    
    private static double getOverheadLikelihoodPrior(int depth){
        double prior = 0.0;

        if(Parameters.getOverheadParams().getQueueDelay()!=null && 
                Parameters.getOverheadParams().getQueueDelay().containsKey(depth)){
            prior = Parameters.getOverheadParams().getQueueDelay().get(depth).getLikelihoodPrior();
        }else
        if(Parameters.getOverheadParams().getWEDDelay()!=null &&
                Parameters.getOverheadParams().getWEDDelay().containsKey(depth)){
            prior = Parameters.getOverheadParams().getWEDDelay().get(depth).getMLEMean();
        }else
        if(Parameters.getOverheadParams().getPostDelay()!=null &&
                Parameters.getOverheadParams().getPostDelay().containsKey(depth)){
            prior = Parameters.getOverheadParams().getPostDelay().get(depth).getMLEMean();
        }
        return prior;
    }
    
    /**
     * Dynamic Reclustering
     *
     * @param jobList, job list
     * @param job, job
     * @param id, job id
     * @param allTaskList, all task list
     * @return
     */
    private static List DRReclustering(List<Job> jobList, Job job, int id, List<Task> allTaskList) {
        Task firstTask = allTaskList.get(0);
      
        /**
         * Do not know why it is here. It is hard-coded, right?
         */
        double taskLength = (double) firstTask.getCloudletLength() / 1000 ;//+ Parameters.getOverheadParams().getClustDelay(job) / getDividend(job.getDepth());
        
        //FailureRecord record = new FailureRecord(taskLength, 0, job.getDepth(), allTaskList.size(), 0, 0, job.getUserId());
        
        double phi_ts = getOverheadLikelihoodPrior(job.getDepth());
        double delay = getCumulativeDelay(job.getDepth());
        //record.delayLength = getCumulativeDelay(job.getDepth());
        int suggestedK;//FailureMonitor.getClusteringFactor(record);
        
        double theta = FailureParameters.getGenerator(job.getVmId(), job.getDepth()).getMLEMean();
        
        double phi_gamma = FailureParameters.getGenerator(job.getVmId(), job.getDepth()).getLikelihoodPrior();
        suggestedK = ClusteringSizeEstimator.estimateK(taskLength, delay, 
                theta, phi_gamma, phi_ts);

        Log.printLine("t=" + taskLength +" d=" + delay + " theta=" + theta + " k=" + suggestedK);
        if (suggestedK == 0) {
            //not really k=0, just too big
            jobList.add(createJob(id, job, job.getCloudletLength(), allTaskList, true));
        } else {

            int actualK = 0;
            List<Task> taskList = new ArrayList<>();
            List<Job> retryJobs = new ArrayList<>();
            long length = 0;
            Job newJob = createJob(id, job, 0, null, false);
            for (Task task : allTaskList) {
                if (task.getCloudletStatus() == Cloudlet.FAILED) {//This is the difference
                    if (actualK < suggestedK) {
                        actualK++;
                        taskList.add(task);
                        length += task.getCloudletLength();
                    } else {
                        newJob.setTaskList(taskList);
                        taskList = new ArrayList();
                        newJob.setCloudletLength(length);
                        length = 0;
                        retryJobs.add(newJob);
                        id++;
                        newJob = createJob(id, job, 0, null, false);
                        actualK = 0;
                    }
                }
            }

            if (!taskList.isEmpty()) {
                newJob.setTaskList(taskList);
                newJob.setCloudletLength(length);
                retryJobs.add(newJob);
            }
            updateDependencies (job, retryJobs);
            jobList.addAll(retryJobs);

        }
        return jobList;
    }
    
    private static void updateDependencies (Job job, List<Job> jobList) {
        // Key idea: avoid setChildList(job.getChildList) within the for loop since
        // it will override the list
        List<Task> parents = job.getParentList();
        List<Task> children = job.getChildList();
        for (Job rawJob : jobList) { 
            rawJob.setChildList(children);
            rawJob.setParentList(parents);
        }
        for (Task parent : parents) {
            parent.getChildList().addAll(jobList);
        }
        for (Task childTask : children) {
            Job childJob = (Job) childTask;
            childJob.getParentList().addAll(jobList);
        }
    }
}
