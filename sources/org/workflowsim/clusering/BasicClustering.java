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
package org.workflowsim.clusering;

import org.workflowsim.Job;
import org.workflowsim.Task;
//import edu.isi.pegasus.workflowsim.TaskFile;
import org.workflowsim.utils.Parameters;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The default clustering does no clustering at all, just map a task to a job
 * @author Weiwei Chen
 */
public class BasicClustering implements ClusteringInterface{
    
    private List<Task> taskList;
    
    private List<Job> jobList;
    
    private Map mTask2Job;
    
    private List<org.cloudbus.cloudsim.File> allFileList;
    
    //private long inputDataSize;
    
    private Task root;
    
    private int idIndex;
    @Override
    public final List<org.cloudbus.cloudsim.File> getTaskFiles(){
        return this.allFileList;
    }
    
//    @Override
//    public final long getInputDataSize(){
//        return this.inputDataSize;
//    }
    
    
    public BasicClustering()
    {
        this.jobList        = new ArrayList<Job>();
        this.taskList       = new ArrayList<Task>();
        this.mTask2Job      = new HashMap<Task, Job>();
        
        this.allFileList    = new ArrayList<org.cloudbus.cloudsim.File>();
        //this.inputDataSize  = 0;
        
        this.idIndex        = 0;
        this.root           = null;
    }
    
    @Override
    public final void setTaskList(List<Task> list){
        this.taskList = list;
    }
    
    @Override
    public final List<Job> getJobList(){
        return this.jobList;
    }
    
    @Override
    public final List<Task>getTaskList(){
        return this.taskList;
    }
    
    public final Map getTask2Job(){
        return this.mTask2Job;
    }
    @Override
    public void run(){
        getTask2Job().clear();
        for(Iterator it = getTaskList().iterator();it.hasNext();)
        {
            Task task = (Task)it.next();
            //twist here
            List taskList = new ArrayList();
            taskList.add(task);
            Job job = addTasks2Job(taskList);
            getTask2Job().put(task, job);
            
        }
        updateDependencies();
        
    }
    protected final Job addTasks2Job(Task task){
        ArrayList job = new ArrayList();
        job.add(task);
        return addTasks2Job(job);
    }
    protected final Job addTasks2Job(List taskList){
        if(taskList!=null && !taskList.isEmpty())
        {
            //Log.printLine("Size:" + taskList.size());
            int length = 0;
            //long inputFileSize =0;
            //long outputFileSize =0;
            int userId = 0;
            int priority = 0;
            int depth = 0;
            /// a bug of cloudsim makes it final of input file size and output file size
            Job job = new Job(idIndex, length/*, inputFileSize, outputFileSize*/);
            for(Iterator it = taskList.iterator();it.hasNext();)
            {
                Task task = (Task)it.next();
                length += task.getCloudletLength();
                //no use at all
                //inputFileSize += task.getCloudletFileSize();
                //outputFileSize += task.getCloudletOutputSize();
                userId      = task.getUserId();
                priority    = task.getPriority();
                depth       = task.getDepth();
                List fileList = task.getFileList();
                job.getTaskList().add(task);
                //job.getFileList().addAll(fileList);
                
                getTask2Job().put(task, job);
                //both
                for(Iterator itc= fileList.iterator(); itc.hasNext();)
                {
                    org.cloudbus.cloudsim.File file = (org.cloudbus.cloudsim.File)itc.next();
                    //if(!WorkflowPlanner.FileName2File.containsKey(file.getName())){
                    //    WorkflowPlanner.FileName2File.put(file.getName(), file);
                    //}
                    //ReplicaCatalog.setFile(file.getName(), file);
                    boolean hasFile = false;
                    /**
                     * Make sure we have only one input instance of file
                     */
                   
                    hasFile = job.getFileList().contains(file);
                     /*
                    for(Iterator itd = job.getFileList().iterator(); itd.hasNext();)
                    {
                        org.cloudbus.cloudsim.File pFile = (org.cloudbus.cloudsim.File)itd.next();
                        if(pFile.getName().equals(file.getName()))
                        {
                            hasFile = true;
                            break;
                        }
                    }*/
                    if(!hasFile){
                        
                        job.getFileList().add(file);
                        if(file.getType() == 1){
                        //if(file.getInout().equals("input")){
                            //inputFileSize += file.getSize();
                            
                            //for stag-in jobs to be used
                            if(!this.allFileList.contains(file)){
                                this.allFileList.add(file);
                                //this.inputDataSize += file.getSize();
                            }
                        }else 
                            if(file.getType() == 2){
                            //if(file.getInout().equals("output")){
                                this.allFileList.add(file);
                                //outputFileSize += file.getSize();
                        }
                    }
                }
                //next
                for(Iterator itc = task.getRequiredFiles().iterator();itc.hasNext();){
                    String fileName = (String)itc.next();
                    
                    if(!job.getRequiredFiles().contains(fileName))
                    {
                        job.getRequiredFiles().add(fileName);
                    }
                }
                
            }
            
//            job.setInputDataSize(inputFileSize);
//            job.setOutputDataSize(outputFileSize);
            
            //because a bug of cloudsim I have to redo it here
            job.setCloudletLength(length);
            job.setUserId(userId);
            job.setDepth(depth);
            job.setPriority(priority);
            
            idIndex++;
            getJobList().add(job);
            return job;
        }
                                 
        return null;
    }
    public void addClustDelay(){
        
        for(Iterator it = getJobList().iterator();it.hasNext();){
            Job job = (Job)it.next();
            
                
                double delay = Parameters.getOverheadParams().getClustDelay(job);
                delay *= 1000;
                long length = job.getCloudletLength();
                length += (long)delay;
                job.setCloudletLength(length);
            
        }
    }
    protected final void updateDependencies()
    {
        for(Iterator it = getTaskList().iterator();it.hasNext();)
        {
            Task task = (Task)it.next();

            Job job = (Job)getTask2Job().get(task);
            for (Iterator itp = task.getParentList().iterator();itp.hasNext();)
            {
              Task parentTask = (Task)itp.next();
              Job parentJob = (Job)getTask2Job().get(parentTask);
              if(!job.getParentList().contains(parentJob)&&parentJob!=job){//avoid dublicate
                  job.addParent(parentJob);
              }
            }
            for(Iterator itc = task.getChildList().iterator();itc.hasNext();)
            {
                Task childTask = (Task)itc.next();
                Job childJob = (Job)getTask2Job().get(childTask);
                if(!job.getChildList().contains(childJob)&&childJob!=job){//avoid dublicate
                   job.addChild(childJob);
                }
            }

        }
        getTask2Job().clear();
        getTaskList().clear();
    }
    /*
     * If you have used addRoot, please use clean() after that
     */
    public Task addRoot(){
        
        if(root==null){
            //bug maybe
            root = new Task(taskList.size() + 1,0/*,0,0*/);
            for(Iterator it = taskList.iterator(); it.hasNext();){
                Task node = (Task)it.next();
                if(node.getParentList().isEmpty()){
                    node.addParent(root);
                    root.addChild(node);
                }
            }
            //it's not sure here
            taskList.add(root);
            
        }
        return root;
    
    }
    public void clean(){
        if(root!=null){
            for(int i = 0; i < root.getChildList().size();i++){
                Task node = (Task)root.getChildList().get(i);
                node.getParentList().remove(root);
                root.getChildList().remove(node);
                i--;
            }
            taskList.remove(root);
        }
    }
}
