/*
 * 
 *   Copyright 2012-2013 University Of Southern California
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
package org.workflowsim.releasing;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.Job;

/**
 * The IFS Releaser 
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Jul 7, 2013
 */
public class ImportantFirstReleaser extends BaseReleaser{
    
    

    /**
     * Sort jobs in an descending order of impact factor
     * @param jobList jobs to be sorted
     */
    private void sortList(List list) {
        Collections.sort(list, new Comparator<Job>() {
            public int compare(Job t1, Job t2) {
                //Decreasing order
                return new Double( t2.getImpact() ).compareTo(new Double( t1.getImpact()) );
                

            }
        });

    }
    
    /**
     * the main function.
     */
    public void run() throws Exception{
        processImpactFactor(getJobList());
//        for(Iterator it = getJobList().iterator(); it.hasNext(); ){
//            Job job = (Job)it.next();
//            Log.printLine("IF:" + job.getImpact());
//        }
//        Log.printLine("Done");
        if(! getJobList().isEmpty()){
            sortList(getJobList());
        }
//                for(Iterator it = getJobList().iterator(); it.hasNext(); ){
//            Job job = (Job)it.next();
//            Log.printLine("IF:" + job.getImpact());
//        }
//        Log.printLine("Done Done");
    }
    private double getImpactFactor(Job job){
        if(job.getChildList().isEmpty()){
                job.setImpact(1.0);
        }
        if(job.getImpact()!= 0.0){
            return job.getImpact();
        }
        double impact  = 0.0;
        for(Iterator it = job.getChildList().iterator(); it.hasNext();){
            Job child = (Job) it.next();
            impact += getImpactFactor(child) / child.getParentList().size();
        }
        job.setImpact(impact);
        return impact;
    }
    
    public void processImpactFactor(List list){
        for(Iterator it = list.iterator(); it.hasNext();){
            Job job = (Job) it.next();
            getImpactFactor(job);
        }
    }
    
}
