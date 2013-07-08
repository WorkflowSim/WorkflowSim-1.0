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

import java.util.List;
import org.cloudbus.cloudsim.Cloudlet;

/**
 * The Releaser base class
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Jul 7, 2013
 */
public abstract class BaseReleaser implements ReleaserInterface{
    
   /**
     * the job list.
     */
    private List<? extends Cloudlet> jobList;
    
//    private List<? extends Cloudlet> releasedJobList;
    
    public BaseReleaser(){
        
    }
    
    /**
     * Sets the job list.
     */
    @Override
    public void setJobList(List list){
        this.jobList = list;
    }
    
    /**
     * Sets the released jobs.
     */
//    @Override
//    public void setReleasedJobList(List list){
//        this.releasedJobList = list;
//    }
    
    /**
     * Gets the job list.
     */
    @Override
    public List getJobList(){
        return this.jobList;
    }
    /**
     * Getst the released jobs
     * @return released jobs
     */
//    @Override
//    public List getReleasedJobList(){
//        return this.releasedJobList;
//    }
    
    
}
