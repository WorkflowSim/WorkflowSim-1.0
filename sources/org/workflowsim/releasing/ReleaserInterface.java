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

/**
 * The Releaser interface
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Jul 7, 2013
 */
public interface ReleaserInterface {
    
    /**
     * Sets the job list.
     */
    public void setJobList(List list);

    /**
     * Sets the vm list.
     */
//    public void setVmList(List list);

    /**
     * Gets the job list.
     */
    public List getJobList();

    /**
     * Gets the vm list. An algorithm must implement it
     */
//    public List getVmList();

    /**
     * Sets the released jobs.
     */
    //public void setReleasedJobList(List list);
    
    /**
     * Getst the released jobs
     * @return released jobs
     */
    //public List getReleasedJobList();
    /**
     * the main function.
     */
    public void run() throws Exception;


}
