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
import java.util.List;

/**
 *
 * @author Weiwei Chen
 */
public interface ClusteringInterface {
    
    public void setTaskList(List<Task> list);
    
    public List<Job> getJobList();
    
    public List<Task> getTaskList();
    
    public void run();
    
    public List<org.cloudbus.cloudsim.File> getTaskFiles();
    
    //public long getInputDataSize();
}
