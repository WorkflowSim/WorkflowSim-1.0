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
package org.workflowsim.clustering;

import java.util.List;
import org.workflowsim.FileItem;
import org.workflowsim.Job;
import org.workflowsim.Task;

/**
 * The ClusteringInterface for all clustering methods
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public interface ClusteringInterface {

    /**
     * set the task list.
     * @param list
     */
    public void setTaskList(List<Task> list);

    /**
     * get job list.
     * @return 
     */
    public List<Job> getJobList();

    /**
     * get task list.
     * @return 
     */
    public List<Task> getTaskList();

    /**
     * the main function.
     */
    public void run();

    /**
     * get all the task files.
     * @return 
     */
    public List<FileItem> getTaskFiles();
}
