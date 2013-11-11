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
package org.workflowsim.planning;

import java.util.List;

/**
 * The Planner interface
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Jun 18, 2013
 */
public interface PlanningAlgorithmInterface {

    /**
     * Sets the task list.
     */
    public void setTaskList(List list);

    /**
     * Sets the vm list.
     */
    public void setVmList(List list);

    /**
     * Gets the task list.
     */
    public List getTaskList();

    /**
     * Gets the vm list. An algorithm must implement it
     */
    public List getVmList();

    /**
     * the main function.
     */
    public void run() throws Exception;


}
