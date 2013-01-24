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
package org.workflowsim.failure;

/**
 *
 * @author chenweiwei
 */
public class FailureRecord {
    
    public double length;
    
    public int failedTasksNum;
    
    public int depth;//used as type
    
    public int allTaskNum;
    
    public int vmId;
    
    public int jobId;
    
    public int workflowId;
    
    public double delayLength;
    
    public FailureRecord(double length, int tasks, int depth, int all, int vm, int job, int workflow){
        this.length = length;
        this.failedTasksNum = tasks;
        this.depth = depth;
        this.allTaskNum = all;
        this.vmId = vm;
        this.jobId = job;
        this.workflowId = workflow;
    }
}
