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
package org.workflowsim;

/**
 *
 * @author Weiwei Chen
 */
public class WorkflowSimTags {

	/** Starting constant value for cloud-related tags **/
	private static final int BASE = 1000;

        
        public static final int VM_STATUS_READY         = BASE + 2;
        
        public static final int VM_STATUS_BUSY          = BASE + 3;
        
        public static final int VM_STATUS_IDLE          = BASE + 4;

        
        public static final int START_SIMULATION        = BASE + 0;
        
        public static final int JOB_SUBMIT              = BASE + 1;
        
        public static final int CLOUDLET_UPDATE         = BASE + 5;
        
        public static final int CLOUDLET_CHECK          = BASE + 6;

	/** Private Constructor */
	private WorkflowSimTags() {
		throw new UnsupportedOperationException("WorkflowSim Tags cannot be instantiated");
	}



}
