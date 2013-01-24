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

import org.workflowsim.utils.ReplicaCatalog;
import org.workflowsim.utils.ReplicaCatalog.FileSystem;
import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Vm;

/**
 * Condor Vm represents a VM: it runs inside a Host, sharing hostList with other VMs. It processes
 * cloudlets. This processing happens according to a policy, defined by the CloudletScheduler. Each
 * VM has a owner, which can submit cloudlets to the VM to be executed
 * 
 * @author Weiwei Chen
 */
public class CondorVM extends Vm{

	
	/**
	 * Creates a new VMCharacteristics object.
	 * 
	 * @param id unique ID of the VM
	 * @param userId ID of the VM's owner
	 * @param mips the mips
	 * @param numberOfPes amount of CPUs
	 * @param ram amount of ram
	 * @param bw amount of bandwidth
	 * @param size amount of storage
	 * @param vmm virtual machine monitor
	 * @param cloudletScheduler cloudletScheduler policy for cloudlets
	 * @pre id >= 0
	 * @pre userId >= 0
	 * @pre size > 0
	 * @pre ram > 0
	 * @pre bw > 0
	 * @pre cpus > 0
	 * @pre priority >= 0
	 * @pre cloudletScheduler != null
	 * @post $none
	 */
        private ClusterStorage storage;
        private int state;
	public CondorVM(
			int id,
			int userId,
			double mips,
			int numberOfPes,
			int ram,
			long bw,
			long size,
			String vmm,
			CloudletScheduler cloudletScheduler) {
		super(id, userId, mips, numberOfPes, ram, bw, size, vmm, cloudletScheduler);
                setState(WorkflowSimTags.VM_STATUS_IDLE);
                if(ReplicaCatalog.getFileSystem()==FileSystem.LOCAL){
                    try{
                        storage = new ClusterStorage(Integer.toString(id),1e6);
                    }catch(Exception e){

                    }
                }
	}
        
        public final void setState(int tag)
        {
            this.state = tag;
        }
        
        public final int getState()
        {
            return this.state;
        }
        /**
         * We have implemented a local file system which stores all local files
         * @param file 
         */
        public void addLocalFile(org.cloudbus.cloudsim.File file){
            this.storage.addFile(file);//localStoredFiles.add(file);
        }
        public void removeLocalFile(org.cloudbus.cloudsim.File file){
            this.storage.deleteFile(file);
            /*if(hasLocalFile(file)){
                this.localStoredFiles.remove(file);
                return true;
            }else{
                return false;
            }*/
        }
        public boolean hasLocalFile(org.cloudbus.cloudsim.File file){
            return this.storage.contains(file);
            //return this.localStoredFiles.contains(file);
            
        }
        //public ArrayList<org.cloudbus.cloudsim.File> getLocalFiles(){
            
            //return this.localStoredFiles;
        //}

}
