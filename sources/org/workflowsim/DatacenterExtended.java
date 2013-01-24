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

import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

/**
 *
 * @author Weiwei Chen
 */
public class DatacenterExtended extends Datacenter{
    public DatacenterExtended(String name,
			DatacenterCharacteristics characteristics,
			VmAllocationPolicy vmAllocationPolicy,
			List<Storage> storageList,
			double schedulingInterval) throws Exception {
        super (name, characteristics, vmAllocationPolicy, storageList, schedulingInterval);
        
    }
    @Override
    protected void processOtherEvent(SimEvent ev) {
        
    }
/**
	 * Processes a Cloudlet submission.
	 * 
	 * @param ev a SimEvent object
	 * @param ack an acknowledgement
	 * @pre ev != null
	 * @post $none
	 */
    @Override
    protected void processCloudletSubmit(SimEvent ev, boolean ack) {
            updateCloudletProcessing();

            try {
                    // gets the Cloudlet object
                    Cloudlet cl = (Cloudlet) ev.getData();
//                    if(cl.getCloudletId()==1)
//                        Log.printLine(cl.getCloudletId());
                    // checks whether this Cloudlet has finished or not
                    if (cl.isFinished()) {
                            String name = CloudSim.getEntityName(cl.getUserId());
                            Log.printLine(getName() + ": Warning - Cloudlet #" + cl.getCloudletId() + " owned by " + name
                                            + " is already completed/finished.");
                            Log.printLine("Therefore, it is not being executed again");
                            Log.printLine();

                            // NOTE: If a Cloudlet has finished, then it won't be processed.
                            // So, if ack is required, this method sends back a result.
                            // If ack is not required, this method don't send back a result.
                            // Hence, this might cause CloudSim to be hanged since waiting
                            // for this Cloudlet back.
                            if (ack) {
                                    int[] data = new int[3];
                                    data[0] = getId();
                                    data[1] = cl.getCloudletId();
                                    data[2] = CloudSimTags.FALSE;

                                    // unique tag = operation tag
                                    int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                                    sendNow(cl.getUserId(), tag, data);
                            }

                            sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);

                            return;
                    }

                    // process this Cloudlet to this CloudResource
                    cl.setResourceParameter(getId(), getCharacteristics().getCostPerSecond(), getCharacteristics()
                                    .getCostPerBw());

                    int userId = cl.getUserId();
                    int vmId = cl.getVmId();
                    //Stage-in file && Shared
                    //if(isRealInputFile()){
                    if(cl.getClassType()==1){
                        stageInFile2FileSystem(cl);
                    }

                    // time to transfer the files
                    Task task = (Task)cl;
                    
                    if(cl.getClassType()!=1 ){
                        double value = Parameters.getOverheadParams().getRandom();
                        Random generator = new Random();
                        //if(cl.getCloudletId()==1){
                        Job job = (Job)task;
                        double impact = 0.0;
                        boolean flag = false;
                        for(int i = 0; i < job.getTaskList().size();i++){
                            Task t = job.getTaskList().get(i);
                            if(i==0){
                                impact = t.getImpact();
                                
                            }else{
                                if(t.getImpact()!= impact){
                                    flag = true;
                                    break;
                                }
                            }
                        }
                        //if(flag){
                        if(generator.nextDouble() < value){
                            Log.printLine("Yes");
                            long length = task.getCloudletLength();
                            task.setCloudletLength((long) ((double)length*Parameters.getOverheadParams().getRandom1()));
                        }
                    }
                    
                    
                    double fileTransferTime = processDataStageIn(task.getFileList(), cl);
                    //double fileTransferTime = predictFileTransferTime(cl.getRequiredFiles());
                    //fileTransferTime = 1.0;
                    //fileTransferTime = 0.0;
                    Host host = getVmAllocationPolicy().getHost(vmId, userId);
                    Vm vm = host.getVm(vmId, userId);
                    CloudletScheduler scheduler = vm.getCloudletScheduler();
                    double estimatedFinishTime = scheduler.cloudletSubmit(cl, fileTransferTime);

                    // if this cloudlet is in the exec queue
                    if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
                            //estimatedFinishTime += fileTransferTime;
                            send(getId(), estimatedFinishTime, CloudSimTags.VM_DATACENTER_EVENT);
                    }else{
                        Log.printLine("Error here");
                    }

                    if (ack) {
                            int[] data = new int[3];
                            data[0] = getId();
                            data[1] = cl.getCloudletId();
                            data[2] = CloudSimTags.TRUE;

                            // unique tag = operation tag
                            int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                            sendNow(cl.getUserId(), tag, data);
                    }
            } catch (ClassCastException c) {
                    Log.printLine(getName() + ".processCloudletSubmit(): " + "ClassCastException error.");

            } catch (Exception e) {
                    Log.printLine(getName() + ".processCloudletSubmit(): " + "Exception error.");

            }

            checkCloudletCompletion();
    }

    private void stageInFile2FileSystem(Cloudlet cl){
        Task t1 = (Task)cl;
        List fList = t1.getFileList();

        for(Iterator it = fList.iterator(); it.hasNext();){
            org.cloudbus.cloudsim.File file = (org.cloudbus.cloudsim.File)it.next();
            if(true){//in generating this stage-in file we have checked it
            //if(isRealInputFile( fList, file)){//has no output data
              switch (ReplicaCatalog.getFileSystem()){
                case LOCAL:
                    //not sure
                     ReplicaCatalog.addStorageList(file.getName(), this.getName());
                     ClusterStorage storage = (ClusterStorage)getStorageList().get(0);
                     //not sure
                     storage.addFile(file);
                     break;
                case SHARED:
                     ReplicaCatalog.addStorageList(file.getName(), this.getName());
                     break;
                default:
                break;
              }
            }
        }
    }
    /**
     * Predict file transfer time.
     * 
     * @param requiredFiles the required files (both input and output
     * @return the double
     */
    /*
    protected double predictFileTransferTime(List<String> requiredFiles) {
            double time = 0.0;

            Iterator<String> iter = requiredFiles.iterator();
            while (iter.hasNext()) {
                    String fileName = iter.next();
                    //tempStorage is what the data center has
                    ClusterStorage tempStorage = (ClusterStorage)getStorageList().get(0);
                    File tempFile = (File)WorkflowPlanner.FileName2File.get(fileName);
                    //Input File
                    if(tempFile.getType() ==1 ){
                        //File tempFile = tempStorage.getFile(fileName);
                        List siteList = (List)WorkflowPlanner.ReplicaCatalog.get(fileName);
                        double maxBwth = 0.0;
                        for(Iterator it = siteList.iterator(); it.hasNext();)
                        {
                            //site is where one replica of this data is located at
                            String site = (String)it.next();
                            double bwth = tempStorage.getMaxTransferRate(site);
                            if(bwth > maxBwth){
                                maxBwth = bwth;
                            }
                        }

                        time += tempFile.getSize() / maxBwth;
                        //        break;
                    }
            }
            return time;
    }*/
    /**
     * If a input file has an output file it does not need stage-in
     * For workflows, we have a rule that a file is written once and 
     * read many times, thus if a file is an output file it means it
     * is generated within this job and then used by another task 
     * within the same job (or other jobs maybe)
     * This is useful when we perform horizontal clustering
     */
    private boolean isRealInputFile(List<File> list, File file){
        if(file.getType() == 1)//input file
        {
            for(File another: list){
                if(another.getName().equals(file.getName()) 
                        && another.getType()==2){
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    /*
     * Stage in for a single job
     */
    protected double processDataStageIn(List<File> requiredFiles, Cloudlet cl) throws Exception{
            double time = 0.0;
            
            Iterator<File> iter = requiredFiles.iterator();
            while (iter.hasNext()) {

                    File file = iter.next();
                    ClusterStorage tempStorage = (ClusterStorage)getStorageList().get(0);
                    
                    //Input File
                    if(isRealInputFile( requiredFiles, file) ){
                        double maxBwth = 0.0;
                        List siteList = ReplicaCatalog.getStorageList(file.getName());
                        if(siteList.isEmpty()){
                            throw new Exception(file.getName() + " does not exist");
                        }
                        
                        
                        switch(ReplicaCatalog.getFileSystem()){
                            case SHARED:
                                //stage-in job
                                if(cl.getClassType()==1){
                                    
                                    for(Iterator it = siteList.iterator(); it.hasNext();)
                                    {
                                        //site is where one replica of this data is located at
                                        String site = (String)it.next();
                                        double bwth = tempStorage.getMaxTransferRate(site);
                                        if(bwth > maxBwth){
                                            maxBwth = bwth;
                                        }
                                    }
                                    time += file.getSize() / maxBwth;
                                    //Log.printLine(file.getName() + " " + file.getSize());
                                }
                                
                                
                                break;
                            case LOCAL:
                                
                                for(Iterator it = siteList.iterator(); it.hasNext();)
                                {
                                    //site is where one replica of this data is located at
                                    String site = (String)it.next();
                                    double bwth = tempStorage.getMaxTransferRate(site);
                                    if(bwth > maxBwth){
                                        maxBwth = bwth;
                                    }
                                }
                                time += file.getSize() / maxBwth;
                                
                                int vmId = cl.getVmId();
                                int userId = cl.getUserId();
                                Host host = getVmAllocationPolicy().getHost(vmId, userId);
                                Vm vm = host.getVm(vmId, userId);
                                CondorVM condorVm = (CondorVM)vm;
                                /**
                                 * Storage is too small?
                                 */
                                //condorVm.addLocalFile(file);
                                ReplicaCatalog.addStorageList(file.getName(), Integer.toString(vmId));
                                
                                
                                break;
                            

                        }
                        
                    }
            }
            
            return time;
    }
    
    @Override
    	protected void updateCloudletProcessing() {
		// if some time passed since last processing
		// R: for term is to allow loop at simulation start. Otherwise, one initial
		// simulation step is skipped and schedulers are not properly initialized
        //this is a bug of CloudSim if the runtime is smaller than 0.1 (now is 0.01) it doesn't work at all
		if (CloudSim.clock() < 0.111 || CloudSim.clock() > getLastProcessTime() + 0.01) {
			List<? extends Host> list = getVmAllocationPolicy().getHostList();
			double smallerTime = Double.MAX_VALUE;
			// for each host...
			for (int i = 0; i < list.size(); i++) {
				Host host = list.get(i);
				// inform VMs to update processing
				double time = host.updateVmsProcessing(CloudSim.clock());
				// what time do we expect that the next cloudlet will finish?
				if (time < smallerTime) {
					smallerTime = time;
				}
			}
			// gurantees a minimal interval before scheduling the event
			if (smallerTime < CloudSim.clock() + 0.11) {
				smallerTime = CloudSim.clock() + 0.11;
			}
			if (smallerTime != Double.MAX_VALUE) {
				schedule(getId(), (smallerTime - CloudSim.clock()), CloudSimTags.VM_DATACENTER_EVENT);
			}
			setLastProcessTime(CloudSim.clock());
		}
	}
    
    
	/**
	 * Verifies if some cloudlet inside this PowerDatacenter already finished. If yes, send it to
	 * the User/Broker
	 * 
	 * @pre $none
	 * @post $none
	 */
    @Override
    protected void checkCloudletCompletion() {
            List<? extends Host> list = getVmAllocationPolicy().getHostList();
            for (int i = 0; i < list.size(); i++) {
                    Host host = list.get(i);
                    for (Vm vm : host.getVmList()) {
                            while (vm.getCloudletScheduler().isFinishedCloudlets()) {
                                    Cloudlet cl = vm.getCloudletScheduler().getNextFinishedCloudlet();
                                    if (cl != null) {
                                            sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);
                                            register(cl);
                                    }
                            }
                    }
            }
    }
    /*
     * Stage-out for a single job
     */
    private void register(Cloudlet cl)
    {
        Task tl = (Task)cl;
        List fList = tl.getFileList();
        for(Iterator it = fList.iterator(); it.hasNext();){
            org.cloudbus.cloudsim.File file = (org.cloudbus.cloudsim.File)it.next();
            if(file.getType()==2)//output file
            {
                
                switch(ReplicaCatalog.getFileSystem()){
                    case SHARED:
                        ReplicaCatalog.addStorageList(file.getName(), this.getName());
                        break;
                    case LOCAL:
                        int vmId = cl.getVmId();
                        int userId = cl.getUserId();
                        Host host = getVmAllocationPolicy().getHost(vmId, userId);
                        CondorVM vm = (CondorVM)host.getVm(vmId, userId);
                        //should you add it to the local list? it is implemented 
                        //it is implemented in condorvm
                        /*
                         * Constraint
                         */
                        //vm.addLocalFile(file);
                        //another approach for storing global path
                        ReplicaCatalog.addStorageList(file.getName(), Integer.toString(vmId));
                        break;
                }
            }
        }
    }
}
