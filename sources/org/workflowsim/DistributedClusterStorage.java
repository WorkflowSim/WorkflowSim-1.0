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
package org.workflowsim;

import org.cloudbus.cloudsim.HarddriveStorage;
import org.cloudbus.cloudsim.ParameterException;

/**
 * DistributedClusterStorage assumes each vm has a local storage and in your configuration
 * please use LOCAL file system. 
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Nov 9, 2013
 */
public class DistributedClusterStorage extends HarddriveStorage{
    
    /**
     * bandwidth from one vm to another vm
     */
    double [][] bandwidthMap;
    
    /**
     * the bandwidth from submit host ('local') to a vm
     * it is used in stage-in job
     */
    double baseBandwidth;
    /**
     * Number of vms (local storage) in this cluster
     */
    int vmm;
   /**
     * Initialize a ClusterStorage
     *
     * @param name, name of this storage
     * @param capacity, capacity
     * @throws ParameterException
     */
    public DistributedClusterStorage(String name, double capacity, int vmm, double baseBw) throws ParameterException {
        super(name, capacity);
        this.vmm = vmm;
        bandwidthMap = new double[vmm][vmm];
        baseBandwidth = baseBw;
    }
    
    /**
     * Gets the bandwidth from one vm to another vm
     * @param source vmId
     * @param destination vmId
     * @return the bandwidth from source to destination
     * @throws ParameterException 
     */
    public double getBandwidth(int source, int destination) throws ParameterException{
        return bandwidthMap[source][destination];
    }
    
    /*
     * Sets the bandwidth map
     */
    public void setBandwidth(int source, int destination, double bandwidth) throws ParameterException{
        bandwidthMap[source][destination]  = bandwidth;
    }
    
    /*
     * Sets the bandwidth map
     */
    public void setBandwidth(double[][] bandwidths){
        bandwidthMap = bandwidths;
    }
    /**
     * Gets the number of vms
     * @return the number of vms
     */
    public int getVmNumber(){
        return this.vmm;
    }
    
    /**
     * Gets the bandwidth from submit host ('local') to a vm
     * @return base bandwidth
     */
    public double getBaseBandwidth(){
        return this.baseBandwidth;
    }
}
