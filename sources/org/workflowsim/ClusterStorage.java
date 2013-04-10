/**
 *  Copyright 2012-2013 University Of Southern California
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

import java.util.HashMap;
import java.util.Map;
import org.cloudbus.cloudsim.HarddriveStorage;
import org.cloudbus.cloudsim.ParameterException;
import org.workflowsim.utils.Parameters;

/**
 * ClusterStorage is an extention of HarddriveStorage and it is used as a local 
 * storage system of a vm
 * 
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class ClusterStorage extends HarddriveStorage{
    
    Map bandwidthMap ;
    
    
    public ClusterStorage(String name, double capacity) throws ParameterException
    {
        super(name, capacity);
        setBandwidthMap();
    }
    public ClusterStorage(double capacity) throws ParameterException 
    {
        super(capacity);
        setBandwidthMap();
    }
    public final void setBandwidthMap()
    {
        bandwidthMap = new HashMap<String, Double>();
        //double bandwidth = Parameters.getOverheadParams().getBandwidth();//20MB/sec
        double bandwidth = 2e8;
        bandwidthMap.put("local", bandwidth);
        bandwidthMap.put("Datacenter_0", bandwidth);
        bandwidthMap.put("Datacenter_1", bandwidth);
        bandwidthMap.put("Datacenter_2", bandwidth);
        bandwidthMap.put("Datacenter_3", bandwidth);
    }
    
    public double getMaxTransferRate(String destination) {
        if(bandwidthMap.containsKey(destination)){
            return (Double)bandwidthMap.get(destination);
        }else{
            //local bandwidth
            return Parameters.getOverheadParams().getBandwidth();
        }
    }
}
