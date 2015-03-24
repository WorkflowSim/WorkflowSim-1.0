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
package org.workflowsim;

import java.util.HashMap;
import java.util.Map;
import org.cloudbus.cloudsim.HarddriveStorage;
import org.cloudbus.cloudsim.ParameterException;

/**
 * ClusterStorage is an extention of HarddriveStorage and it is used as a local
 * storage system of a vm
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class ClusterStorage extends HarddriveStorage {

    /**
     * The map stores the bandwidth from this cluster-storage to others
     */
    Map<String, Double> bandwidthMap;

    /**
     * Initialize a ClusterStorage
     *
     * @param name, name of this storage
     * @param capacity, capacity
     * @throws ParameterException
     */
    public ClusterStorage(String name, double capacity) throws ParameterException {
        super(name, capacity);
    }

    /**
     * Sets the bandwidth between this storage to the destination storage
     *
     * @param name the destination storage
     * @param bandwidth
     */
    public final void setBandwidth(String name, double bandwidth) {
        if (bandwidth >= 0) {
            if (bandwidthMap == null) {
                bandwidthMap = new HashMap<>();
            }
            bandwidthMap.put(name, bandwidth);
        }
    }

    /**
     * Gets the bandwidth from this storage to the destination storage
     *
     * @param destination
     * @return bandwidth
     */
    public double getMaxBandwidth(String destination) {
        if (bandwidthMap.containsKey(destination)) {
            return bandwidthMap.get(destination);
        } else {
            //local bandwidth between vms
            return bandwidthMap.get("local");
        }
    }
}
