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
package org.workflowsim.utils;

import org.cloudbus.cloudsim.Log;

/**
 *
 * @author chenweiwei
 */
public class ClusteringParameters {
    
    private int clusters_num;
    
    private int clusters_size;
    
    public enum ClusteringMethod{
        HORIZONTAL, VERTICAL, NONE, BLOCK, BALANCED
    }
    private String code;
    
    private ClusteringMethod method;
    
    
    public String getCode(){
        return code;
    }
    public int getClustersNum(){
        return clusters_num;
    }
    public int getClustersSize(){
        return clusters_size;
    }
    public ClusteringMethod getClusteringMethod(){
        return method;
    }
    public ClusteringParameters(int cNum, int cSize, ClusteringMethod method, String code){
        this.clusters_num   = cNum;
        this.clusters_size  = cSize;
        this.method         = method;
        this.code           = code;
    }
    
}
