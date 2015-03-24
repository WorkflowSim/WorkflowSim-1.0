/**
 * Copyright 2012-2013University Of Southern California
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
package org.workflowsim.utils;

/**
 * ClusteringParameters contains all the parameters used in task clustering
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class ClusteringParameters {

    /**
     * The number of clustered jobs per level. You just need to set one of
     * clusters.num or clusteres.size
     */
    private final int clusters_num;
    /**
     * The size of clustered jobs (=The number of tasks in a clustered job)
     */
    private final int clusters_size;

    /**
     * Supported Clustering Method, by default it is none
     */
    public enum ClusteringMethod {

        HORIZONTAL, VERTICAL, NONE, BLOCK, BALANCED
    }
    /**
     * Used for balanced clustering to tell which specific balanced clustering
     * to use
     */
    private final String code;
    /**
     * Supported Clustering Method, by default it is none
     */
    private final ClusteringMethod method;

    /**
     * Gets the code for balanced clustering Please refer to our balanced
     * clustering paper for details
     *
     * @return the code
     */
    public String getCode() {
        return code;
    }

    /**
     * Gets the number of clustered jobs per level
     *
     * @return clusters.num
     */
    public int getClustersNum() {
        return clusters_num;
    }

    /**
     * Gets the size of clustered jobs, which is equal to the number of tasks in
     * a job
     *
     * @return clusters.size
     */
    public int getClustersSize() {
        return clusters_size;
    }

    /**
     * Gets the clustering method
     *
     * @return clusters.method
     */
    public ClusteringMethod getClusteringMethod() {
        return method;
    }

    /**
     * Initialize a ClusteringParameters
     *
     * @param cNum, clustes.num
     * @param cSize, clusters.size
     * @param method, clusters.method
     * @param code , balanced clustering code (used for research)
     */
    public ClusteringParameters(int cNum, int cSize, ClusteringMethod method, String code) {
        this.clusters_num = cNum;
        this.clusters_size = cSize;
        this.method = method;
        this.code = code;
    }
}
