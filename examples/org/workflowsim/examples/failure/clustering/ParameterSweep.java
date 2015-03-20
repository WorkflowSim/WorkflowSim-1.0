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
package org.workflowsim.examples.failure.clustering;

/**
 *
 * @author chenweiwei
 */
public class ParameterSweep {

    public static void main(String[] args) {
        String p = "10";
        if (args.length != 0) {
            p = args[0];
        }

        String clustering = "DR";
        //Search for best
        String result = "";
        for (double q_scale = 10; q_scale <= 100; q_scale += 10) {
            for (double q_weight = 10; q_weight <= 10e4; q_weight *= 10) {
                for (double q_shape = 2; q_shape <= 10; q_shape += 2) {
                    for (double theta_weight = 10; theta_weight <= 10e4; theta_weight *= 10) {
                        double makespan = execute100(p, q_scale, q_weight, q_shape, theta_weight, clustering);
                        result += q_scale + " " + q_weight + " " + q_shape + " " + theta_weight + " " + makespan;
                        result += "\n";
                    }
                }
            }
        }
        System.out.println(result);
    }

    public static double execute(String p, double q_scale, double q_weight, double q_shape,
            double theta_weight, String clustering) {
        //String dax = "/Users/chenweiwei/Research/balanced_clustering/generator/BharathiPaper/Montage_300.xml";
        String dax = "/root/Montage_300.xml";
        String[] args = {"-d", dax,
            "-q", Double.toString(q_scale),
            "-w", Double.toString(q_weight),
            "-s", Double.toString(q_shape),
            "-p", p,
            "-t", Double.toString(theta_weight),
            "-c", clustering};
        return FaultTolerantClusteringExample5.main2(args);
    }

    public static double execute100(String p, double q_scale, double q_weight, double q_shape,
            double theta_weight, String clustering) {
        double sum = 0.0;
        int n = 100;
        for (int i = 0; i < n; i++) {
            sum += execute(p, q_scale, q_weight, q_shape, theta_weight, clustering);
        }
        sum /= n;
        return sum;
    }
}
