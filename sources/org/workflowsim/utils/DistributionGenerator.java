/*
 * 
 *   Copyright 2013-2014 University Of Southern California
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
package org.workflowsim.utils;

import java.util.ArrayList;
import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.distribution.WeibullDistribution;

/**
 * This is a OverheadDistributionGenrator for one typic overhead per level. 
 * 
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Mar 11, 2014
 */
public class DistributionGenerator {
    
    protected DistributionFamily dist;
    protected double scale;
    protected double shape;
    private double scale_prior;
    private double shape_prior;
    private double[] samples;
    private int cursor;
    private final int SAMPLE_SIZE = 1500;
    public enum DistributionFamily {
        LOGNORMAL, GAMMA, WEIBULL, NORMAL
    }
    /**
     * 
     * @param dist
     * @param scale
     * @param shape 
     */
    public DistributionGenerator(DistributionFamily dist, double scale, double shape){
        this.dist = dist;
        this.scale = scale;
        this.shape = shape;
        this.scale_prior = scale;
        this.shape_prior = shape;
        RealDistribution distribution = getDistribution(scale, shape);
        samples = distribution.sample(SAMPLE_SIZE);
        cursor = 0;
    }
    
    public DistributionGenerator(DistributionFamily dist, double scale, double shape, double a, double b){
        this(dist, scale, shape);
        this.scale_prior = b;
        this.shape_prior = a;
    }
    
    /**
     * Gets the Prior Knowledge based Estimation of samples
     * @return delay
     */
    public double getPKEMean(){
        return this.shape_prior/this.scale_prior;
    }
    /**
     * Gets the average of samples
     * @return average
     */
    public double getMean(){
        double sum = 0.0;
        for(int i = 0; i < cursor; i ++){
            sum += samples[i];
        }
        return sum/cursor;
    }
    
    /**
     * Gets the Maximum Likelihood Estimation of samples
     * @return the delay
     */
     
    public double getMLEMean(){
        double a = shape_prior, b = scale_prior;
        double sum = 0.0;
        for(int i = 0; i < cursor; i ++){
            sum += samples[i];
        }
        //???
        return (b+sum)/(a + cursor + 1);
    }
    
    /**
     * Vary the distribution parameters but not the prior knowledge
     * @param scale the first param
     * @param shape  the second param
     */
    public void varyDistribution(double scale, double shape){
        this.scale = scale;
        this.shape = shape;
        RealDistribution distribution = getDistribution(scale, shape);
        samples = distribution.sample(SAMPLE_SIZE);
        cursor = 0;
    }
    
    /**
     * Gets the next sample from samples
     * @return delay
     */
    public double getNextSample(){
        double delay = samples[cursor];
        cursor ++ ;
        return delay;
    }    
    
    /**
     * Gets the RealDistribution with two parameters
     * @param scale the first param scale 
     * @param shape the second param shape
     * @return the RealDistribution Object
     */
    public RealDistribution getDistribution(double scale, double shape) {
        RealDistribution distribution = null;
        switch (this.dist) {
            case LOGNORMAL:
                distribution = new LogNormalDistribution(scale, shape);
                break;
            case WEIBULL:
                distribution = new WeibullDistribution(shape, scale);
                break;
            case GAMMA:
                distribution = new GammaDistribution(shape,  scale);
                break;
            case NORMAL:
                //shape is the std, scale is the mean
                distribution = new NormalDistribution(scale, shape);
                break;
            default:
                break;
        }
        return distribution;
    }
}
