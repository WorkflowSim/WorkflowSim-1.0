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

import java.util.Arrays;
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
    protected double scale_prior;
    protected double shape_prior;
    protected double likelihood_prior;
    protected double[] samples;
    protected double[] cumulativeSamples;
    protected int cursor;
    protected final int SAMPLE_SIZE = 1500 ; //DistributionGenerator will automatically increase the size
    

    public enum DistributionFamily {

        LOGNORMAL, GAMMA, WEIBULL, NORMAL
    }

    /**
     *
     * @param dist
     * @param scale
     * @param shape
     */
    public DistributionGenerator(DistributionFamily dist, double scale, double shape) {
        this.dist = dist;
        this.scale = scale;
        this.shape = shape;
        this.scale_prior = scale;
        this.shape_prior = shape;
        RealDistribution distribution = getDistribution(scale, shape);
        samples = distribution.sample(SAMPLE_SIZE);
        updateCumulativeSamples();
        cursor = 0;
    }

    public DistributionGenerator(DistributionFamily dist, double scale, double shape, double a, double b, double c) {
        this(dist, scale, shape);
        this.scale_prior = b;
        this.shape_prior = a;
        this.likelihood_prior = c;
    }

    /**
     * Gets the sample data
     *
     * @return samples
     */
    public double[] getSamples() {
        return samples;
    }

    /**
     * Gets the cumulative Samples
     *
     * @return cumulativeSamples
     */
    public double[] getCumulativeSamples() {
        return cumulativeSamples;
    }

    /**
     * Extends the sample size
     */
    public void extendSamples() {
        double[] new_samples = getDistribution(scale, shape).sample(SAMPLE_SIZE);
        samples = concat(samples, new_samples);
        updateCumulativeSamples();
    }

    /**
     * Update cumulativeSamples from samples
     */
    public void updateCumulativeSamples() {
        cumulativeSamples = new double[samples.length];
        cumulativeSamples[0] = samples[0];
        for (int i = 1; i < samples.length; i++) {
            cumulativeSamples[i] = cumulativeSamples[i - 1] + samples[i];
        }
    }

    /**
     * Gets the Prior Knowledge based Estimation of samples
     *
     * @return delay
     */
    public double getPKEMean() {
        return this.shape_prior / this.scale_prior;
    }

    /**
     * Gets the average of samples
     *
     * @return average
     */
    public double getMean() {
        double sum = 0.0;
        for (int i = 0; i < cursor; i++) {
            sum += samples[i];
        }
        return sum / cursor;
    }

    /**
     * Gets the likelihood prior
     *
     * @return likelihood_prior
     */
    public double getLikelihoodPrior() {
        return this.likelihood_prior;
    }

    /**
     * Gets the Maximum Likelihood Estimation of samples based on the ftc paper
     *
     * @return the delay
     */
    public double getMLEMean() {
        double a = shape_prior, b = scale_prior;
        double sum = 0.0;

        for (int i = 0; i < cursor; i++) {
            switch (dist) {
                case GAMMA:
                    sum += samples[i];
                    break;
                case WEIBULL:
                    sum += Math.pow(samples[i], likelihood_prior);
                    break;
            }
        }
        double result = 0.0;
        switch (dist) {
            case GAMMA:
                result = (b + sum) / (a + cursor * likelihood_prior - 1);
                break;
            case WEIBULL:
                result = (b + sum) / (a + cursor + 1);
                break;
            default:
                break;
        }
        return result;
    }

    /**
     * Vary the distribution parameters but not the prior knowledge
     *
     * @param scale the first param
     * @param shape the second param
     */
    public void varyDistribution(double scale, double shape) {
        this.scale = scale;
        this.shape = shape;
        RealDistribution distribution = getDistribution(scale, shape);
        samples = distribution.sample(SAMPLE_SIZE);
        updateCumulativeSamples();
        //cursor = 0;
    }

    /**
     * Merge two arrays
     *
     * @param first first array
     * @param second second array
     * @return new array
     */
    public double[] concat(double[] first, double[] second) {
        double[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    /**
     * Gets the next sample from samples
     *
     * @return delay
     */
    public double getNextSample() {
        while (cursor >= samples.length) {
            double[] new_samples = getDistribution(scale, shape).sample(SAMPLE_SIZE);
            samples = concat(samples, new_samples);
            updateCumulativeSamples();
        }
        double delay = samples[cursor];
        cursor++;
        return delay;
    }

    /**
     * Gets the RealDistribution with two parameters
     *
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
                distribution = new GammaDistribution(shape, scale);
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
    
    /**
     * Gets the scale parameter
     * @return scale
     */
    public double getScale()
    {
        return this.scale;
    }
    
    /**
     * Gets the shape parameter
     * @return shape
     */
    public double getShape(){
        return this.shape;
    }
}
