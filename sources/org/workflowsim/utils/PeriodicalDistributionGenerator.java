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
package org.workflowsim.utils;

import org.apache.commons.math3.distribution.RealDistribution;

/**
 *  This is a PeriodicalDistributionGenrator.
 *
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 11, 2014
 * @author Weiwei Chen
 */
public class PeriodicalDistributionGenerator extends DistributionGenerator{
    
    /**
     * The periodical signal stored 
     */
    protected PeriodicalSignal signal;
    
    /**
     * Initialize a Class
     * @param dist distribution
     * @param scale scale parameter
     * @param shape shape parameter
     * @param signal Periodical signal
     */
    public PeriodicalDistributionGenerator(DistributionFamily dist, double scale, double shape, PeriodicalSignal signal){
        super(dist, scale, shape);
        this.signal = signal;
        //generate samples periodically
        double currentTime = 0.0;
        samples = generatePeriodicalSamples(currentTime);
                
        updateCumulativeSamples();
        cursor = 0;
       
    }
    
    /**
     * Initialize a Class
     * 
     * @param dist distribution
     * @param scale scale parameter
     * @param shape shape parameter
     * @param a prior knowledge 
     * @param b prior knowledge
     * @param c prior knowledge
     * @param signal periodical signal
     */
    public PeriodicalDistributionGenerator(DistributionFamily dist, double scale, double shape, double a, double b, double c, PeriodicalSignal signal){
        super(dist, scale, shape, a, b, c);
        this.signal = signal;
        double currentTime = 0.0;
        samples = generatePeriodicalSamples(currentTime);
        updateCumulativeSamples();
        cursor = 0;
    }
    /**
     * Extends the sample size
     */
    @Override
    public void extendSamples() {
        double currentTime = cumulativeSamples[cumulativeSamples.length - 1];
        double[] new_samples = generatePeriodicalSamples(currentTime);
        samples = concat(samples, new_samples);
        updateCumulativeSamples();
    }
    
    /**
     * Generates a periodical sample
     * @return samples
     */
    private double[] generatePeriodicalSamples(double currentTime){
        RealDistribution distribution_upper = getDistribution(signal.getUpperBound(), shape);
        RealDistribution distribution_lower = getDistribution(signal.getLowerBound(), shape);
        RealDistribution distribution;
        double[] periodicalSamples = new double[SAMPLE_SIZE];
        boolean direction = signal.getDirection();
        for(int i = 0; i < SAMPLE_SIZE; i ++){
            if(currentTime % signal.getPeriod() < signal.getPeriod() * signal.getPortion()){
                if(direction){
                    distribution = distribution_upper;
                }else{
                    distribution = distribution_lower;
                }
            }else{
                if(direction){
                    distribution = distribution_lower;
                }else{
                    distribution = distribution_upper;
                }
            }
            periodicalSamples[i] = distribution.sample();
            currentTime += periodicalSamples[i];
            
        }
        return periodicalSamples;
    }
}
