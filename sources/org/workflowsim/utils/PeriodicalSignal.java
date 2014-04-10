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

/**
 * This class simulates a dynamic failure signal that changes periodically
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2014
 */
public class PeriodicalSignal {

    /**
     * The period of the signal (seconds)
     */
    private double period;
    /**
     * The upper bound of the signal
     */
    private double upperbound;
    /**
     * The lower bound of the signal
     */
    private double lowerbound;
    /**
     * The portion of upperbound (0<=portion<=1.0)
     */
    private double portion;
    /**
     * if direction is true (default), the initial is upperbound; otherwise it
     * is lowerbound.
     */
    private boolean direction;

    /**
     * Initialize the class
     *
     * @param period
     * @param upperbound
     * @param lowerbound
     * @param portion
     * @param direction
     */
    public PeriodicalSignal(double period, double upperbound, double lowerbound, double portion,
            boolean direction) {
        this.lowerbound = lowerbound;
        this.upperbound = upperbound;
        this.period = period;
        this.portion = portion;
        this.direction = direction;
    }

    /**
     * Initialize the class
     *
     * @param period
     * @param upperbound
     * @param lowerbound
     * @param portion
     */
    public PeriodicalSignal(double period, double upperbound, double lowerbound, double portion) {
        this(period, upperbound, lowerbound, portion, true);
    }

    /**
     * Initialize the class
     *
     * @param period
     * @param upperbound
     * @param lowerbound
     */
    public PeriodicalSignal(double period, double upperbound, double lowerbound) {
        this(period, upperbound, lowerbound, 0.5);
    }

    /**
     * Gets the signal at the currentTime
     *
     * @param currentTime
     * @return the signal (either lowerbound or upperbound)
     */
    public double getCurrentSignal(double currentTime) {
        if (currentTime < 0.0) {
            return 0.0;
        }
        currentTime = currentTime % period;
        if (currentTime <= period * portion) {
            if (direction) {
                return upperbound;
            } else {
                return lowerbound;
            }
        } else {
            if (direction) {
                return lowerbound;
            } else {
                return upperbound;
            }
        }
    }

    /**
     * Gets the upper bound
     *
     * @return upperbound
     */
    public double getUpperBound() {
        return upperbound;
    }

    /**
     * Gets the lower bound
     *
     * @return lowerbound
     */
    public double getLowerBound() {
        return lowerbound;
    }

    /**
     * Gets the period
     *
     * @return period
     */
    public double getPeriod() {
        return period;
    }

    /**
     * Gets the portion
     *
     * @return portion
     */
    public double getPortion() {
        return portion;
    }
    
    /**
     * Gets the direction
     * 
     * @return direction
     */
    public boolean getDirection(){
        return direction;
    }
}
