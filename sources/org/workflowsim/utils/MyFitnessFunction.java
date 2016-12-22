package org.workflowsim.utils;

import java.util.List;
import java.util.Map;

import net.sourceforge.jswarm_pso.FitnessFunction;

import org.cloudbus.cloudsim.Vm;
import org.workflowsim.CondorVM;
import org.workflowsim.Task;

public class MyFitnessFunction extends FitnessFunction {

	private Map<Task, Map<CondorVM, Double>> TP;
	private Map<CondorVM, Map<CondorVM, Double>> PP;
	private Map<Task, Map<Task, Double>> e;
	private List<Task> currentTasks;
	private List<? extends Vm> vmList;

	public MyFitnessFunction(boolean maximize,
			Map<Task, Map<CondorVM, Double>> TP,
			Map<CondorVM, Map<CondorVM, Double>> PP,
			Map<Task, Map<Task, Double>> e, List<? extends Vm> vmList,
			List<Task> tasks) {
		super(maximize);
		this.TP = TP;
		this.PP = PP;
		this.e = e;
		this.vmList = vmList;
		this.currentTasks = tasks;
	}

	/**
	 * Evaluates a particles at a given position
	 */
	@Override
	public double evaluate(double[] position) {
		return getCostMaximization(position);
	}

	/**
	 * Equation 4 of the paper
	 */
	private double getCostMaximization(double[] position) {
		double maxCost = Double.MIN_VALUE;

		for (Object vmObject : vmList) {
			CondorVM vm = (CondorVM) vmObject;
			double currentCost = getTotalCost(position, vm);
			if (currentCost > maxCost)
				maxCost = currentCost;
		}
		return maxCost;
	}

	/**
	 * Equation 3 of the paper
	 */
	private double getTotalCost(double[] position, CondorVM vm) {
		return (getTrasmissionCost(position, vm) + getComputationCost(position,
				vm));
	}

	/**
	 * Equation 2 of the paper
	 */
	private double getTrasmissionCost(double[] position, CondorVM vm) {
		double totalTransferCost = 0.0;

		for (int d1 = 0; d1 < position.length; d1++) {
			Task t1 = this.currentTasks.get(d1);
			if ((int)Math.round(position[d1]) == vm.getId()) {
				for (int d2 = 0; d2 < position.length; d2++) {
					Task t2 = this.currentTasks.get(d2);
					if ((int)Math.round(position[d2]) != vm.getId()
							&& t2.getParentList().contains(t1)
							&& t1.getChildList().contains(t2)) {
						totalTransferCost += this.PP.get(vm).get(
								vmList.get((int)Math.round(position[d2])))
								* this.e.get(t1).get(t2);
					}
				}
			}

		}

		return totalTransferCost;
	}

	/**
	 * Equation 1 of the paper
	 */
	private double getComputationCost(double[] position, CondorVM vm) {
		double totalComputationCost = 0.0;

		for (int dimension = 0; dimension < position.length; dimension++)
			if (vm.getId() == (int) Math.round(position[dimension]))
				totalComputationCost += TP.get(currentTasks.get(dimension))
						.get(vmList.get((int) Math.round(position[dimension])));
		return totalComputationCost;
	}

}