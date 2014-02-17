package org.workflowsim.planning;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sourceforge.jswarm_pso.FitnessFunction;
import net.sourceforge.jswarm_pso.Particle;
import net.sourceforge.jswarm_pso.Swarm;

import org.cloudbus.cloudsim.File;
import org.workflowsim.CondorVM;
import org.workflowsim.Task;
import org.workflowsim.utils.Parameters;

public class PSOPlanningAlgorithm extends BasePlanningAlgorithm {

	private class MyParticle extends Particle {
		private List <Task> tasks;
		public MyParticle(List<Task> tasks) {
			super(tasks.size());
			this.tasks = tasks;
		}
	}

	private class MyFitnessFunction extends FitnessFunction {
		
		public MyFitnessFunction(boolean maximize) {
			super(maximize);
		}

		/**
		 * Evaluates a particles at a given position
		 * 
		 * @param position
		 *            : Particle's position
		 * @return Fitness function for a particle
		 */
		@Override
		public double evaluate(double[] position) {
			return  getCostMaximization(position);
		}
		
		/**
		 * Equation 4 of the paper
		 * 
		 * @param position
		 *            : Particle's position
		 * @return Maximum total cost execution of this particle
		 */
		private double  getCostMaximization(double[] position) {
			double maxMappingTotalCost = Double.MIN_VALUE;
			for (Vm vm : this.vms) {
				double totalMappingCost = getMappingTotalCost(position, vm);
				if (maxMappingTotalCost < totalMappingCost) {
					maxMappingTotalCost = totalMappingCost;
				}
			}
			return maxMappingTotalCost;
		}

	}

	private static final int MAX_ITERATION = 50;

	private Map<Task, Map<CondorVM, Double>> TP;

	private Map<CondorVM, Map<CondorVM, Double>> PP;

	private Map<Task, Map<Task, Double>> e;

	public PSOPlanningAlgorithm() {
		TP = new HashMap<Task, Map<CondorVM, Double>>();
		PP = new HashMap<CondorVM, Map<CondorVM, Double>>();
		e = new HashMap<Task, Map<Task, Double>>();
	}

	@Override
	public void run() throws Exception {
		schedulingHeuristic();
	}

	private void schedulingHeuristic() {
		/*
		 * Step 1: Calculate average computation cost of all tasks in all
		 * compute resources
		 */
		averageComputationCostAllTaskInAllResources();

		/*
		 * Step 2: Calculate average cost of communication between resources
		 */
		averageCommunicationCostBetweenResources();

		/*
		 * Step 3: Set task node weight w(k,j) as average computation cost. In
		 * fact, w(k,j) == TP[k][j], where k is a task and j is a resource
		 */

		/*
		 * Step 4: Set edge weight e(k1,k2) as size of all file transferred
		 * between tasks k1 and k2
		 */
		calculateEdgeWeight();

		/*
		 * Step 5: Compute PSO({ a set of all tasks }). Initialize the
		 * readyTasks vector with the list of all tasks
		 */

		/*
		 * Call PSO Algorithm
		 */
		double bestPosition[] = pso(getTaskList());

	}

	private void averageCommunicationCostBetweenResources() {
		double[][] bandwidths = Parameters.getBandwidths();
		for (Object vmObject : getVmList()) {
			CondorVM vm = (CondorVM) vmObject;

			Map<CondorVM, Double> costs = new HashMap<CondorVM, Double>();
			for (Object vmObject2 : getVmList()) {
				CondorVM vm2 = (CondorVM) vmObject2;

				if (vm.equals(vm2))
					costs.put(vm2, 0.0);
				else
					costs.put(vm2, vm2.getDataTransferOutPrice()
							/ bandwidths[vm.getId()][vm2.getId()]);
			}

			PP.put(vm, costs);
		}

	}

	private void averageComputationCostAllTaskInAllResources() {
		for (Object taskObject : getTaskList()) {

			Task task = (Task) taskObject;
			Map<CondorVM, Double> costsVm = new HashMap<CondorVM, Double>();

			for (Object vmObject : getVmList()) {
				CondorVM vm = (CondorVM) vmObject;
				if (vm.getNumberOfPes() < task.getNumberOfPes())
					costsVm.put(vm, Double.MAX_VALUE);
				else
					costsVm.put(
							vm,
							(task.getCloudletTotalLength() / vm.getMips())
									* vm.getPrice());
			}
			TP.put(task, costsVm);
		}
	}

	private void calculateEdgeWeight() {
		for (Object parentObject : getTaskList()) {
			Task parent = (Task) parentObject;
			Map<Task, Double> edge = new HashMap<Task, Double>();

			for (Task child : parent.getChildList()) {
				edge.put(child, calculateSizeOfAllFilesTransfer(parent, child));
			}
			e.put(parent, edge);
		}
	}

	/**
	 * Accounts the time in seconds necessary to transfer all files described
	 * between parent and child
	 * 
	 * @param parent
	 * @param child
	 * @return Transfer cost in seconds
	 */
	@SuppressWarnings("unchecked")
	private double calculateSizeOfAllFilesTransfer(Task parent, Task child) {
		List<File> parentFiles = (List<File>) parent.getFileList();
		List<File> childFiles = (List<File>) child.getFileList();

		double sum = 0.0;

		for (File parentFile : parentFiles) {
			if (parentFile.getType() != Parameters.FileType.OUTPUT.value) {
				continue;
			}

			for (File childFile : childFiles) {
				if (childFile.getType() == Parameters.FileType.INPUT.value
						&& childFile.getName().equals(parentFile.getName())) {
					sum += childFile.getSize();
					break;
				}
			}
		}

		return sum;
	}

	/**
	 * The PSO_Algorithm function: it receives a list with all tasks
	 * 
	 * @param readyTasks
	 * 
	 * @param dag
	 */
	private double[] pso(List<Task> readyTasks) {
		/*
		 * Step 1: Set particle dimension as equal to the size of ready tasks
		 * list
		 */
		int dimension = readyTasks.size();

		if (dimension == 0)
			return null;

		/*
		 * Step 2: Initialize particles position and velocity randomly
		 */
		Swarm swarm = new Swarm(dimension,
				new MyParticle(readyTasks), new MyFitnessFunction(false));

		swarm.setMaxPosition(getVmList().size() - 1);
		swarm.setMinPosition(0);

		for (int i = 0; i < MAX_ITERATION; i++)
			swarm.evolve();

		return swarm.getBestPosition();
	}

}
