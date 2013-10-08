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
package org.workflowsim.planning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.CondorVM;
import org.workflowsim.Task;

/**
 * The HEFT planning algorithm.
 * 
 * @author Pedro Paulo Vezzá Campos
 * @date Aug 16, 2013
 */
public class HEFTPlanner extends BasePlanner {
	private class Event {
		public double start;
		public double length;

		public Event(double start, double length) {
			this.start = start;
			this.length = length;
		}
	}

	private Map<Task, Map<CondorVM, Double>> computationCosts;
	private Map<Task, Map<Task, Double>> transferCosts;
	private Map<Task, Double> rank;

	private Map<CondorVM, List<Event>> schedules;

	private static final int INPUT = 1;
	private static final int OUTPUT = 2;
	private double averageBandwidth;

	public HEFTPlanner() {
		computationCosts = new HashMap<>();
		transferCosts = new HashMap<>();
		rank = new HashMap<>();
	}

	/**
	 * The main function
	 */
	@Override
	public void run() {
		Log.printLine("HEFT planner running with " + getTaskList().size()
				+ " tasks.");

		averageBandwidth = calculateAverageBandwidth();

		// Prioritization phase
		calculateComputationCosts();
		calculateTransferCosts();
		calculateRanks();

		// Selection phase
		allocateTasks();
	}

	private class TaskRank implements Comparable<TaskRank> {
		public Task task;
		public Double rank;

		public TaskRank(Task task, Double rank) {
			this.task = task;
			this.rank = rank;
		}

		@Override
		public int compareTo(TaskRank o) {
			return o.rank.compareTo(rank);
		}
	}

	private void allocateTasks() {
		List<TaskRank> taskRank = new ArrayList<>();
		for (Task task : rank.keySet()) {
			taskRank.add(new TaskRank(task, rank.get(task)));
		}

		// Sorting in non-ascending order of rank
		Collections.sort(taskRank);
		for (TaskRank tr : taskRank) {
			allocateTask(tr.task);
		}

	}

	private void allocateTask(Task task) {
		double earliestFinishTime = Double.MAX_VALUE;

		for (Object vmObject : getVmList()) {
			CondorVM vm = (CondorVM) vmObject;
			double transferTime = 0.0;
			for (Task parent : task.getParentList()) {
				if (parent.getVmId() == vm.getId())
					continue; // We do not need to count transfers inside the
								// same machine
				transferTime += transferCosts.get(parent).get(task);
			}

		}
		// task.setVmId(vm.getId());
	}

	private void calculateRanks() {
		for (Object taskObject : getTaskList()) {
			Task task = (Task) taskObject;
			calculateRank(task);
		}
	}

	private double calculateRank(Task task) {
		if (rank.containsKey(task))
			return rank.get(task);

		double averageComputationCost = calculateAverage(computationCosts
				.get(task));
		double max = 0.0;
		for (Task child : task.getChildList()) {
			double childCost = transferCosts.get(task).get(child)
					+ calculateRank(child);
			max = Math.max(max, childCost);
		}

		rank.put(task, averageComputationCost + max);

		return rank.get(task);
	}

	private double calculateAverage(Map<?, Double> map) {
		double acc = 0.0;

		for (Double cost : map.values())
			acc += cost;

		return acc / map.size();
	}

	private void calculateTransferCosts() {
		// Initializing the matrix
		for (Object taskObject1 : getTaskList()) {
			Task task1 = (Task) taskObject1;
			Map<Task, Double> taskTransferCosts = new HashMap<Task, Double>();

			for (Object taskObject2 : getTaskList()) {
				Task task2 = (Task) taskObject2;
				taskTransferCosts.put(task2, 0.0);
			}

			transferCosts.put(task1, taskTransferCosts);
		}

		// Calculating the actual values
		for (Object parentObject : getTaskList()) {
			Task parent = (Task) parentObject;
			for (Task child : parent.getChildList()) {
				transferCosts.get(parent).put(child,
						calculateTransferCost(parent, child));
			}
		}
	}

	/**
	 * 
	 * @param parent
	 * @param child
	 * @return Transfer costs in seconds
	 */
	private double calculateTransferCost(Task parent, Task child) {
		List<File> parentFiles = parent.getFileList();
		List<File> childFiles = child.getFileList();

		double acc = 0.0;

		for (File parentFile : parentFiles) {
			if (parentFile.getType() != OUTPUT)
				continue;

			for (File childFile : childFiles) {
				if (childFile.getType() == INPUT
						&& childFile.getName().equals(parentFile.getName())) {
					acc += childFile.getSize();
					break;
				}
			}
		}

		// acc in MB, averageBandwidth in Mb/s
		return acc * 8 / averageBandwidth;
	}

	/**
	 * Calculates the average available bandwidth among all VMs in Mbit/s
	 * 
	 * @return Average available bandwidth in Mbit/s
	 */
	private double calculateAverageBandwidth() {
		double avg = 0.0;
		for (Object vmObject : getVmList()) {
			CondorVM vm = (CondorVM) vmObject;
			avg += vm.getBw();
		}
		return avg / getVmList().size();
	}

	private void calculateComputationCosts() {
		for (Object taskObject : getTaskList()) {
			Task task = (Task) taskObject;

			Map<CondorVM, Double> costsVm = new HashMap<CondorVM, Double>();

			for (Object vmObject : getVmList()) {
				CondorVM vm = (CondorVM) vmObject;
				if (vm.getNumberOfPes() < task.getNumberOfPes()) {
					costsVm.put(vm, Double.MAX_VALUE);
				} else {
					costsVm.put(vm,
							task.getCloudletTotalLength() / vm.getMips());
				}
			}
			computationCosts.put(task, costsVm);
		}
	}
}
