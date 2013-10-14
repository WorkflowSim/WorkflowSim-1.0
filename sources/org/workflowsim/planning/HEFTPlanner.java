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
 * @date Oct 12, 2013
 */
public class HEFTPlanner extends BasePlanner {
	private Map<Task, Map<CondorVM, Double>> computationCosts;
	private Map<Task, Map<Task, Double>> transferCosts;
	private Map<Task, Double> rank;

	private Map<CondorVM, List<Event>> schedules;
	private Map<Task, Double> earliestFinishTimes;

	private static final int INPUT = 1;
	private static final int OUTPUT = 2;
	private double averageBandwidth;

	private class Event {
		public double start;
		public double finish;

		public Event(double start, double finish) {
			this.start = start;
			this.finish = finish;
		}
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

	public HEFTPlanner() {
		computationCosts = new HashMap<>();
		transferCosts = new HashMap<>();
		rank = new HashMap<>();
		earliestFinishTimes = new HashMap<>();
		schedules = new HashMap<>();
	}

	/**
	 * The main function
	 */
	@Override
	public void run() {
		Log.printLine("HEFT planner running with " + getTaskList().size()
				+ " tasks.");

		averageBandwidth = calculateAverageBandwidth();

		for (Object vmObject : getVmList()) {
			CondorVM vm = (CondorVM) vmObject;
			schedules.put(vm, new ArrayList<Event>());
		}

		// Prioritization phase
		calculateComputationCosts();
		calculateTransferCosts();
		calculateRanks();

		// Selection phase
		allocateTasks();
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

	/**
	 * Populates the computationCosts field with the time in seconds to compute
	 * a task in a vm.
	 */
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

	/**
	 * Populates the transferCosts map with the time in seconds to transfer all
	 * files from each parent to each child
	 */
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
	 * Accounts the time in seconds necessary to transfer all files described
	 * between parent and child
	 * 
	 * @param parent
	 * @param child
	 * @return Transfer cost in seconds
	 */
	private double calculateTransferCost(Task parent, Task child) {
		List<File> parentFiles = (List<File>) parent.getFileList();
		List<File> childFiles = (List<File>) child.getFileList();

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
	 * Invokes calculateRank for each task to be scheduled
	 */
	private void calculateRanks() {
		for (Object taskObject : getTaskList()) {
			Task task = (Task) taskObject;
			calculateRank(task);
		}
	}

	/**
	 * Populates rank.get(task) with the rank of task as defined in the HEFT
	 * paper.
	 * 
	 * @param task
	 *            The task have the rank calculates
	 * @return The rank
	 */
	private double calculateRank(Task task) {
		if (rank.containsKey(task))
			return rank.get(task);

		double averageComputationCost = 0.0;

		for (Double cost : computationCosts.get(task).values())
			averageComputationCost += cost;

		averageComputationCost /= computationCosts.get(task).size();

		double max = 0.0;
		for (Task child : task.getChildList()) {
			double childCost = transferCosts.get(task).get(child)
					+ calculateRank(child);
			max = Math.max(max, childCost);
		}

		rank.put(task, averageComputationCost + max);

		return rank.get(task);
	}

	/**
	 * Allocates all tasks to be scheduled in non-ascending order of schedule.
	 */
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

	/**
	 * Schedules the task given in one of the VMs minimizing the earliest finish
	 * time
	 * 
	 * @param task
	 *            The task to be scheduled
	 * @pre All parent tasks are already scheduled
	 */
	private void allocateTask(Task task) {
		CondorVM chosenVM = null;
		double earliestFinishTime = Double.MAX_VALUE;
		double bestReadyTime = 0.0;
		double finishTime;

		for (Object vmObject : getVmList()) {
			CondorVM vm = (CondorVM) vmObject;
			double minReadyTime = 0.0;

			for (Task parent : task.getParentList()) {
				double readyTime = earliestFinishTimes.get(parent);
				if (parent.getVmId() != vm.getId())
					readyTime += transferCosts.get(parent).get(task);

				minReadyTime = Math.max(minReadyTime, readyTime);
			}

			finishTime = findFinishTime(task, vm, minReadyTime);

			if (finishTime < earliestFinishTime) {
				bestReadyTime = minReadyTime;
				earliestFinishTime = finishTime;
				chosenVM = vm;
			}
		}
		occupySlot(task, chosenVM, bestReadyTime);
		earliestFinishTimes.put(task, earliestFinishTime);

		task.setVmId(chosenVM.getId());
	}

	/**
	 * Determines the earliest finish time of a task if scheduled in the given
	 * vm with the constraint of not scheduling it before readyTime
	 * 
	 * @param task
	 *            The task to have its finish time calculated
	 * @param vm
	 *            The chosen vm to schedule
	 * @param readyTime
	 *            The minimum moment in seconds when the task may be scheduled
	 * @return The finish time in seconds
	 */
	private double findFinishTime(Task task, CondorVM vm, double readyTime) {
		List<Event> sched = schedules.get(vm);
		double computationCost = computationCosts.get(task).get(vm);

		if (sched.size() == 0)
			return readyTime + computationCost;

		if (sched.size() == 1) {
			if (readyTime >= sched.get(0).finish)
				return readyTime + computationCost;
			else if (readyTime + computationCost <= sched.get(0).start)
				return readyTime + computationCost;
			else
				return sched.get(0).finish + computationCost;
		}

		// Trivial case: Start after the latest task scheduled
		double start = Math.max(readyTime, sched.get(sched.size() - 1).finish);
		double finish = start + computationCost;
		int i = sched.size() - 1, j = sched.size() - 2;
		while (j >= 0) {
			Event current = sched.get(i);
			Event previous = sched.get(j);

			if (readyTime > previous.finish) {
				if (readyTime + computationCost <= current.start) {
					start = readyTime;
					finish = readyTime + computationCost;
				}

				break;
			}

			if (previous.finish + computationCost <= current.start) {
				start = previous.finish;
				finish = previous.finish + computationCost;
			}

			i--;
			j--;
		}

		if (readyTime + computationCost <= sched.get(0).start)
			return readyTime + computationCost;

		return finish;
	}

	/**
	 * Reserves the best time slot available to minimize the finish time of the
	 * given task in the vm with the constraint of not scheduling it before
	 * readyTime.
	 * 
	 * @param task
	 *            The task to have the time slot reserved
	 * @param vm
	 *            The vm that will execute the task
	 * @param readyTime
	 */
	private void occupySlot(Task task, CondorVM vm, double readyTime) {
		List<Event> sched = schedules.get(vm);
		double computationCost = computationCosts.get(task).get(vm);

		if (sched.size() == 0) {
			sched.add(new Event(readyTime, readyTime + computationCost));
			return;
		}

		if (sched.size() == 1) {
			if (readyTime >= sched.get(0).finish)
				sched.add(1, new Event(readyTime, readyTime + computationCost));
			else if (readyTime + computationCost <= sched.get(0).start)
				sched.add(0, new Event(readyTime, readyTime + computationCost));
			else
				sched.add(1, new Event(sched.get(0).finish, sched.get(0).finish
						+ computationCost));

			return;
		}

		// Trivial case: Start after the latest task scheduled
		double start = Math.max(readyTime, sched.get(sched.size() - 1).finish);
		double finish = start + computationCost;
		int i = sched.size() - 1, j = sched.size() - 2, pos = i + 1;
		while (j >= 0) {
			Event current = sched.get(i);
			Event previous = sched.get(j);

			if (readyTime > previous.finish) {
				if (readyTime + computationCost <= current.start) {
					start = readyTime;
					finish = readyTime + computationCost;
				}

				break;
			}

			if (previous.finish + computationCost <= current.start) {
				start = previous.finish;
				finish = previous.finish + computationCost;
				pos = i;
			}

			i--;
			j--;
		}

		if (readyTime + computationCost <= sched.get(0).start) {
			sched.add(0, new Event(readyTime, readyTime + computationCost));
			return;
		}
		sched.add(pos, new Event(start, finish));
	}

}
