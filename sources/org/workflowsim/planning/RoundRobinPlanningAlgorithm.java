package org.workflowsim.planning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.workflowsim.CondorVM;
import org.workflowsim.Task;

public class RoundRobinPlanningAlgorithm extends BasePlanningAlgorithm {

	private List<Task> topologicalOrder;

	private int depth;

	private class TaskComparator implements Comparator<Task> {

		@Override
		public int compare(Task a, Task b) {
			if (a.getDepth() < b.getDepth())
				return -1;
			else if (a.getDepth() > b.getDepth())
				return 1;
			return 0;
		}
	}

	public RoundRobinPlanningAlgorithm() {
		topologicalOrder = new ArrayList<Task>();
		depth = 0;
	}

	private void topologicalOrder() {

		ArrayList<Task> removeNodes = new ArrayList<Task>();

		while (!getTaskList().isEmpty()) {
			/*
			 * search for "leafs" (nodes with out degree 0)
			 */
			for (Object o : getTaskList()) {
				Task t = (Task) o;
				/*
				 * if it's a leaf, then it has no dependency
				 */
				if (t.getDepth() == depth)
					removeNodes.add(t);
			}

			/*
			 * Increment depth
			 */
			depth++;

			/*
			 * sort leafs on same level
			 */
			Collections.sort(removeNodes, new TaskComparator());

			/*
			 * remove leafs from node list
			 */
			for (Task t : removeNodes) {
				getTaskList().remove(t);

				/*
				 * and add it to sortedNodes
				 */
				topologicalOrder.add(t);
			}

			removeNodes.clear();

		}

	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() throws Exception {
		topologicalOrder();

		int currentHost = 0;

		/*
		 * for each task
		 */
		while (topologicalOrder.size() > 0) {
			Task task = topologicalOrder.get(0);
			/*
			 * round-robin host selected
			 */
			CondorVM vm = (CondorVM) getVmList().get(currentHost);
			currentHost = (currentHost + 1) % getVmList().size();

			task.setVmId(vm.getId());
			topologicalOrder.remove(0);
			getTaskList().add(task);
		}

	}

}
