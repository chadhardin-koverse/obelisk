/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;

/**
 *
 * @author Chad
 */
public class CheckReplicationTask implements Task {

	private final Cluster cluster;
	private final ExecutorService executorService;

	public void run() {
		final Set<DataBlockId> firstPriorityReplicateIntraRack = new HashSet<DataBlockId>();
		final Set<DataBlockId> secondPriorityReplicateIntraRack = new HashSet<DataBlockId>();
		final Set<DataBlockId> thirdPriorityReplicateInterRack = new HashSet<DataBlockId>();

		for (final Map.Entry<DataBlockId, Integer> dataBlockCount : cluster.getDataBlockReplicationCount().entrySet()) {
			final DataBlockId dataBlock = dataBlockCount.getKey();
			final int count = dataBlockCount.getValue();

			if (count == 1) {
				firstPriorityReplicateIntraRack.add(dataBlock);
				thirdPriorityReplicateInterRack.add(dataBlock);
			} else if (count == 2) {
				final Node node1 = Iterables.get(cluster.findNodesOfDataBlock(dataBlock), 0);
				final Node node2 = Iterables.get(cluster.findNodesOfDataBlock(dataBlock), 1);
				final Rack rack1 = cluster.findRackOfNode(node1);
				final Rack rack2 = cluster.findRackOfNode(node2);

				if (rack1 != rack2) {
					secondPriorityReplicateIntraRack.add(dataBlock);
				} else {
					thirdPriorityReplicateInterRack.add(dataBlock);
				}
			}
		}


		for (final DataBlockId dataBlockId : firstPriorityReplicateIntraRack) {
			final Node fromNode = dataBlock.getNodes().iterator().next();
			final Node toNode = fromNode.getRack().pickRandomNodeNot(fromNode);
			final ReplicateTask replicateTask = new ReplicateTask(dataBlock, fromNode, toNode, null);

			executorService.execute(replicateTask);
		}

		for (final DataBlockId dataBlockId : secondPriorityReplicateIntraRack) {
			final Node fromNode = dataBlock.getNodes().iterator().next();
			final Node toNode = fromNode.getRack().pickRandomNodeNot(fromNode);
			final ReplicateTask replicateTask = new ReplicateTask(dataBlock, fromNode, toNode, null);

			executorService.execute(replicateTask);
		}

		for (final DataBlockId dataBlockId : thirdPriorityReplicateInterRack) {
			final Node fromNode = dataBlock.getNodes().iterator().next();
			final Rack fromRack = fromNode.getRack();
			final Rack toRack = cluster.pickRandomNodeNot(fromRack);
			final Node toNode = toRack.pickRandomNode();
			final ReplicateTask replicateTask = new ReplicateTask(dataBlock, fromNode, toNode, null);

			executorService.execute(replicateTask);
		}
	}
}
