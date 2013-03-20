/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.task;

import cehardin.nsu.mr.prioritize.replicate.DataBlock;
import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.hardware.Node;
import cehardin.nsu.mr.prioritize.replicate.hardware.Rack;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Atomics;
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/**
 *
 * @author Chad
 */
public class CheckReplicationTask implements Task {

    private final Runnable callback;
    private final Cluster cluster;
    private final Executor executor;

    public CheckReplicationTask(Runnable callback, Cluster cluster, Executor executor) {
	this.callback = callback;
	this.cluster = cluster;
	this.executor = executor;
    }

    public void run() {
	final Set<DataBlockId> firstPriorityReplicateIntraRack = new HashSet<DataBlockId>();
	final Set<DataBlockId> secondPriorityReplicateIntraRack = new HashSet<DataBlockId>();
	final Set<DataBlockId> thirdPriorityReplicateInterRack = new HashSet<DataBlockId>();
	final List<ReplicateTask> replicateTasks = new ArrayList<ReplicateTask>();
	final CountDownLatch countDownLatch;
	final Runnable countdowner;

	for (final Map.Entry<DataBlockId, Integer> dataBlockCount : cluster.getDataBlockReplicationCount().entrySet()) {
	    final DataBlockId dataBlockId = dataBlockCount.getKey();
	    final int count = dataBlockCount.getValue();

	    if (count == 1) {
		firstPriorityReplicateIntraRack.add(dataBlockId);
		thirdPriorityReplicateInterRack.add(dataBlockId);
	    } else if (count == 2) {
		final Node node1 = Iterables.get(cluster.findNodesOfDataBlock(dataBlockId), 0);
		final Node node2 = Iterables.get(cluster.findNodesOfDataBlock(dataBlockId), 1);
		final Rack rack1 = cluster.findRackOfNode(node1);
		final Rack rack2 = cluster.findRackOfNode(node2);

		if (!rack1.equals(rack2)) {
		    secondPriorityReplicateIntraRack.add(dataBlockId);
		} else {
		    thirdPriorityReplicateInterRack.add(dataBlockId);
		}
	    }
	}

	countDownLatch = new CountDownLatch(
		firstPriorityReplicateIntraRack.size()
		+ secondPriorityReplicateIntraRack.size()
		+ thirdPriorityReplicateInterRack.size());

	countdowner = new Runnable() {
	    public void run() {
		countDownLatch.countDown();
	    }
	};

	for (final DataBlockId dataBlockId : firstPriorityReplicateIntraRack) {
	    final Set<Node> nodes = cluster.findNodesOfDataBlock(dataBlockId);
	    final Node fromNode = nodes.iterator().next();
	    final Rack rack = cluster.findRackOfNode(fromNode);
	    final Node toNode = rack.pickRandomNodeNot(fromNode);
	    final DataBlock dataBlock = fromNode.getDataBlocksById().get(dataBlockId).iterator().next();
	    final ReplicateTask replicateTask = new ReplicateTask(
		    dataBlock,
		    cluster,
		    rack,
		    rack,
		    fromNode,
		    toNode,
		    countdowner);

	    replicateTasks.add(replicateTask);
	}

	for (final DataBlockId dataBlockId : secondPriorityReplicateIntraRack) {
	    final Set<Node> nodes = cluster.findNodesOfDataBlock(dataBlockId);
	    final Node fromNode = nodes.iterator().next();
	    final Rack rack = cluster.findRackOfNode(fromNode);
	    final Node toNode = rack.pickRandomNodeNot(fromNode);
	    final DataBlock dataBlock = fromNode.getDataBlocksById().get(dataBlockId).iterator().next();
	    final ReplicateTask replicateTask = new ReplicateTask(
		    dataBlock,
		    cluster,
		    rack,
		    rack,
		    fromNode,
		    toNode,
		    countdowner);

	    replicateTasks.add(replicateTask);
	}

	for (final DataBlockId dataBlockId : thirdPriorityReplicateInterRack) {
	    final Set<Node> nodes = cluster.findNodesOfDataBlock(dataBlockId);
	    final Node fromNode = nodes.iterator().next();
	    final Rack fromRack = cluster.findRackOfNode(fromNode);
	    final Rack toRack = cluster.pickRandomNodeNot(fromRack);
	    final Node toNode = toRack.pickRandomNode();
	    final DataBlock dataBlock = fromNode.getDataBlocksById().get(dataBlockId).iterator().next();
	    final ReplicateTask replicateTask = new ReplicateTask(
		    dataBlock,
		    cluster,
		    fromRack,
		    toRack,
		    fromNode,
		    toNode,
		    countdowner);

	    replicateTasks.add(replicateTask);
	}

	for (final ReplicateTask replicateTask : replicateTasks) {
	    executor.execute(replicateTask);
	}

	Uninterruptibles.awaitUninterruptibly(countDownLatch);
	callback.run();
    }
}
