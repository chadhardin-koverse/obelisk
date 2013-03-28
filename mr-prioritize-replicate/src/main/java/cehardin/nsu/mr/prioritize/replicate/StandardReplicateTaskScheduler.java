/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.hardware.Node;
import cehardin.nsu.mr.prioritize.replicate.hardware.Rack;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 *
 * @author Chad
 */
public class StandardReplicateTaskScheduler implements ReplicateTaskScheduler {
	
	public StandardReplicateTaskScheduler() {
	}
	
	public List<ReplicateTask> schedule(Cluster cluster) {
		final List<ReplicateTask> tasks = Lists.newArrayList();

		for (final Map.Entry<Integer, Set<DataBlockId>> countEntry : cluster.getReplicationCounts().entrySet()) {
			final int count = countEntry.getKey();
			final Set<DataBlockId> datablockIds = countEntry.getValue();
			
			for (final DataBlockId dataBlockId : datablockIds) {
				final Set<Rack> racksContainingDataBlock = cluster.findRacksOfDataBlock(dataBlockId);
				final Rack fromRack = racksContainingDataBlock.iterator().next();
				final Node fromNode = fromRack.findNodesOfDataBlockId(dataBlockId).iterator().next();
				final DataBlock dataBlock = fromNode.getDataBlockById().get(dataBlockId);
				final Rack toRack;
				final Node toNode;
				
				if (count == 1) {
					toRack = fromRack;			
				}
				else {
					toRack = cluster.pickRandomNodeNot(fromRack);
				}
				
				toNode = toRack.pickRandomNode();
				
				tasks.add(new ReplicateTask(dataBlock, cluster, fromRack, toRack, fromNode, toNode, null));
			}
		}

		return Collections.unmodifiableList(tasks);
	}
}
