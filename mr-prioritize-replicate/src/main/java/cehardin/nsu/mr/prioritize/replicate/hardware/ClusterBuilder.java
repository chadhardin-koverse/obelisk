/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.hardware;

import cehardin.nsu.mr.prioritize.replicate.DataBlock;
import cehardin.nsu.mr.prioritize.replicate.Resource;
import cehardin.nsu.mr.prioritize.replicate.Variables;
import cehardin.nsu.mr.prioritize.replicate.id.RackId;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Chad
 */
public class ClusterBuilder {

    public Cluster buildCluster(Variables variables) {
	final Set<Rack> racks = Sets.newHashSet();
	final Map<NodeId, Set<DataBlockId>> nodeIdToDataBlockIds = Maps.newHashMap();
	final Map<RackId, Set<NodeId>> rackIdtoNodeIds = Maps.newHashMap();
	
	for(final DataBlockId dataBlockId : variables.getDataBlockIds()) {
		for(final NodeId nodeId : variables.getDataBlockIdToNodeIds().apply(dataBlockId)) {
			if(!nodeIdToDataBlockIds.containsKey(nodeId)) {
				nodeIdToDataBlockIds.put(nodeId, new HashSet<DataBlockId>());
			}
			
			nodeIdToDataBlockIds.get(nodeId).add(dataBlockId);
		}
	}
	
	for(final NodeId nodeId : variables.getNodeIds()) {
		final RackId rackId = variables.getNodeIdToRackId().apply(nodeId);
		if(!rackIdtoNodeIds.containsKey(rackId)) {
			rackIdtoNodeIds.put(rackId, new HashSet<NodeId>());
		}
		
		rackIdtoNodeIds.get(rackId).add(nodeId);
	}
	
	for(final Map.Entry<RackId, Set<NodeId>> entry : rackIdtoNodeIds.entrySet()) {
		final RackId rackId = entry.getKey();;
		final Set<Node> nodes = new HashSet<Node>();
		
		for(final NodeId nodeId : entry.getValue()) {
			final Set<DataBlock> dataBlocks = Sets.newHashSet();
			
			for(final DataBlockId dataBlockId : nodeIdToDataBlockIds.get(nodeId)) {
				dataBlocks.add(new DataBlock(dataBlockId, variables.getBlockSize()));
			}
			nodes.add(new Node(nodeId, new Resource(variables.getDiskBandwidth().getBytesPerMs()), dataBlocks));
		}
		
		racks.add(new Rack(nodes, new Resource(variables.getRackBandwidth().getBytesPerMs()), rackId));
	}
	
	return new Cluster(racks, new Resource(variables.getClusterBandwidth().getBytesPerMs()));
    }
}
