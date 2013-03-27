/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.hardware;

import cehardin.nsu.mr.prioritize.replicate.DataBlock;
import cehardin.nsu.mr.prioritize.replicate.Resource;
import cehardin.nsu.mr.prioritize.replicate.id.RackId;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;

/**
 *
 * @author Chad
 */
public class Rack extends AbstractHardware<RackId> {
	private final Set<Node> nodes;
	private final Resource networkResource;

	public Rack(Set<Node> nodes, Resource networkResource, RackId id) {
		super(id);
		this.nodes = nodes;
		this.networkResource = networkResource;
	}

	
	
	public Set<Node> getNodes() {
		return nodes;
	}
	
	public Map<NodeId, Node> getNodeMap() {
		return Collections.unmodifiableMap(Maps.uniqueIndex(nodes, new Function<Node, NodeId>() {

			public NodeId apply(Node node) {
				return node.getId();
			}
		}));
	}

	public Resource getNetworkResource() {
		return networkResource;
	}
	
	public Set<DataBlock> getDataBlocks() {
		final Set<DataBlock> dataBlocks = new HashSet<DataBlock>();
		
		for(final Node node : getNodes()) {
			dataBlocks.addAll(node.getDataBlocks());
		}
		
		return dataBlocks;
	}
	
	public Map<DataBlockId, Set<DataBlock>> getDataBlocksById() {
		final Map<DataBlockId, Set<DataBlock>> blocksById = Maps.newHashMap();
		
		for(final Node node : getNodes()) {
			for(final Map.Entry<DataBlockId, DataBlock> nodeBlocksById : node.getDataBlockById().entrySet()) {
				final DataBlockId id = nodeBlocksById.getKey();
				final DataBlock dataBlock = nodeBlocksById.getValue();
				
				if(!blocksById.containsKey(id)) {
					blocksById.put(id, new HashSet<DataBlock>());	
				}
				
				blocksById.get(id).add(dataBlock);
			}
		}
		
		return Collections.unmodifiableMap(blocksById);
	}
	
	public Map<DataBlockId, Integer> getDataBlockReplicationCount() {
		final Map<DataBlockId, Integer> result = Maps.newHashMap();
		
		for(final Node node : getNodes()) {
			for(final DataBlock dataBlock : node.getDataBlocks()) {
				final DataBlockId dataBlockId = dataBlock.getId();
				
				if(result.containsKey(dataBlockId)) {
					final int count = result.get(dataBlockId);
					result.put(dataBlockId, count + 1);
				}
				else {
					result.put(dataBlockId, 1);
				}
			}
		}
		
		return Collections.unmodifiableMap(result);
	}
	
	public SortedMap<Integer, Set<DataBlockId>> getReplicationCounts() {
		final SortedMap<Integer, Set<DataBlockId>> result = Maps.newTreeMap();
		
		for(final Map.Entry<DataBlockId, Integer> entry : getDataBlockReplicationCount().entrySet()) {
			final DataBlockId dataBlockId = entry.getKey();
			final Integer count = entry.getValue();
			
			if(!result.containsKey(count)) {
				result.put(count, new HashSet<DataBlockId>());
			}
			
			result.get(count).add(dataBlockId);
		}
		
		return Collections.unmodifiableSortedMap(result);
	}
	
	
	public Node pickRandomNode() {
		final Random random = new Random();
		final int offset = random.nextInt(getNodes().size());
		final Iterator<Node> nodeIterator = getNodes().iterator();
		
		for(int i=0; i < offset; i++) {
			nodeIterator.next();
		}
		
		return nodeIterator.next();
	}
	
	public Node pickRandomNodeNot(Node node) {
		Node randomNode;
		do {
			randomNode = pickRandomNode();
		} while(randomNode == node);
		
		return randomNode;
			
	}
	
	public Set<Node> findNodesOfDataBlockId(final DataBlockId dataBlockId) {
		return Collections.unmodifiableSet(
			Sets.filter(getNodes(), new Predicate<Node>() {

			public boolean apply(final Node node) {
				return node.getDataBlockById().containsKey(dataBlockId);
			}
		}));
	}
}
