/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

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
			for(final Map.Entry<DataBlockId, Set<DataBlock>> nodeBlocksById : node.getDataBlocksById().entrySet()) {
				final DataBlockId id = nodeBlocksById.getKey();
				final Set<DataBlock> dataBlocks = nodeBlocksById.getValue();
				
				if(blocksById.containsKey(id)) {
					blocksById.get(id).addAll(dataBlocks);
				}
				else {
					blocksById.put(id, dataBlocks);
				}
			}
		}
		
		return Collections.unmodifiableMap(blocksById);
	}
	
	public Map<DataBlockId, Integer> getDataBlockReplicationCount() {
		return Collections.unmodifiableMap(
			Maps.transformValues(getDataBlocksById(), new Function<Set<DataBlock>, Integer>() {

			public Integer apply(Set<DataBlock> dataBlocks) {
				return dataBlocks.size();
			}
			
		}));
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
				return node.getDataBlocksById().containsKey(dataBlockId);
			}
		}));
	}
}
