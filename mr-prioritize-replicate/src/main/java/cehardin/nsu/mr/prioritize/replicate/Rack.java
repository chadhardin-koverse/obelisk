/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import java.util.Collection;
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
public class Rack {
	private Cluster cluster;
	private Set<Node> nodes;
	private Resource networkResource;

	public Set<Node> getNodes() {
		return nodes;
	}

	public void setNodes(Set<Node> nodes) {
		this.nodes = nodes;
	}

	public Resource getNetworkResource() {
		return networkResource;
	}

	public void setNetworkResource(Resource networkResource) {
		this.networkResource = networkResource;
	}
	
	public Set<DataBlock> getDataBlocks() {
		final Set<DataBlock> dataBlocks = new HashSet<DataBlock>();
		
		for(final Node node : getNodes()) {
			dataBlocks.addAll(node.getDataBlocks());
		}
		
		return dataBlocks;
	}
	
	public Map<DataBlock, Integer> getDataBlockReplicationCount() {
		final Map<DataBlock, Integer> dataBlockReplicationCount = new HashMap<DataBlock, Integer>();
		
		for(final Node node : getNodes()) {
			for(final DataBlock dataBlock : node.getDataBlocks()) {
				if(dataBlockReplicationCount.containsKey(dataBlock)) {
					final int count = dataBlockReplicationCount.get(dataBlock);
					final int newCount = count + 1;
					dataBlockReplicationCount.put(dataBlock, newCount);
				}
				else {
					dataBlockReplicationCount.put(dataBlock, 1);
				}
			}
		}
		
		return dataBlockReplicationCount;
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
	
	public Set<Node> findNodesOfDataBlock(DataBlock dataBlock) {
		Set<Node> found = new HashSet<Node>();
		
		for(final Node node : nodes) {
			if(node.getDataBlocks().contains(dataBlock)) {
				found.add(node);
			}
		}
		
		return found;
	}
}
