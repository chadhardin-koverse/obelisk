/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.hardware;

import cehardin.nsu.mr.prioritize.replicate.DataBlock;
import cehardin.nsu.mr.prioritize.replicate.Resource;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Chad
 */
public class Node extends AbstractHardware<NodeId> {
	private final Resource diskResource;
	private final Set<DataBlock> dataBlocks;

	public Node(NodeId id, Resource diskResource, Set<DataBlock> dataBlocks) {
		super(id);
		this.diskResource = diskResource;
		this.dataBlocks = dataBlocks;
	}

	public Resource getDiskResource() {
		return diskResource;
	}

	public Set<DataBlock> getDataBlocks() {
		return dataBlocks;
	}
	
	public Map<DataBlockId, Set<DataBlock>> getDataBlocksById() {
		final Map<DataBlockId, Set<DataBlock>> blocksById = Maps.newHashMap();
		
		for(final DataBlock dataBlock : getDataBlocks()) {
			final DataBlockId id = dataBlock.getId();
			
			if(!blocksById.containsKey(id)) {
				blocksById.put(id, new HashSet<DataBlock>());
			}
			
			blocksById.get(id).add(dataBlock);
		}
		
		return Collections.unmodifiableMap(blocksById);
	}
}
