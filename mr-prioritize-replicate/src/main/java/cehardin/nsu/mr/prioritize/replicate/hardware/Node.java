/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.hardware;

import cehardin.nsu.mr.prioritize.replicate.DataBlock;
import cehardin.nsu.mr.prioritize.replicate.Resource;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import com.google.common.base.Objects;
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
	
	public Map<DataBlockId, DataBlock> getDataBlockById() {
		final Map<DataBlockId, DataBlock> blockById = Maps.newHashMap();
		
		for(final DataBlock dataBlock : getDataBlocks()) {
			final DataBlockId id = dataBlock.getId();
			
			blockById.put(id, dataBlock);
		}
		
		return Collections.unmodifiableMap(blockById);
	}
	
	@Override
	public String toString() {
	    return Objects.toStringHelper(getClass()).
		    add("dataBlocks", dataBlocks).
		    add("diskResource", diskResource).
		    toString();
	}
}
