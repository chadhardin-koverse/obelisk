/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

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
public class Node extends AbstractHardware {
	private final Resource diskResource;
	private final Set<DataBlock> dataBlocks;

	public Node(String name, Resource diskResource, Set<DataBlock> dataBlocks) {
		super(name);
		this.diskResource = diskResource;
		this.dataBlocks = dataBlocks;
	}


	
	public Resource getDiskResource() {
		return diskResource;
	}

	public Set<DataBlock> getDataBlocks() {
		return dataBlocks;
	}
	
	public Map<String, Set<DataBlock>> getDataBlocksById() {
		final Map<String, Set<DataBlock>> blocksById = Maps.newHashMap();
		
		for(final DataBlock dataBlock : getDataBlocks()) {
			final String id = dataBlock.getId();
			
			if(!blocksById.containsKey(id)) {
				blocksById.put(id, new HashSet<DataBlock>());
			}
			
			blocksById.get(id).add(dataBlock);
		}
		
		return Collections.unmodifiableMap(blocksById);
	}
}
