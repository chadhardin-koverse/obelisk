/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import java.util.Collection;
import java.util.Set;

/**
 *
 * @author Chad
 */
public class Node {
	private Resource diskResource;
	private Set<DataBlock> dataBlocks;

	public Resource getDiskResource() {
		return diskResource;
	}

	public void setDiskResource(Resource diskResource) {
		this.diskResource = diskResource;
	}

	public Set<DataBlock> getDataBlocks() {
		return dataBlocks;
	}

	public void setDataBlocks(Set<DataBlock> dataBlocks) {
		this.dataBlocks = dataBlocks;
	}
}
