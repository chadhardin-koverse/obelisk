/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.task;

import cehardin.nsu.mr.prioritize.replicate.DataBlock;
import cehardin.nsu.mr.prioritize.replicate.hardware.Node;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 *
 * @author Chad
 */
public class MapReduceTask implements Task {
	private final Node node;
	private final DataBlock dataBlock;

	public MapReduceTask(Node node, DataBlock dataBlock) {
		this.node = node;
		this.dataBlock = dataBlock;
	}
	
	public void run() {
		Preconditions.checkState(node.getDataBlocks().contains(dataBlock));
		
                node.getDiskResource().consume(this, dataBlock.getSize());
	}
	
	@Override
	public String toString() {
		return Objects.toStringHelper(getClass()).
			add("node", node.getId()).
			add("dataBlock", dataBlock.getId()).
			toString();
	}
}
