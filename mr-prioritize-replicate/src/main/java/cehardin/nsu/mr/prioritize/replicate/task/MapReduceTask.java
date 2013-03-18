/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.task;

import cehardin.nsu.mr.prioritize.replicate.DataBlock;
import cehardin.nsu.mr.prioritize.replicate.hardware.Node;
import com.google.common.base.Preconditions;

/**
 *
 * @author Chad
 */
public class MapReduceTask implements Task {
	private final Node node;
	private final DataBlock dataBlock;
        private final Runnable callback;

	public MapReduceTask(Node node, DataBlock dataBlock, Runnable callback) {
		this.node = node;
		this.dataBlock = dataBlock;
                this.callback = callback;
	}
	
	public void run() {
		Preconditions.checkState(node.getDataBlocks().contains(dataBlock));
		
                node.getDiskResource().consume(dataBlock.getSize(), callback);
	}
}
