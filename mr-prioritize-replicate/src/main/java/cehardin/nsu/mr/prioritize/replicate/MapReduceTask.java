/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

/**
 *
 * @author Chad
 */
public class MapReduceTask implements Task {
	private final Runnable callback;
	private final Node node;
	private final DataBlock dataBlock;

	public MapReduceTask(Runnable callback, Node node, DataBlock dataBlock) {
		this.callback = callback;
		this.node = node;
		this.dataBlock = dataBlock;
	}
	
	public void run() {
		if(node.getDataBlocks().contains(dataBlock)) {
			node.getDiskResource().consume(dataBlock.getSize(), callback);
		}
		else {
			throw new IllegalStateException();
		}
	}
	
	
	
}
