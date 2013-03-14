/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

/**
 *
 * @author Chad
 */
public class ReplicateTask implements Task {
	private final DataBlock dataBlock;
	private final Node fromNode;
	private final Node toNode;
	private final Runnable callback;

	public ReplicateTask(DataBlock dataBlock, Node fromNode, Node toNode, Runnable callback) {
		this.dataBlock = dataBlock;
		this.fromNode = fromNode;
		this.toNode = toNode;
		this.callback = callback;
	}
	
	public void run() {
		final long size = dataBlock.getSize();
		final Rack fromRack = fromNode.getRack();
		final Rack toRack = toNode.getRack();
		final Cluster cluster = fromRack.getCluster();
	
		if(!fromNode.getDataBlocks().contains(dataBlock)) {
			return;
		}
		
		if(dataBlock.getNodes().contains(toNode)) {
			return;
		}
		
		fromNode.getDiskResource().consume(size, new Runnable() {

			public void run() {
				fromRack.getNetworkResource().consume(size, new Runnable() {

					public void run() {
						cluster.getNetworkResource().consume(size, new Runnable() {

							public void run() {
								toRack.getNetworkResource().consume(size, new Runnable() {

									public void run() {
										toNode.getDiskResource().consume(size, callback);
									}
								});
							}
						});
					}
				});
			}
		});
	}
}
