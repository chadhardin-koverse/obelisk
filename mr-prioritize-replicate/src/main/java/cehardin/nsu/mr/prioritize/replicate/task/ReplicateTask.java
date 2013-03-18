/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.task;

import cehardin.nsu.mr.prioritize.replicate.DataBlock;
import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.hardware.Node;
import cehardin.nsu.mr.prioritize.replicate.hardware.Rack;

/**
 *
 * @author Chad
 */
public class ReplicateTask implements Task {
	private final DataBlock dataBlock;
	private final Cluster cluster;
	private final Rack fromRack;
	private final Rack toRack;
	private final Node fromNode;
	private final Node toNode;
	private final Runnable callback;

	public ReplicateTask(
                DataBlock dataBlock, 
                Cluster cluster, 
                Rack fromRack, 
                Rack toRack, 
                Node fromNode, 
                Node toNode, 
                Runnable callback) {
		this.dataBlock = dataBlock;
		this.cluster = cluster;
		this.fromRack = fromRack;
		this.toRack = toRack;
		this.fromNode = fromNode;
		this.toNode = toNode;
		this.callback = callback;
	}

	
	
	public void run() {
		final long size = dataBlock.getSize();
		
		fromNode.getDiskResource().consume(size, new Runnable() {

			public void run() {
				fromRack.getNetworkResource().consume(size, new Runnable() {

					public void run() {
                                                if(fromRack.equals(toRack)) {
                                                    toNode.getDiskResource().consume(size, callback);
                                                }
                                                else {
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
					}
				});
			}
		});
	}
}
