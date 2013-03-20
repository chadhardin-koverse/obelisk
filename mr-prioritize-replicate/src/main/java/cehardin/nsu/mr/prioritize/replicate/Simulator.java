/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.hardware.Node;
import cehardin.nsu.mr.prioritize.replicate.hardware.Rack;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.MapReduceTaskId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.task.MapReduceTask;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Futures;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * @author Chad
 */
public class Simulator implements Callable<Object> {
	private final ExecutorService executorService;
	private final Collection<Simulated> simulateds = new ArrayList<Simulated>();
	private final Queue<DataBlockId> mapReduceTaskDataBlockQueue = Queues.newLinkedBlockingDeque();
	private final Map<NodeId, MapReduceTask> nodeTasks = new HashMap<NodeId, MapReduceTask>();
	private final Cluster cluster;
	
	public Object call() throws Exception {
		final double totalTime = 1000000;
		final double timeStep = 1000;
		double currentTime = 0;
	
		for(final DataBlockId dataBlockId : mapReduceTaskDataBlockQueue) {
		    final Set<Node> nodes = cluster.findNodesOfDataBlock(dataBlockId);
		    
		}
		for(final Rack rack : cluster.getRacks()) {
		    for(final Node node : rack.getNodes()) {
			if(!nodeTasks.containsKey(node)) {
			    final  task = mapReduceTaskQueue.poll();
			    if(task != null) {
				
			    }
			}
		    }
		}
		
		while(currentTime < totalTime) {
			final double availableStepTime = (currentTime + timeStep) > totalTime ? totalTime - currentTime : timeStep;
			final Collection<Future<Double>> futures = new ArrayList<Future<Double>>(simulateds.size());
			for(final Simulated simulated : simulateds) {
				futures.add(executorService.submit(new Callable<Double>() {
					public Double call() throws Exception {
						return simulated.execute(availableStepTime);
					}
				}));
			}
			
			for(final Future<Double> future : futures) {
				future.get();
			}
		}
		
	}
	
	
}
