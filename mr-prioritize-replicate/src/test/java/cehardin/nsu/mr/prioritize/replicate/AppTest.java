package cehardin.nsu.mr.prioritize.replicate;

import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.id.RackId;
import cehardin.nsu.mr.prioritize.replicate.id.TaskId;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Assert.*;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
	@Test
	public void test1() throws Exception {
		final Variables.Bandwidth diskBandwidth = new Variables.Bandwidth(100000000L, 1, TimeUnit.SECONDS);
		final Variables.Bandwidth rackNetworkBandwidth = new Variables.Bandwidth(1000000000L/8, 1, TimeUnit.SECONDS);
		final Variables.Bandwidth clusterNetworkBandwidth = new Variables.Bandwidth(10000000000L/8, 1, TimeUnit.SECONDS);
		final int blockSize = 128*1024*1024;
		final int numNodes = 1024;
		final int maxConcurrentTasks = numNodes / 8;
		final int maxTasksPerNode = 2;
		final RackId rack1 = new RackId("r1");
		final RackId rack2 = new RackId("r2");
		final NodeId node11 = new NodeId("r1-n1");
		final NodeId node12 = new NodeId("r1-n2");
		final NodeId node21 = new NodeId("r2-n1");
		final NodeId node22 = new NodeId("r2-n2");
		final DataBlockId dataBlock1 = new DataBlockId("db1");
		final DataBlockId dataBlock2 = new DataBlockId("db2");
		final DataBlockId dataBlock3 = new DataBlockId("db3");
		final DataBlockId dataBlock4 = new DataBlockId("db4");
		final DataBlockId dataBlock5 = new DataBlockId("db5");
		final DataBlockId dataBlock6 = new DataBlockId("db6");
		final DataBlockId dataBlock7 = new DataBlockId("db7");
		final DataBlockId dataBlock8 = new DataBlockId("db8");
		final TaskId task1 = new TaskId("t1");
		final TaskId task2 = new TaskId("t2");
		final TaskId task3 = new TaskId("t3");
		final Set<RackId> rackIds = Sets.newHashSet(rack1, rack2);
		final Set<NodeId> nodeIds = Sets.newHashSet(node11, node12, node21, node22);
		final Set<DataBlockId> dataBlockIds = Sets.newHashSet(dataBlock1, dataBlock2, dataBlock3, dataBlock4, dataBlock5, dataBlock6, dataBlock7, dataBlock8);
		final SortedSet<Variables.NodeFailure> nodeFailures = Sets.newTreeSet(Lists.newArrayList(new Variables.NodeFailure(node11, 30, TimeUnit.SECONDS)));
		final Set<TaskId> taskIds = Sets.newHashSet(task1, task2, task3);
		final Map<TaskId, DataBlockId> taskIdToDataBlockId;
		final Map<NodeId, RackId> nodeIdToRackId;
		final Map<DataBlockId, Set<NodeId>> dataBlockIdToNodeIds;
		final TaskNodeAllocator taskNodeAllocator = new StandardTaskNodeAllocator();
		final ReplicateTaskScheduler replicateTaskScheduler = new StandardReplicateTaskScheduler();
		final Variables.MapReduceJob mapReduceJob;
		final Variables variables;
		final Simulator simulator;
		final ExecutorService executorService = Executors.newFixedThreadPool(16);
		nodeIdToRackId = Maps.newHashMap();
		
		nodeIdToRackId.put(node11, rack1);
		nodeIdToRackId.put(node12, rack1);
		nodeIdToRackId.put(node21, rack2);
		nodeIdToRackId.put(node22, rack2);

		dataBlockIdToNodeIds = Maps.newHashMap();
		dataBlockIdToNodeIds.put(dataBlock1, Sets.newHashSet(node11, node21));
		dataBlockIdToNodeIds.put(dataBlock2, Sets.newHashSet(node12, node22));
		dataBlockIdToNodeIds.put(dataBlock3, Sets.newHashSet(node11, node21));
		dataBlockIdToNodeIds.put(dataBlock4, Sets.newHashSet(node12, node22));
		dataBlockIdToNodeIds.put(dataBlock5, Sets.newHashSet(node11, node21));
		dataBlockIdToNodeIds.put(dataBlock6, Sets.newHashSet(node12, node22));
		dataBlockIdToNodeIds.put(dataBlock7, Sets.newHashSet(node11, node21));
		dataBlockIdToNodeIds.put(dataBlock8, Sets.newHashSet(node12, node22));
		
		taskIdToDataBlockId = Maps.newHashMap();
		taskIdToDataBlockId.put(task1, dataBlock1);
		taskIdToDataBlockId.put(task2, dataBlock2);
		taskIdToDataBlockId.put(task3, dataBlock3);
		
		mapReduceJob = new Variables.MapReduceJob(
			10L, 
			TimeUnit.SECONDS, 
			taskIds, 
			Functions.forMap(taskIdToDataBlockId));
		
		variables = new Variables(
			diskBandwidth, 
			rackNetworkBandwidth, 
			clusterNetworkBandwidth, 
			blockSize, 
			maxConcurrentTasks, 
			maxTasksPerNode, 
			nodeFailures, 
			rackIds, 
			nodeIds, 
			dataBlockIds, 
			Functions.forMap(nodeIdToRackId), 
			Functions.forMap(dataBlockIdToNodeIds), 
			taskNodeAllocator, 
			replicateTaskScheduler, 
			mapReduceJob);
		
		simulator = new Simulator(variables, executorService);
		
		simulator.call();
	}
}
