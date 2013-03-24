/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.id.RackId;
import cehardin.nsu.mr.prioritize.replicate.id.TaskId;
import com.google.common.base.Function;
import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Chad
 */
public class Variables implements Serializable {
	public static class NodeFailure implements Comparable<NodeFailure> {
		private final NodeId nodeId;
		private final long time;
		private final TimeUnit timeUnit;

		public NodeFailure(NodeId nodeId, long time, TimeUnit timeUnit) {
			this.nodeId = nodeId;
			this.time = time;
			this.timeUnit = timeUnit;
		}

		public NodeId getNodeId() {
			return nodeId;
		}

		public long getTime() {
			return time;
		}

		public TimeUnit getTimeUnit() {
			return timeUnit;
		}

		public int compareTo(NodeFailure o) {
			return time < o.time ? -1 : time > o.time ? 1 : 0; 
		}
	}
	
	public static class Bandwidth {
		private final long bytes;
		private final long time;
		private final TimeUnit timeUnit;

		public Bandwidth(long bytes, long time, TimeUnit timeUnit) {
			this.bytes = bytes;
			this.time = time;
			this.timeUnit = timeUnit;
		}

		public long getBytes() {
			return bytes;
		}

		public long getTime() {
			return time;
		}

		public TimeUnit getTimeUnit() {
			return timeUnit;
		}
		
		public double getBytesPerMs() {
			final double bytesPerUnit = (double)bytes / (double)time;
			final double bytesPerMs = bytesPerUnit / timeUnit.toMillis(1);
			
			return bytesPerMs;
		}
	}
	
	private final Bandwidth diskBandwidth;
	private final Bandwidth rackBandwidth;
	private final Bandwidth clusterBandwidth;
	private final int blockSize;
	private final int maxConcurrentTasks;
	private final int maxTasksPerNode;
	private final SortedSet<NodeFailure> nodeFailures;
	private final Set<RackId> rackIds;
	private final Set<NodeId> nodeIds;
	private final Set<DataBlockId> dataBlockIds;
	private final Set<TaskId> taskIds;
	private final Function<NodeId, RackId> nodeIdToRackId;
	private final Function<DataBlockId, Set<NodeId>> dataBlockIdToNodeIds;
	private final Function<TaskId, DataBlockId> taskIdToDataBlockId;
	private final TaskNodeAllocator taskScheduler;

	public Variables(Bandwidth diskBandwidth, Bandwidth rackBandwidth, Bandwidth clusterBandwidth, int blockSize, int maxConcurrentTasks, int maxTasksPerNode, SortedSet<NodeFailure> nodeFailures, Set<RackId> rackIds, Set<NodeId> nodeIds, Set<DataBlockId> dataBlockIds, Set<TaskId> taskIds, Function<NodeId, RackId> nodeIdToRackId, Function<DataBlockId, Set<NodeId>> dataBlockIdToNodeIds, Function<TaskId, DataBlockId> taskIdToDataBlockId, TaskNodeAllocator taskScheduler) {
		this.diskBandwidth = diskBandwidth;
		this.rackBandwidth = rackBandwidth;
		this.clusterBandwidth = clusterBandwidth;
		this.blockSize = blockSize;
		this.maxConcurrentTasks = maxConcurrentTasks;
		this.maxTasksPerNode = maxTasksPerNode;
		this.nodeFailures = nodeFailures;
		this.rackIds = rackIds;
		this.nodeIds = nodeIds;
		this.dataBlockIds = dataBlockIds;
		this.taskIds = taskIds;
		this.nodeIdToRackId = nodeIdToRackId;
		this.dataBlockIdToNodeIds = dataBlockIdToNodeIds;
		this.taskIdToDataBlockId = taskIdToDataBlockId;
		this.taskScheduler = taskScheduler;
	}

	public Bandwidth getDiskBandwidth() {
		return diskBandwidth;
	}

	public Bandwidth getRackBandwidth() {
		return rackBandwidth;
	}

	public Bandwidth getClusterBandwidth() {
		return clusterBandwidth;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public int getMaxConcurrentTasks() {
		return maxConcurrentTasks;
	}

	public int getMaxTasksPerNode() {
		return maxTasksPerNode;
	}

	public SortedSet<NodeFailure> getNodeFailures() {
		return nodeFailures;
	}

	public Set<RackId> getRackIds() {
		return rackIds;
	}

	public Set<NodeId> getNodeIds() {
		return nodeIds;
	}

	public Set<DataBlockId> getDataBlockIds() {
		return dataBlockIds;
	}

	public Set<TaskId> getTaskIds() {
		return taskIds;
	}

	public Function<NodeId, RackId> getNodeIdToRackId() {
		return nodeIdToRackId;
	}

	public Function<DataBlockId, Set<NodeId>> getDataBlockIdToNodeIds() {
		return dataBlockIdToNodeIds;
	}

	public Function<TaskId, DataBlockId> getTaskIdToDataBlockId() {
		return taskIdToDataBlockId;
	}

	public TaskNodeAllocator getTaskScheduler() {
		return taskScheduler;
	}

	
}
