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
	
	private static class Bandwidth {
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
	private final TaskScheduler taskScheduler;

}
