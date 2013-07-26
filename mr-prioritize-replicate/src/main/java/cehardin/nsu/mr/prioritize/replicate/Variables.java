package cehardin.nsu.mr.prioritize.replicate;

import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.id.RackId;
import cehardin.nsu.mr.prioritize.replicate.id.TaskId;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import java.io.Serializable;
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
            return ComparisonChain.start().compare(time, o.time).compare(nodeId, o.nodeId).result();
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(nodeId);
        }
        
        @Override
        public boolean equals(Object o) {
            final boolean equal;
            
            if(this == o) {
                equal = true;
            }
            else if(getClass().isInstance(o)) {
                NodeFailure other = getClass().cast(o);
                equal = Objects.equal(nodeId, other.nodeId);
            }
            else {
                equal = false;
            }
            
            return equal;
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
            final double bytesPerUnit = (double) bytes / (double) time;
            final double bytesPerMs = bytesPerUnit / timeUnit.toMillis(1);

            return bytesPerMs;
        }
    }

    public static class MapReduceJob {

        private final long startTime;
        private final TimeUnit timeUnit;
        private final Set<TaskId> taskIds;
        private final Function<TaskId, DataBlockId> taskIdToDataBlockId;

        public MapReduceJob(long startTime, TimeUnit timeUnit, Set<TaskId> taskIds, Function<TaskId, DataBlockId> taskIdToDataBlockId) {
            this.startTime = startTime;
            this.timeUnit = timeUnit;
            this.taskIds = taskIds;
            this.taskIdToDataBlockId = taskIdToDataBlockId;
        }

        public long getStartTime() {
            return startTime;
        }

        public TimeUnit getTimeUnit() {
            return timeUnit;
        }

        public Set<TaskId> getTaskIds() {
            return taskIds;
        }

        public Function<TaskId, DataBlockId> getTaskIdToDataBlockId() {
            return taskIdToDataBlockId;
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
    private final Function<NodeId, RackId> nodeIdToRackId;
    private final Function<DataBlockId, Set<NodeId>> dataBlockIdToNodeIds;
    private final TaskNodeAllocator taskNodeAllocator;
    private final ReplicateTaskScheduler replicateTaskScheduler;
    private final MapReduceJob mapReduceJob;

    public Variables(
            Bandwidth diskBandwidth,
            Bandwidth rackBandwidth,
            Bandwidth clusterBandwidth,
            int blockSize,
            int maxConcurrentTasks,
            int maxTasksPerNode,
            SortedSet<NodeFailure> nodeFailures,
            Set<RackId> rackIds,
            Set<NodeId> nodeIds,
            Set<DataBlockId> dataBlockIds,
            Function<NodeId, RackId> nodeIdToRackId,
            Function<DataBlockId, Set<NodeId>> dataBlockIdToNodeIds,
            TaskNodeAllocator taskNodeAllocator,
            ReplicateTaskScheduler replicateTaskScheduler,
            MapReduceJob mapReduceJob) {
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
        this.nodeIdToRackId = nodeIdToRackId;
        this.dataBlockIdToNodeIds = dataBlockIdToNodeIds;
        this.taskNodeAllocator = taskNodeAllocator;
        this.replicateTaskScheduler = replicateTaskScheduler;
        this.mapReduceJob = mapReduceJob;
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

    public Function<NodeId, RackId> getNodeIdToRackId() {
        return nodeIdToRackId;
    }

    public Function<DataBlockId, Set<NodeId>> getDataBlockIdToNodeIds() {
        return dataBlockIdToNodeIds;
    }

    public TaskNodeAllocator getTaskNodeAllocator() {
        return taskNodeAllocator;
    }

    public ReplicateTaskScheduler getReplicateTaskScheduler() {
        return replicateTaskScheduler;
    }

    public MapReduceJob getMapReduceJob() {
        return mapReduceJob;
    }
}
