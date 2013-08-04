package cehardin.nsu.mr.prioritize.replicate;

import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newTreeSet;
import static java.util.Collections.unmodifiableSet;
import static java.util.Collections.unmodifiableSortedSet;

import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.id.RackId;
import cehardin.nsu.mr.prioritize.replicate.id.TaskId;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Chad
 */
public class Variables implements Serializable {

    public static class NodeFailure implements Comparable<NodeFailure>, Cloneable {

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
        
        @Override
        public NodeFailure clone() {
            return this;
        }
    }

    public static class Bandwidth implements Cloneable {

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
        
        @Override
        public Bandwidth clone() {
            return this;
        }
    }

    public static class MapReduceJob implements Cloneable {

        private final long startTime;
        private final TimeUnit timeUnit;
        private final Set<TaskId> taskIds;
        private final Function<TaskId, DataBlockId> taskIdToDataBlockId;

        public MapReduceJob(long startTime, TimeUnit timeUnit, Set<TaskId> taskIds, Function<TaskId, DataBlockId> taskIdToDataBlockId) {
            this.startTime = startTime;
            this.timeUnit = timeUnit;
            this.taskIds = newHashSet(taskIds);
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
        
        @Override
        public MapReduceJob clone() {
            return new MapReduceJob(startTime, timeUnit, taskIds, taskIdToDataBlockId);
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
    private final Function<NodeId, Set<DataBlockId>> nodeIdToDataBlockIds;
    private final TaskNodeAllocator taskNodeAllocator;
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
            Function<NodeId, Set<DataBlockId>> nodeIdsToDataBlockIds,
            TaskNodeAllocator taskNodeAllocator,
            MapReduceJob mapReduceJob) {
        this.diskBandwidth = diskBandwidth;
        this.rackBandwidth = rackBandwidth;
        this.clusterBandwidth = clusterBandwidth;
        this.blockSize = blockSize;
        this.maxConcurrentTasks = maxConcurrentTasks;
        this.maxTasksPerNode = maxTasksPerNode;
        this.nodeFailures = unmodifiableSortedSet(newTreeSet(nodeFailures));
        this.rackIds = unmodifiableSet(newHashSet(rackIds));
        this.nodeIds = unmodifiableSet(newHashSet(nodeIds));
        this.dataBlockIds = unmodifiableSet(newHashSet(dataBlockIds));
        this.nodeIdToRackId = nodeIdToRackId;
        this.dataBlockIdToNodeIds = dataBlockIdToNodeIds;
        this.nodeIdToDataBlockIds = nodeIdsToDataBlockIds;
        this.taskNodeAllocator = taskNodeAllocator;
        this.mapReduceJob = mapReduceJob.clone();
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

    public Function<NodeId, Set<DataBlockId>> getNodeIdToDataBlockIds() {
        return nodeIdToDataBlockIds;
    }

    public TaskNodeAllocator getTaskNodeAllocator() {
        return taskNodeAllocator;
    }

    public MapReduceJob getMapReduceJob() {
        return mapReduceJob;
    }
    
    @Override
    public Variables clone() {
        return new Variables(
                diskBandwidth, 
                rackBandwidth, 
                clusterBandwidth, 
                blockSize, 
                maxConcurrentTasks, 
                maxTasksPerNode, 
                nodeFailures, 
                rackIds, 
                nodeIds, 
                dataBlockIds, 
                nodeIdToRackId, 
                dataBlockIdToNodeIds, 
                nodeIdToDataBlockIds, 
                taskNodeAllocator, 
                mapReduceJob);
    }
}
