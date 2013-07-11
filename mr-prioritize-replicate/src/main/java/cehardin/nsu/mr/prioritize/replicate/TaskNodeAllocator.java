package cehardin.nsu.mr.prioritize.replicate;

import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.id.TaskId;
import com.google.common.base.Function;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Chad
 */
public interface TaskNodeAllocator {

    Map<TaskId, NodeId> allocate(
            Set<TaskId> taskIds,
            Set<NodeId> nodeIds,
            Function<DataBlockId, Set<NodeId>> dataBlockIdToNodeIds,
            Function<TaskId, DataBlockId> taskIdToDataBlockId);
}
