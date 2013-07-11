package cehardin.nsu.mr.prioritize.replicate;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.intersection;
import static java.util.Collections.unmodifiableMap;

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
public class StandardTaskNodeAllocator implements TaskNodeAllocator {

    public Map<TaskId, NodeId> allocate(
            Set<TaskId> taskIds,
            Set<NodeId> nodeIds,
            Function<DataBlockId, Set<NodeId>> dataBlockIdToNodeIds,
            Function<TaskId, DataBlockId> taskIdToDataBlockId) {

        final Map<TaskId, NodeId> schedule = newHashMap();
        final Set<NodeId> availableNodes = newHashSet();

        for (final TaskId taskId : taskIds) {
            final DataBlockId dataBlockId = taskIdToDataBlockId.apply(taskId);
            final Set<NodeId> dataBlockNodeIds = dataBlockIdToNodeIds.apply(dataBlockId);
            final Set<NodeId> possibleNodeIds;

            if (availableNodes.isEmpty()) {
                availableNodes.addAll(nodeIds);
            }

            possibleNodeIds = intersection(availableNodes, dataBlockNodeIds);

            if (!possibleNodeIds.isEmpty()) {
                final NodeId selection = possibleNodeIds.iterator().next();
                schedule.put(taskId, selection);
                availableNodes.remove(selection);
            }
        }

        return unmodifiableMap(schedule);
    }
}
