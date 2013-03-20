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
import java.util.Set;

/**
 *
 * @author Chad
 */
public interface TaskScheduler {
	NodeId schedule(
		TaskId taskId, 
		DataBlockId dataBlockId,
		Set<RackId> rackIds,
		Set<NodeId> nodeIds,
		Function<DataBlockId, Set<NodeId>> dataBlockIdToNodeIds,
		Function<NodeId, Integer> nodeIdToNumTasks,
		int maxTasksPerNode);
}
