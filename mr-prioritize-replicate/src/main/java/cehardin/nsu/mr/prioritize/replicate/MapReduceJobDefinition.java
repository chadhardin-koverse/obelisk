/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.MapReduceTaskId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import com.google.common.base.Function;
import java.util.Set;

/**
 *
 * @author Chad
 */
public class MapReduceJobDefinition {
    final Set<DataBlockId> dataBlockIds;
    final Function<MapReduceTaskId, DataBlockId> taskToBlockFunction;
    final Function<MapReduceTaskId, NodeId> taskToNodeIdFunction;
}
