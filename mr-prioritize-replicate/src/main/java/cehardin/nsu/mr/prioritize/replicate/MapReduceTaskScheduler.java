/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.task.MapReduceTask;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Chad
 */
public interface MapReduceTaskScheduler {
    List<MapReduceTask> schedule(Cluster cluster, Variables.MapReduceJob mapReduceJob, Set<NodeId> workingNodes, int maxTasks);
}
