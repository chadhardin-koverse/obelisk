package cehardin.nsu.mr.prioritize.replicate;

import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask;
import java.util.List;

/**
 *
 * @author Chad
 */
public interface ReplicateTaskScheduler {

    List<ReplicateTask> schedule(
            Cluster cluster);
}
