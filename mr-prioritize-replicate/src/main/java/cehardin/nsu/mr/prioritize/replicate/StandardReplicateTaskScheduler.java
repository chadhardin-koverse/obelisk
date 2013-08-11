package cehardin.nsu.mr.prioritize.replicate;

import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;

/**
 *
 * @author Chad
 */
public class StandardReplicateTaskScheduler extends AbstractReplicateTaskScheduler {

    public StandardReplicateTaskScheduler(Random random, ExecutorService executorService) {
        super(random, executorService);
    }

    @Override
    protected void sort(List<ReplicateTask> tasks) {
        //do nothing
    }

}
