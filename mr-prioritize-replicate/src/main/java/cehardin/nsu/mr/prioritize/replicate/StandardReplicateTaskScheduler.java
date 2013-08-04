package cehardin.nsu.mr.prioritize.replicate;

import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
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
    protected void sort(List<DataBlockId> oneCopy, List<DataBlockId> twoCopies) {
        //do nothing
    }

}
