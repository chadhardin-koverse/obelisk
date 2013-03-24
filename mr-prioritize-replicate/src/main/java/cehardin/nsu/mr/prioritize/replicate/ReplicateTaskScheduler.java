/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import cehardin.nsu.mr.prioritize.replicate.hardware.Cluster;
import cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask;
import com.google.common.base.Optional;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Chad
 */
public interface ReplicateTaskScheduler {
	List<ReplicateTask> schedule(
		Cluster cluster);
}
