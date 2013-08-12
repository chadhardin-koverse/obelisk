/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.event;

import static cehardin.nsu.mr.prioritize.replicate.task.ReplicateTask.isReplicateTaskCritical;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.size;
import static com.google.common.collect.Sets.difference;
import java.io.Console;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 *
 * @author Chad
 */
public class PrintWriterStatusWriter implements StatusWriter {

    private final PrintWriter printWriter;

    public PrintWriterStatusWriter(PrintWriter printWriter) {
        this.printWriter = printWriter;
    }

    @Override
    public void write(Status status) throws IOException {
        printWriter.format("%nFIX -> STEP #%,d%n", 0);
        printWriter.format("Current Time (Elapsed) : %,d, ( %,d )%n", status.getTime(), status.getTimeStep());
        printWriter.format("Nodes ( Failed ) : %,d ( %,d )%n", status.getNumNodes(), status.getNumFailedNodes());
        printWriter.format("FIX -> Running MapReduce Tasks: %,d%n", 0);
        printWriter.format("FIX -> Running Replicate Tasks ( Critical / Non-Critical ) : %,d ( %,d / %,d )%n", 0, 0, 0);
        printWriter.format("Completed MapReduce Tasks (Remaining) : %,d ( %,d )%n", status.getNumMRTasks(), status.getNumMRTasksLeft());
        printWriter.format("Completed Replicate Tasks ( Critical / Non-Critical ) : %,d ( %,d / %,d )%n", status.getNumReplicaTasks(), status.getNumCriticalReplicaTasks(), status.getNumNonCriticalReplicTasks());
        printWriter.format("Tasks Killed (MapReduce / Replicate) : %,d ( %,d / %,d )%n", status.getNumMRTasksKilled() + status.getNumReplicateTasksKilled(), status.getNumMRTasksKilled(), status.getNumReplicateTasksKilled());
        printWriter.format("Data Block Report...%n");
        for (final Map.Entry<Integer, Integer> countEntry : status.getSummarizedReplicaCount().entrySet()) {
            printWriter.format("# Data Blocks with %,d copies : %,d%n", countEntry.getKey(), countEntry.getValue());
        }
        printWriter.format("%n");
        printWriter.flush();
    }

    @Override
    public void close() throws IOException {
        printWriter.close();
    }
    
    
}
