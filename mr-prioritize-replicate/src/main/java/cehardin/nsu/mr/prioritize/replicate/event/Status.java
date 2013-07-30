/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.event;

import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import com.google.common.collect.Maps;
import java.util.Date;
import java.util.Map;

/**
 *
 * @author cehar_000
 */
public class Status {
    private long time;
    private int numNodes;
    private int numFailedNodes;
    private int numDataBlocks;
    private Map<DataBlockId, Integer> replicaCount = Maps.newHashMap();
    private int numMRTasks;
    private int numReplicaTasks;

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public int getNumNodes() {
        return numNodes;
    }

    public void setNumNodes(int numNodes) {
        this.numNodes = numNodes;
    }

    public int getNumFailedNodes() {
        return numFailedNodes;
    }

    public void setNumFailedNodes(int numFailedNodes) {
        this.numFailedNodes = numFailedNodes;
    }

    public int getNumDataBlocks() {
        return numDataBlocks;
    }

    public void setNumDataBlocks(int numDataBlocks) {
        this.numDataBlocks = numDataBlocks;
    }

    public Map<DataBlockId, Integer> getReplicaCount() {
        return replicaCount;
    }

    public void setReplicaCount(Map<DataBlockId, Integer> replicaCount) {
        this.replicaCount = replicaCount;
    }

    public int getNumMRTasks() {
        return numMRTasks;
    }

    public void setNumMRTasks(int numMRTasks) {
        this.numMRTasks = numMRTasks;
    }

    public int getNumReplicaTasks() {
        return numReplicaTasks;
    }

    public void setNumReplicaTasks(int numReplicaTasks) {
        this.numReplicaTasks = numReplicaTasks;
    }
}
