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
    private long timeStep;
    private int numNodes;
    private int numFailedNodes;
    private int numDataBlocks;
    private Map<DataBlockId, Integer> replicaCount = Maps.newHashMap();
    private int numMRTasks;
    private int numReplicaTasks;
    private int numCriticalReplicaTasks;
    private int numNonCriticalReplicTasks;
    private int numMRTasksLeft;
    private int numMRTasksKilled;
    private int numReplicateTasksKilled;

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getTimeStep() {
        return timeStep;
    }

    public void setTimeStep(long timeStep) {
        this.timeStep = timeStep;
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

    public int getNumCriticalReplicaTasks() {
        return numCriticalReplicaTasks;
    }

    public void setNumCriticalReplicaTasks(int numCriticalReplicaTasks) {
        this.numCriticalReplicaTasks = numCriticalReplicaTasks;
    }

    public int getNumNonCriticalReplicTasks() {
        return numNonCriticalReplicTasks;
    }

    public void setNumNonCriticalReplicTasks(int numNonCriticalReplicTasks) {
        this.numNonCriticalReplicTasks = numNonCriticalReplicTasks;
    }

    public int getNumMRTasksLeft() {
        return numMRTasksLeft;
    }

    public void setNumMRTasksLeft(int numMRTasksLeft) {
        this.numMRTasksLeft = numMRTasksLeft;
    }

    public int getNumMRTasksKilled() {
        return numMRTasksKilled;
    }

    public void setNumMRTasksKilled(int numMRTasksKilled) {
        this.numMRTasksKilled = numMRTasksKilled;
    }

    public int getNumReplicateTasksKilled() {
        return numReplicateTasksKilled;
    }

    public void setNumReplicateTasksKilled(int numReplicateTasksKilled) {
        this.numReplicateTasksKilled = numReplicateTasksKilled;
    }
    
    
}
