package cehardin.nsu.mr.prioritize.replicate.event;

import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import cehardin.nsu.mr.prioritize.replicate.id.RackId;
import java.util.Date;

/**
 *
 * @author cehar_000
 */
public class Event {
    
    public static Event newMapReduceEvent(Type type, NodeId nodeId, DataBlockId dataBlockId) {
        final Event event = new Event();
        event.setTask(Task.MapReduce);
        event.setType(type);
        event.setDate(new Date());
        event.setNodeId(nodeId);
        event.setDataBlockId(dataBlockId);
        
        return event;
    }
    
    public static Event newReplicateEvent(Type type, NodeId fromNode, NodeId toNode, RackId fromRack, RackId toRack, DataBlockId dataBlock) {
        final Event event = new Event();
        event.setTask(Task.Replicate);
        event.setType(type);
        event.setDate(new Date());
        event.setFromNodeId(fromNode);
        event.setToNodeId(toNode);
        event.setFromRackId(fromRack);
        event.setToRackId(toRack);
        event.setDataBlockId(dataBlock);
        
        return event;
    }
    
    public enum Task {
        MapReduce,
        Replicate
    }
    
    public enum Type {
        Started,
        Finished,
        Failed
    }
    
    private Task task;
    private Type type;
    private Date date;
    private NodeId nodeId;
    private DataBlockId dataBlockId;
    private RackId fromRackId;
    private RackId toRackId;
    private NodeId fromNodeId;
    private NodeId toNodeId;

    private Event() {
        super();
    }
    
    public Task getTask() {
        return task;
    }

    public void setTask(Task task) {
        this.task = task;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public NodeId getNodeId() {
        return nodeId;
    }

    public void setNodeId(NodeId nodeId) {
        this.nodeId = nodeId;
    }

    public DataBlockId getDataBlockId() {
        return dataBlockId;
    }

    public void setDataBlockId(DataBlockId dataBlockId) {
        this.dataBlockId = dataBlockId;
    }

    public RackId getFromRackId() {
        return fromRackId;
    }

    public void setFromRackId(RackId fromRackId) {
        this.fromRackId = fromRackId;
    }

    public RackId getToRackId() {
        return toRackId;
    }

    public void setToRackId(RackId toRackId) {
        this.toRackId = toRackId;
    }

    public NodeId getFromNodeId() {
        return fromNodeId;
    }

    public void setFromNodeId(NodeId fromNodeId) {
        this.fromNodeId = fromNodeId;
    }

    public NodeId getToNodeId() {
        return toNodeId;
    }

    public void setToNodeId(NodeId toNodeId) {
        this.toNodeId = toNodeId;
    }
}
