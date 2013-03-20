/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.id;

import com.google.common.base.Objects;

/**
 *
 * @author Chad
 */
public class NodeAddress {
	private final RackId rackId;
	private final NodeId nodeId;

	public NodeAddress(RackId rackId, NodeId nodeId) {
		this.rackId = rackId;
		this.nodeId = nodeId;
	}

	public RackId getRackId() {
		return rackId;
	}

	public NodeId getNodeId() {
		return nodeId;
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(rackId, nodeId);
	}
	
	@Override
	public boolean equals(final Object o) {
		if(this == o) {
			return true;
		}
		else if(o instanceof NodeAddress) {
			final NodeAddress other = NodeAddress.class.cast(o);
			return Objects.equal(rackId, other.rackId) && Objects.equal(nodeId, other.nodeId);
		}
		else {
			return false;
		}
	}
	
	@Override
	public String toString() {
		return Objects.toStringHelper(getClass()).add("rackId", rackId).add("nodeId", nodeId).toString();
	}
}
