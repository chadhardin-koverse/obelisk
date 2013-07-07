package cehardin.nsu.mr.prioritize.replicate.id;

import com.google.common.base.Objects;

/**
 *
 * @author Chad
 */
public final class DataBlockAddress {

    private final NodeAddress nodeAddress;
    private final DataBlockId dataBlockId;

    public DataBlockAddress(NodeAddress nodeAddress, DataBlockId dataBlockId) {
        this.nodeAddress = nodeAddress;
        this.dataBlockId = dataBlockId;
    }

    public NodeAddress getNodeAddress() {
        return nodeAddress;
    }

    public DataBlockId getDataBlockId() {
        return dataBlockId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(nodeAddress, dataBlockId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (getClass().isInstance(o)) {
            final DataBlockAddress other = DataBlockAddress.class.cast(o);
            return Objects.equal(nodeAddress, other.nodeAddress) && Objects.equal(dataBlockId, other.dataBlockId);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(getClass()).add("nodeAddress", nodeAddress).add("dataBlockId", dataBlockId).toString();
    }
}
