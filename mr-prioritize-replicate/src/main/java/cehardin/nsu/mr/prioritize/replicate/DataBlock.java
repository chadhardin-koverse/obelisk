package cehardin.nsu.mr.prioritize.replicate;

import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import com.google.common.base.Function;
import com.google.common.base.Objects;

/**
 *
 * @author Chad
 */
public class DataBlock {

    private static class ExtractId implements Function<DataBlock, DataBlockId> {

        public DataBlockId apply(DataBlock dataBlock) {
            return dataBlock.getId();
        }
    }
    private static Function<DataBlock, DataBlockId> ExtractId = new ExtractId();

    public static Function<DataBlock, DataBlockId> extractIdFromDataBlock() {
        return ExtractId;
    }
    private final DataBlockId id;
    private final int size;

    public DataBlock(DataBlockId id, int size) {
        this.id = id;
        this.size = size;
    }

    public DataBlockId getId() {
        return id;
    }

    public int getSize() {
        return size;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(getClass()).
                add("id", id).
                add("size", size).
                toString();
    }
}
