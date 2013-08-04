package cehardin.nsu.mr.prioritize.replicate.id;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;

/**
 *
 * @author Chad
 */
public abstract class AbstractId implements Id {

    private final String value;
    private final int hashCode;

    protected AbstractId(final String value) {
        Preconditions.checkNotNull(value, "value must not be null");
        Preconditions.checkArgument(!value.trim().isEmpty(), "value must not be empty");
        this.value = value.trim();
        this.hashCode = value.hashCode();
    }

    @Override
    public final String getValue() {
        return value;
    }

    @Override
    public int compareTo(Id o) {
        return ComparisonChain.start().compare(value, o.getValue()).result();
    }
    
    @Override
    public final int hashCode() {
        return hashCode;
    }

    @Override
    public final boolean equals(Object o) {
        final boolean equal;

        if (o == this) {
            equal = true;
        } else if (o == null) {
            equal = false;
        } else if (AbstractId.class.isInstance(o)) {
            final AbstractId other = AbstractId.class.cast(o);
                equal = value.equals(other.value);
        } else {
            equal = false;
        }

        return equal;
    }

    @Override
    public final String toString() {
        return Objects.toStringHelper(getClass()).add("value", value).toString();
    }
}
