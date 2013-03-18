/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 *
 * @author Chad
 */
public abstract class AbstractId implements Id {

    private final String value;

    protected AbstractId(final String value) {
        Preconditions.checkNotNull(value, "value must not be null");
        Preconditions.checkArgument(!value.trim().isEmpty(), "value must not be empty");
        this.value = value.trim();
    }

    public final String getValue() {
        return value;
    }

    @Override
    public final int hashCode() {
        return Objects.hashCode(getClass(), getValue());
    }

    @Override
    public final boolean equals(Object o) {
        final boolean equal;

        if (o == this) {
            equal = true;
        } else if (o == null) {
            equal = false;
        } else if (o instanceof AbstractId) {
            final AbstractId other = (AbstractId) o;
            if (o.getClass().equals(getClass())) {
                equal = value.equals(other.value);
            } else {
                equal = false;
            }
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
