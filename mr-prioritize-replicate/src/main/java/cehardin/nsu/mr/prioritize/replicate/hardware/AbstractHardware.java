/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.hardware;

import cehardin.nsu.mr.prioritize.replicate.id.Id;
import com.google.common.base.Function;
import com.google.common.base.Objects;

/**
 *
 * @author Chad
 */
public abstract class AbstractHardware<ID extends Id> implements Hardware<ID> {

    private static class ExtractId<ID extends Id> implements Function<AbstractHardware<ID>, ID> {

        public ID apply(AbstractHardware<ID> abstractHardware) {
            return abstractHardware.getId();
        }
    }

    public static <ID extends Id> Function<AbstractHardware<ID>, ID> extractIdFromHardware() {
        return new ExtractId<ID>();
    }

    public static <ID extends Id> Function<AbstractHardware<ID>, ID> extractIdFromHardware(final Class<ID> idType) {
        return new ExtractId<ID>();
    }
    private final ID id;

    public AbstractHardware(ID id) {
        this.id = id;
    }

    public ID getId() {
        return id;
    }
    
    
    @Override
    public final int hashCode() {
        return Objects.hashCode(getId());
    }
    
    @Override
    public final boolean equals(Object o) {
        final boolean equal;
        
        if(this == o) {
            equal = true;
        }
        else if(getClass().isInstance(o)) {
            final AbstractHardware other = getClass().cast(o);
            equal = Objects.equal(getId(), other.getId());
        }
        else {
            equal = false;
        }
        
        return equal;
    }
}
