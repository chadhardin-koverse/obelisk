/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.hardware;

import cehardin.nsu.mr.prioritize.replicate.id.Id;
import com.google.common.base.Function;

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
}
