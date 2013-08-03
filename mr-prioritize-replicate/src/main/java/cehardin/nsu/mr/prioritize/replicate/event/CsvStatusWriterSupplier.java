/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.event;

import com.google.common.base.Supplier;
import java.io.Writer;

/**
 *
 * @author Chad
 */
public class CsvStatusWriterSupplier implements Supplier<StatusWriter> {
    private final Supplier<Writer> writerSupplier;

    public CsvStatusWriterSupplier(Supplier<Writer> writerSupplier) {
        this.writerSupplier = writerSupplier;
    }

    @Override
    public CsvStatusWriter get() {
        return new CsvStatusWriter(writerSupplier.get());
    }
    
    
    
}
