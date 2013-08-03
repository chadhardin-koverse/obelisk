/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.event;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

/**
 *
 * @author Chad
 */
public class FileWriterSupplier implements Supplier<Writer> {
    private final Supplier<File> fileSupplier;

    public FileWriterSupplier(Supplier<File> fileSupplier) {
        this.fileSupplier = fileSupplier;
    }

    @Override
    public FileWriter get() {
        try {
            return new FileWriter(fileSupplier.get());
        }
        catch(IOException e) {
            throw Throwables.propagate(e);
        }
    }
    
    
}
