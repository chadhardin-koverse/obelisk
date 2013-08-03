/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.event;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author Chad
 */
public class DateSequenceFileSupplier implements Supplier<File> {
    private final File directory;
    private final String base;
    private final String extension;
    private long sequence;
    
    public DateSequenceFileSupplier(final File directory, final String base, final String extension, final Date date, long startingSequence) {
        final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        this.directory = directory;
        this.base = String.format("%s-%s", base, dateFormat.format(date));
        this.extension = extension;
        this.sequence = startingSequence;
    }

    @Override
    public File get() {
        final String fileName = String.format("%s-%s.%s", base, sequence++, extension);
        final File file = new File(directory, fileName);
        
        try {
            file.createNewFile();
        }
        catch(IOException e) {
            throw Throwables.propagate(e);
        }
        
        return file;
    }
    
    
    
    
}
