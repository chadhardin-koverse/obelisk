package cehardin.nsu.mr.prioritize.replicate;

import cehardin.nsu.mr.prioritize.replicate.task.Task;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import org.easymock.IMocksControl;
import org.junit.Ignore;

/**
 *
 * @author cehar_000
 */
public class ResourceTest {
    
    @Test
    public void shouldTriggerSimple() throws Exception {
        final Resource resource = new Resource("resource", 1);
        final Runnable callback = createMock(Runnable.class);
        
        callback.run();
        
        replay(callback);
        
        resource.consume(1, callback);
        
        resource.execute(1);
        
        verify(callback);
    }
    
    @Test
    public void shouldTrigger() throws Exception {
        final Resource resource = new Resource("resource", 1);
        final Runnable callback = createMock(Runnable.class);
        
        callback.run();
        
        replay(callback);
        
        resource.consume(10, callback);
        
        for(int i=0; i < 10; i++) {
            resource.execute(1);
        }
        
        
        verify(callback);
    }
    
     @Test
    public void shouldTriggerMultiple() throws Exception {
        final Resource resource = new Resource("resource", 1);
        final Runnable[] callbacks = new Runnable[10];
        final IMocksControl control = createStrictControl();
        
        for(int i=0; i < callbacks.length; i++) {
            callbacks[i] = control.createMock(Runnable.class);
            callbacks[i].run();
            resource.consume(i, callbacks[i]);
        }
        
        control.replay();
       
        
        for(int i=0; i < callbacks.length; i++) {
            resource.execute(i);
        }
        
        
        control.verify();
    }
    
    @Test
    public void shouldNotTrigger() throws Exception {
        final Resource resource = new Resource("resource", 1);
        final Runnable callback = createMock(Runnable.class);
        
        replay(callback);
        
        resource.consume(11, callback);
        
        for(int i=0; i < 10; i++) {
            resource.execute(1);
        }
        
        verify(callback);
    }
}