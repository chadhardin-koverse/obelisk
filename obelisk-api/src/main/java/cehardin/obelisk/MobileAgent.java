/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.obelisk;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Serializable;
import java.util.Date;

/**
 *
 * @author Chad
 */
public interface MobileAgent extends Serializable {
	void created(Object...arguments);
	
	void freeze(DataOutput dataOutput);
	
	void resume(DataInput dataInput);
	
	Date execute();
}
