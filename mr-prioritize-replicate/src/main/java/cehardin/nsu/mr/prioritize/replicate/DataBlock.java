/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

/**
 *
 * @author Chad
 */
public class DataBlock {
	private final String id;
	private final int size;

	public DataBlock(String id, int size) {
		this.id = id;
		this.size = size;
	}

	public String getId() {
		return id;
	}
	
	public int getSize() {
		return size;
	}

}
