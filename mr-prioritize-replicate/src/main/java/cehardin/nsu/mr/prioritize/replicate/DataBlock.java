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
	private final DataBlockId id;
	private final int size;

	public DataBlock(DataBlockId id, int size) {
		this.id = id;
		this.size = size;
	}

	public DataBlockId getId() {
		return id;
	}
	
	public int getSize() {
		return size;
	}

}
