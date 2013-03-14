/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 *
 * @author Chad
 */
public class Cluster {

	private Set<Rack> racks;
	private Resource networkResource;

	public Set<Rack> getRacks() {
		return racks;
	}

	public void setRacks(Set<Rack> racks) {
		this.racks = racks;
	}

	public Resource getNetworkResource() {
		return networkResource;
	}

	public void setNetworkResource(Resource networkResource) {
		this.networkResource = networkResource;
	}

	public Set<DataBlock> getDataBlocks() {
		final Set<DataBlock> dataBlocks = new HashSet<DataBlock>();

		for (final Rack rack : getRacks()) {
			dataBlocks.addAll(rack.getDataBlocks());
		}

		return dataBlocks;
	}

	public Map<DataBlock, Integer> getDataBlockReplicationCount() {
		final Map<DataBlock, Integer> dataBlockReplicationCount = new HashMap<DataBlock, Integer>();

		for (final Rack rack : getRacks()) {
			for (final Map.Entry<DataBlock, Integer> dataBlockCountEntry : rack.getDataBlockReplicationCount().entrySet()) {
				final DataBlock dataBlock = dataBlockCountEntry.getKey();
				final int countToAdd = dataBlockCountEntry.getValue();

				if (dataBlockReplicationCount.containsKey(dataBlock)) {
					final int count = dataBlockReplicationCount.get(dataBlock);
					final int newCount = count + countToAdd;
					dataBlockReplicationCount.put(dataBlock, newCount);
				} else {
					dataBlockReplicationCount.put(dataBlock, countToAdd);
				}
			}
		}

		return dataBlockReplicationCount;
	}

	public Rack pickRandomRack() {
		final Random random = new Random();
		final int offset = random.nextInt(getRacks().size());
		final Iterator<Rack> rackIterator = getRacks().iterator();

		for (int i = 0; i < offset; i++) {
			rackIterator.next();
		}

		return rackIterator.next();
	}

	public Rack pickRandomNodeNot(Rack rack) {
		Rack randomRack;
		do {
			randomRack = pickRandomRack();
		} while (randomRack == rack);

		return randomRack;
	}

	public Rack findRackOfNode(Node node) {
		Rack found = null;

		for (final Rack rack : racks) {
			if (rack.getNodes().contains(node)) {
				found = rack;
				break;
			}
		}

		return found;
	}

	public Set<Node> findNodesOfDataBlock(DataBlock dataBlock) {
		Set<Node> found = new HashSet<Node>();

		for (final Rack rack : racks) {
			found.addAll(rack.findNodesOfDataBlock(dataBlock));
		}

		return found;
	}

	public Set<Rack> findRacksOfDataBlock(DataBlock dataBlock) {
		Set<Rack> found = new HashSet<Rack>();

		for (final Rack rack : racks) {
			if(!rack.findNodesOfDataBlock(dataBlock).isEmpty()) {
				found.add(rack);
			}
		}

		return found;
	}
}
