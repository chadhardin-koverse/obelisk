/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Collections;
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

	private final Set<Rack> racks;
	private final Resource networkResource;

	public Cluster(Set<Rack> racks, Resource networkResource) {
		this.racks = racks;
		this.networkResource = networkResource;
	}
	
	public Set<Rack> getRacks() {
		return racks;
	}

	public Resource getNetworkResource() {
		return networkResource;
	}

	public Set<DataBlock> getDataBlocks() {
		final Set<DataBlock> dataBlocks = new HashSet<DataBlock>();

		for (final Rack rack : getRacks()) {
			dataBlocks.addAll(rack.getDataBlocks());
		}

		return dataBlocks;
	}
	
	public Map<DataBlockId, Set<DataBlock>> getDataBlocksById() {
		final Map<DataBlockId, Set<DataBlock>> blocksById = Maps.newHashMap();
		
		for(final Rack rack : getRacks()) {
			for(final Map.Entry<DataBlockId, Set<DataBlock>> nodeBlocksById : rack.getDataBlocksById().entrySet()) {
				final DataBlockId id = nodeBlocksById.getKey();
				final Set<DataBlock> dataBlocks = nodeBlocksById.getValue();
				
				if(blocksById.containsKey(id)) {
					blocksById.get(id).addAll(dataBlocks);
				}
				else {
					blocksById.put(id, dataBlocks);
				}
			}
		}
		
		return Collections.unmodifiableMap(blocksById);
	}

	public Map<DataBlockId, Integer> getDataBlockReplicationCount() {
		return Collections.unmodifiableMap(
			Maps.transformValues(getDataBlocksById(), new Function<Set<DataBlock>, Integer>() {

			public Integer apply(Set<DataBlock> dataBlocks) {
				return dataBlocks.size();
			}
			
		}));
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

	public Rack findRackOfNode(final Node node) {
		return Iterables.find(getRacks(), new Predicate<Rack>() {

			public boolean apply(Rack rack) {
				return rack.getNodes().contains(node);
			}
		
		});
	}

	public Set<Node> findNodesOfDataBlock(DataBlockId dataBlockId) {
		final Set<Node> found = new HashSet<Node>();

		for (final Rack rack : racks) {
			found.addAll(rack.findNodesOfDataBlockId(dataBlockId));
		}

		return Collections.unmodifiableSet(found);
	}

	public Set<Rack> findRacksOfDataBlock(final String dataBlockId) {
		return Collections.unmodifiableSet(
			Sets.filter(getRacks(), new Predicate<Rack>() {

			public boolean apply(Rack rack) {
				return !rack.findNodesOfDataBlockId(dataBlockId).isEmpty();
			}
			
		}));
	}
}
