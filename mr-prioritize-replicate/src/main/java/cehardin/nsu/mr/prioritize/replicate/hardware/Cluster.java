/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.hardware;

import cehardin.nsu.mr.prioritize.replicate.DataBlock;
import cehardin.nsu.mr.prioritize.replicate.Resource;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;

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

		for (final Rack rack : getRacks()) {
			for (final Map.Entry<DataBlockId, Set<DataBlock>> nodeBlocksById : rack.getDataBlocksById().entrySet()) {
				final DataBlockId id = nodeBlocksById.getKey();
				final Set<DataBlock> dataBlocks = nodeBlocksById.getValue();

				if (blocksById.containsKey(id)) {
					blocksById.get(id).addAll(dataBlocks);
				} else {
					blocksById.put(id, dataBlocks);
				}
			}
		}

		return Collections.unmodifiableMap(blocksById);
	}

	public Map<DataBlockId, Integer> getDataBlockReplicationCount() {
		final Map<DataBlockId, Integer> result = Maps.newHashMap();

		for (final Rack rack : getRacks()) {
			for (final Map.Entry<DataBlockId, Integer> entry : rack.getDataBlockReplicationCount().entrySet()) {
				final DataBlockId dataBlockId = entry.getKey();
				final Integer count = entry.getValue();

				if (result.containsKey(dataBlockId)) {
					final Integer currentCount = result.get(dataBlockId);
					result.put(dataBlockId, currentCount + count);
				} else {
					result.put(dataBlockId, count);
				}
			}
		}

		return Collections.unmodifiableMap(result);
	}

	public SortedMap<Integer, Set<DataBlockId>> getReplicationCounts() {
		final SortedMap<Integer, Set<DataBlockId>> result = Maps.newTreeMap();
		
		for(final Rack rack : getRacks()) {
			for(final Map.Entry<Integer, Set<DataBlockId>> entry : rack.getReplicationCounts().entrySet()) {
				final int count = entry.getKey();
				final Set<DataBlockId> dataBlockIds = entry.getValue();
				
				if(result.containsKey(count)) {
					result.get(count).addAll(dataBlockIds);
				}
				else {
					result.put(count, dataBlockIds);
				}
			}
		}

		return Collections.unmodifiableSortedMap(result);
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

	public Set<Rack> findRacksOfDataBlock(final DataBlockId dataBlockId) {
		return Collections.unmodifiableSet(
			Sets.filter(getRacks(), new Predicate<Rack>() {
			public boolean apply(Rack rack) {
				return !rack.findNodesOfDataBlockId(dataBlockId).isEmpty();
			}
		}));
	}
}
