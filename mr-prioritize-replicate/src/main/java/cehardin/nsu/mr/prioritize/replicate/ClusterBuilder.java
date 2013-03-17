/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 *
 * @author Chad
 */
public class ClusterBuilder {
	private long seed = 123456789;
	private int numRacks = 64;
	private int nodesPerRack = 16;
	private int dataBlockSize = 128*1024*1024;
	private int numDataBlocks = 64 * 1024;
	private int dataBlockReplicationCount = 3;
	
	
	
	private Map<String, Map<String, Set<String>>> buildTopologyMap() {
		final Map<String, Map<String, Set<String>>> topologyMap = Maps.newHashMap();
		final Map<String, Set<String>> rackNodeNameMap = buildRackNodeNameMap();
		final Set<String> rackNames = rackNodeNameMap.keySet();
		final Random random = new Random(seed);
		
		for(final Map.Entry<String, Set<String>> rackNodesEntry : rackNodeNameMap.entrySet()) {
			final String rackName = rackNodesEntry.getKey();
			final Map<String, Set<String>> nodeBlockMap = Maps.newHashMap();
			for(final String nodeId : rackNodesEntry.getValue()) {
				nodeBlockMap.put(nodeId, new HashSet<String>());
			}
			
			topologyMap.put(rackName, nodeBlockMap);
		}
		
		for(final String dataBlockId : buildDataBlockIds()) {
			
			for(int i=0; i < dataBlockReplicationCount; i++) {
				final String rackName = Iterables.get(rackNames, random.nextInt(rackNames.size()));
				final Set<String> nodes = rackNodeNameMap.get(rackName);
				if(i==0) {
					final String firstNode = Iterables.get(nodes, random.nextInt(nodes.size()));
					final String secondNode = Iterables.get(nodes, random.nextInt(nodes.size()));
					topologyMap.get(rackName).get(firstNode).add(dataBlockId);
					if(dataBlockReplicationCount > 1) {
						topologyMap.get(rackName).get(secondNode).add(dataBlockId);
						i++;
					}
				}
				else {
					final String node = Iterables.get(nodes, random.nextInt(nodes.size()));
					topologyMap.get(rackName).get(node).add(dataBlockId);
				}
			}
		}
	}
	
	private Map<String, Set<String>> buildRackNodeNameMap() {
		final Map<String, Set<String>> rackNodeNameMap = Maps.newHashMap();
		
		for(final String rackName : buildRackNames()) {
			rackNodeNameMap.put(rackName, buildNodeNames(rackName));
		}
		
		return Collections.unmodifiableMap(rackNodeNameMap);
	}
	
	private Set<String> buildRackNames() {
		final Set<String> rackNames = Sets.newHashSet();
		
		for(int id=0; id < numRacks; id++) {
			rackNames.add(String.format("r%s", id));
		}
		
		return Collections.unmodifiableSet(rackNames);
	}
	
	private Set<String> buildNodeNames(final String rackName) {
		final Set<String> nodeNames = new HashSet<String>();
		
		for(int id=0; id < nodesPerRack; id++) {
			nodeNames.add(String.format("%s-n%s", rackName, id));
		}
		
		return Collections.unmodifiableSet(nodeNames);
	}
	
	private Set<String> buildDataBlockIds() {
		final Set<String> dataBlockIds = new HashSet<String>();
		for(int id=0; id < numDataBlocks; id++) {
			dataBlockIds.add(Integer.toString(id));
		}
		return Collections.unmodifiableSet(dataBlockIds);
	}
	
	
}
