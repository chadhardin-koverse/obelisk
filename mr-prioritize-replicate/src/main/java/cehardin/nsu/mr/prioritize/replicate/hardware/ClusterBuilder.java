/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.nsu.mr.prioritize.replicate.hardware;

import cehardin.nsu.mr.prioritize.replicate.DataBlock;
import cehardin.nsu.mr.prioritize.replicate.Resource;
import cehardin.nsu.mr.prioritize.replicate.id.RackId;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.HashMap;
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
    private int dataBlockSize = 128 * 1024 * 1024;
    private int numDataBlocks = 64 * 1024;
    private int dataBlockReplicationCount = 3;
    private double diskResourceCapacity = 100d*1024d*1024d/1000d;
    private double rackNetworkResourceCapacity = 1024d*1024d*1024d/8d/1000d;
    private double clusterNetworkResourceCapacity = 100d*1024d*1024d*1024d/8d/1000d;

    public long getSeed() {
        return seed;
    }

    public void setSeed(long seed) {
        this.seed = seed;
    }

    public int getNumRacks() {
        return numRacks;
    }

    public void setNumRacks(int numRacks) {
        this.numRacks = numRacks;
    }

    public int getNodesPerRack() {
        return nodesPerRack;
    }

    public void setNodesPerRack(int nodesPerRack) {
        this.nodesPerRack = nodesPerRack;
    }

    public int getDataBlockSize() {
        return dataBlockSize;
    }

    public void setDataBlockSize(int dataBlockSize) {
        this.dataBlockSize = dataBlockSize;
    }

    public int getNumDataBlocks() {
        return numDataBlocks;
    }

    public void setNumDataBlocks(int numDataBlocks) {
        this.numDataBlocks = numDataBlocks;
    }

    public int getDataBlockReplicationCount() {
        return dataBlockReplicationCount;
    }

    public void setDataBlockReplicationCount(int dataBlockReplicationCount) {
        this.dataBlockReplicationCount = dataBlockReplicationCount;
    }

    public double getDiskResourceCapacity() {
        return diskResourceCapacity;
    }

    public void setDiskResourceCapacity(double diskResourceCapacity) {
        this.diskResourceCapacity = diskResourceCapacity;
    }

    public double getRackNetworkResourceCapacity() {
        return rackNetworkResourceCapacity;
    }

    public void setRackNetworkResourceCapacity(double rackNetworkResourceCapacity) {
        this.rackNetworkResourceCapacity = rackNetworkResourceCapacity;
    }

    public double getClusterNetworkResourceCapacity() {
        return clusterNetworkResourceCapacity;
    }

    public void setClusterNetworkResourceCapacity(double clusterNetworkResourceCapacity) {
        this.clusterNetworkResourceCapacity = clusterNetworkResourceCapacity;
    }

    
    
    public Cluster buildCluster() {
        final Set<Rack> racks = new HashSet<Rack>();
        
        for(final Map.Entry<RackId, Map<NodeId, Set<DataBlockId>>> topologyEntry : buildTopologyMap().entrySet()) {
            final RackId rackName = topologyEntry.getKey();
            final Map<NodeId, Set<DataBlockId>> nodeMap = topologyEntry.getValue();
            final Set<Node> nodes = new HashSet<Node>();
            
            for(final Map.Entry<NodeId, Set<DataBlockId>> nodeEntry : nodeMap.entrySet()) {
                final NodeId nodeName = nodeEntry.getKey();
                final Resource diskResource = new Resource(diskResourceCapacity);
                final Set<DataBlock> dataBlocks = new HashSet<DataBlock>();
                for(final DataBlockId dataBlockId : nodeEntry.getValue()) {
                    dataBlocks.add(new DataBlock(dataBlockId, dataBlockSize));
                }
                nodes.add(new Node(nodeName, diskResource, dataBlocks));
            }
            
            racks.add(new Rack(nodes, new Resource(rackNetworkResourceCapacity), rackName));
        }
        
        return new Cluster(racks, new Resource(clusterNetworkResourceCapacity));
        
    }
    
    private Map<RackId, Map<NodeId, Set<DataBlockId>>> buildTopologyMap() {
        final Map<RackId, Map<NodeId, Set<DataBlockId>>> topologyMap = Maps.newHashMap();
        final Map<RackId, Set<NodeId>> rackNodeNameMap = buildRackNodeNameMap();
        final Set<RackId> rackNames = rackNodeNameMap.keySet();
        final Random random = new Random(seed);

        for (final Map.Entry<RackId, Set<NodeId>> rackNodesEntry : rackNodeNameMap.entrySet()) {
            final RackId rackName = rackNodesEntry.getKey();
            final Map<NodeId, Set<DataBlockId>> nodeBlockMap = Maps.newHashMap();
            for (final NodeId nodeId : rackNodesEntry.getValue()) {
                nodeBlockMap.put(nodeId, new HashSet<DataBlockId>());
            }

            topologyMap.put(rackName, nodeBlockMap);
        }

        for (final DataBlockId dataBlockId : buildDataBlockIds()) {

            for (int i = 0; i < dataBlockReplicationCount; i++) {
                if (i == 0) {
                    final RackId rackName = Iterables.get(rackNames, random.nextInt(rackNames.size()));
                    final Set<NodeId> nodes = rackNodeNameMap.get(rackName);
                    final NodeId firstNode = Iterables.get(nodes, random.nextInt(nodes.size()));

                    topologyMap.get(rackName).get(firstNode).add(dataBlockId);
                    if (dataBlockReplicationCount > 1) {
                        NodeId secondNode;
                        do {
                            secondNode = Iterables.get(nodes, random.nextInt(nodes.size()));
                        } while (!topologyMap.get(rackName).get(secondNode).add(dataBlockId));
                        i++;
                    }
                } else {
                    RackId rackName;
                    NodeId node;
                    
                    do {
                        final Set<NodeId> nodes;
                        rackName = Iterables.get(rackNames, random.nextInt(rackNames.size()));
                        nodes = rackNodeNameMap.get(rackName);
                        node = Iterables.get(nodes, random.nextInt(nodes.size()));
                    } while(!topologyMap.get(rackName).get(node).add(dataBlockId));
                }
            }
        }
        
        return Collections.unmodifiableMap(topologyMap);
    }

    private Map<RackId, Set<NodeId>> buildRackNodeNameMap() {
        final Map<RackId, Set<NodeId>> rackNodeNameMap = Maps.newHashMap();

        for (final RackId rackName : buildRackIds()) {
            rackNodeNameMap.put(rackName, buildNodeIds(rackName));
        }

        return Collections.unmodifiableMap(rackNodeNameMap);
    }

    private Set<RackId> buildRackIds() {
        final Set<RackId> rackNames = Sets.newHashSet();

        for (int id = 0; id < numRacks; id++) {
            rackNames.add(new RackId(String.format("r%s", id)));
        }

        return Collections.unmodifiableSet(rackNames);
    }

    private Set<NodeId> buildNodeIds(final RackId rackName) {
        final Set<NodeId> nodeNames = Sets.newHashSet();

        for (int id = 0; id < nodesPerRack; id++) {
            nodeNames.add(new NodeId(String.format("%s-n%s", rackName, id)));
        }

        return Collections.unmodifiableSet(nodeNames);
    }

    private Set<DataBlockId> buildDataBlockIds() {
        final Set<DataBlockId> dataBlockIds = Sets.newHashSet();
        for (int id = 0; id < numDataBlocks; id++) {
            dataBlockIds.add(new DataBlockId(Integer.toString(id)));
        }
        return Collections.unmodifiableSet(dataBlockIds);
    }
}
