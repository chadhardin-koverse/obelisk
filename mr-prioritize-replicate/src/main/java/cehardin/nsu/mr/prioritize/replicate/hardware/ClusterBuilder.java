package cehardin.nsu.mr.prioritize.replicate.hardware;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;

import cehardin.nsu.mr.prioritize.replicate.DataBlock;
import cehardin.nsu.mr.prioritize.replicate.Resource;
import cehardin.nsu.mr.prioritize.replicate.Variables;
import cehardin.nsu.mr.prioritize.replicate.id.RackId;
import cehardin.nsu.mr.prioritize.replicate.id.DataBlockId;
import cehardin.nsu.mr.prioritize.replicate.id.NodeId;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 *
 * @author Chad
 */
public class ClusterBuilder {

    public Cluster buildCluster(Variables variables) {
        final Set<Rack> racks = newHashSet();
        final Map<NodeId, Set<DataBlockId>> nodeIdToDataBlockIds = newHashMap();
        final Map<RackId, Set<NodeId>> rackIdtoNodeIds = newHashMap();

        for (final DataBlockId dataBlockId : variables.getDataBlockIds()) {
            for (final NodeId nodeId : variables.getDataBlockIdToNodeIds().apply(dataBlockId)) {
                if (!nodeIdToDataBlockIds.containsKey(nodeId)) {
                    nodeIdToDataBlockIds.put(nodeId, new HashSet<DataBlockId>());
                }

                nodeIdToDataBlockIds.get(nodeId).add(dataBlockId);
            }
        }

        for (final NodeId nodeId : variables.getNodeIds()) {
            final RackId rackId = variables.getNodeIdToRackId().apply(nodeId);
            if (!rackIdtoNodeIds.containsKey(rackId)) {
                rackIdtoNodeIds.put(rackId, new HashSet<NodeId>());
            }

            rackIdtoNodeIds.get(rackId).add(nodeId);
        }

        for (final Entry<RackId, Set<NodeId>> entry : rackIdtoNodeIds.entrySet()) {
            final RackId rackId = entry.getKey();;
            final Set<Node> nodes = new HashSet<Node>();

            for (final NodeId nodeId : entry.getValue()) {
                final Set<DataBlock> dataBlocks = newHashSet();
                
                if(nodeIdToDataBlockIds.containsKey(nodeId)) {
                    for (final DataBlockId dataBlockId : nodeIdToDataBlockIds.get(nodeId)) {
                        dataBlocks.add(new DataBlock(dataBlockId, variables.getBlockSize()));
                    }
                }
                nodes.add(new Node(nodeId, new Resource(String.format("%s-disk", nodeId), variables.getDiskBandwidth().getBytesPerMs()), dataBlocks));
            }

            racks.add(new Rack(nodes, new Resource(String.format("%s-network", rackId), variables.getRackBandwidth().getBytesPerMs()), rackId));
        }

        return new Cluster(racks, new Resource(String.format("%s-network", "cluster"), variables.getClusterBandwidth().getBytesPerMs()));
    }
}
