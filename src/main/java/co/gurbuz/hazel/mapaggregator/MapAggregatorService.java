package co.gurbuz.hazel.mapaggregator;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;

/**
 * @ali 22/11/13
 */
public class MapAggregatorService implements RemoteService {

    public static String SERVICE_NAME = "grbz:mapAggregatorService";
    final NodeEngine nodeEngine;

    public MapAggregatorService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public DistributedObject createDistributedObject(String objectName) {
        return new MapAggregatorProxy(objectName, nodeEngine, this);
    }

    public void destroyDistributedObject(String objectName) {

    }
}
