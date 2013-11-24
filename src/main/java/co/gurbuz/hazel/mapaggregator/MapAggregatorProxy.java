package co.gurbuz.hazel.mapaggregator;

import com.hazelcast.query.Predicate;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

import static com.hazelcast.map.MapService.SERVICE_NAME;

/**
 * @ali 22/11/13
 */
public class MapAggregatorProxy extends AbstractDistributedObject<MapAggregatorService> implements MapAggregator {

    final String name;

    public MapAggregatorProxy(String name, NodeEngine nodeEngine, MapAggregatorService service) {
        super(nodeEngine, service);
        this.name = name;
    }

    public <V, T, R> R aggregate(Predicate predicate, Aggregator<V, T, R> aggregator){
        NodeEngine nodeEngine = getNodeEngine();
        try {
            final Map<Integer, T> partialResultsMap = (Map<Integer, T>)nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME, new AggregateOperationFactory(name, predicate, aggregator));
            final Collection<T> nonNull = new LinkedList<T>();
            for (T t : partialResultsMap.values()) {
                if (t != null) {
                    nonNull.add(t);
                }
            }
            return aggregator.collate(nonNull);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public <V, T, R> R aggregate(Collection keys, Aggregator<V, T, R> aggregator) {
        return null;
    }

    public String getName() {
        return name;
    }

    public String getServiceName() {
        return MapAggregatorService.SERVICE_NAME;
    }
}
