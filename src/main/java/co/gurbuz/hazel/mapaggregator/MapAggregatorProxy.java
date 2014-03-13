package co.gurbuz.hazel.mapaggregator;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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

    public <V, T, R> R aggregateAll(Aggregator<V, T, R> aggregator) {
        return aggregate((Predicate) null, aggregator);
    }

    public <V, T, R> R aggregate(Predicate predicate, Aggregator<V, T, R> aggregator) {
        NodeEngine nodeEngine = getNodeEngine();
        try {
            final Map<Integer, T> partialResultsMap = (Map<Integer, T>) nodeEngine.getOperationService()
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
        NodeEngine nodeEngine = getNodeEngine();
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();
        try {
            Map<Address, Collection<Data>> memberKeysMap = new HashMap<Address, Collection<Data>>();
            for (Object key : keys) {
                final Data dataKey = nodeEngine.toData(key);
                final int partitionId = partitionService.getPartitionId(dataKey);
                final Address partitionOwner = partitionService.getPartitionOwner(partitionId);
                Collection<Data> dataKeys = memberKeysMap.get(partitionOwner);
                if (dataKeys == null) {
                    dataKeys = new LinkedList<Data>();
                    memberKeysMap.put(partitionOwner, dataKeys);
                }
                dataKeys.add(dataKey);
            }
            Collection<Future<T>> futures = new ArrayList<Future<T>>(memberKeysMap.size());
            Data dataAggregator = nodeEngine.toData(aggregator);
            for (Map.Entry<Address, Collection<Data>> entry : memberKeysMap.entrySet()) {
                final Address target = entry.getKey();
                final Collection<Data> dateKeys = entry.getValue();
                final TargetAggregateOperation operation = new TargetAggregateOperation(name, dateKeys, dataAggregator);
                final Future<T> future = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, operation, target).invoke();
                futures.add(future);
            }

            final Collection<T> nonNull = new LinkedList<T>();
            for (Future<T> future : futures) {
                final T partialResult = future.get(15, TimeUnit.SECONDS);
                if (partialResult != null) {
                    nonNull.add(partialResult);
                }
            }
            return aggregator.collate(nonNull);


        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public String getName() {
        return name;
    }

    public String getServiceName() {
        return MapAggregatorService.SERVICE_NAME;
    }
}
