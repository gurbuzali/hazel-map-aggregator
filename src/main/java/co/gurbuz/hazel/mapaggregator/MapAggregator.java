package co.gurbuz.hazel.mapaggregator;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.query.Predicate;

import java.util.Collection;

/**
 * @ali 22/11/13
 */
public interface MapAggregator extends DistributedObject {

    public <V, T, R> R aggregate(Predicate predicate, Aggregator<V, T, R> aggregator);

    public <V, T, R> R aggregate(Collection keys, Aggregator<V, T, R> aggregator);

}
