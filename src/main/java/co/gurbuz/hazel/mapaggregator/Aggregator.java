package co.gurbuz.hazel.mapaggregator;

import java.io.Serializable;
import java.util.Collection;

/**
 * @ali 22/11/13
 */
public interface Aggregator<V, T, R> extends Serializable {

    T reduce(Collection<V> values);

    R collate(Collection<T> partialResults);

}
