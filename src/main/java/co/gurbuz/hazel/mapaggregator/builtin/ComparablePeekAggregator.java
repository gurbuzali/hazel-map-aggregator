package co.gurbuz.hazel.mapaggregator.builtin;

import com.hazelcast.nio.serialization.DataSerializable;

import java.util.Collection;

/**
 * @ali 23/11/13
 */
public abstract class ComparablePeekAggregator extends AbstractAggregator<Comparable, Comparable, Comparable> implements DataSerializable {

    public ComparablePeekAggregator() {
    }

    public ComparablePeekAggregator(String attribute) {
        super(attribute);
    }

    public abstract Comparable peek(Comparable peek, Comparable value);

    public Comparable innerReduce(Collection<Comparable> values) {
        Comparable peek = null;
        for (Comparable value : values) {
            if (peek == null) {
                peek = value;
                continue;
            }
            peek = peek(peek, value);
        }
        return peek;
    }

    public Comparable collate(Collection<Comparable> partialResults) {
        return innerReduce(partialResults);
    }
}
