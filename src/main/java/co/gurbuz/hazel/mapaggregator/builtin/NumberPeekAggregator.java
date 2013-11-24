package co.gurbuz.hazel.mapaggregator.builtin;

import com.hazelcast.nio.serialization.DataSerializable;

import java.util.Collection;

/**
 * @ali 23/11/13
 */
public abstract class NumberPeekAggregator extends AbstractAggregator<Number, Number, Number> implements DataSerializable {

    public NumberPeekAggregator() {
    }

    public NumberPeekAggregator(String attribute) {
        super(attribute);
    }

    public Number innerReduce(Collection<Number> values) {
        Double peek = null;
        for (Number value : values) {
            final double doubleValue = value.doubleValue();
            if (peek == null) {
                peek = doubleValue;
                continue;
            }
            peek = peek(peek, doubleValue);
        }
        return peek;
    }

    public abstract Double peek(Double peek, double value);

    public Number collate(Collection<Number> partialResults) {
        return innerReduce(partialResults);
    }


}
