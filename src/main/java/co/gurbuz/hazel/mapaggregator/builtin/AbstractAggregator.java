package co.gurbuz.hazel.mapaggregator.builtin;

import co.gurbuz.hazel.mapaggregator.Aggregator;
import co.gurbuz.hazel.mapaggregator.ReflectionExtractor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @ali 22/11/13
 */
public abstract class AbstractAggregator<V, T, R> implements Aggregator<V, T, R>, DataSerializable {

    String attribute;

    protected AbstractAggregator() {
    }

    protected AbstractAggregator(String attribute) {
        this.attribute = attribute;
    }

    public abstract T innerReduce(Collection<V> values);

    public final T reduce(Collection values) {
        if (attribute == null) {
            return innerReduce(values);
        }
        Collection<V> attributeValues = new ArrayList<V>(values.size());
        final ReflectionExtractor extractor = new ReflectionExtractor(attribute);
        for (Object value : values) {
            final V attributeValue = extractor.extract(value);
            attributeValues.add(attributeValue);
        }
        return innerReduce(attributeValues);
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(attribute);
    }

    public void readData(ObjectDataInput in) throws IOException {
        attribute = in.readUTF();
    }



}
