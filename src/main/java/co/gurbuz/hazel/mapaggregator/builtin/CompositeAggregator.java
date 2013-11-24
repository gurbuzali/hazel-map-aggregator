package co.gurbuz.hazel.mapaggregator.builtin;

import co.gurbuz.hazel.mapaggregator.Aggregator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @ali 23/11/13
 */
public class CompositeAggregator extends AbstractAggregator<Object, ObjectSerializableCollection, Collection> {

    Aggregator[] aggregators;

    public CompositeAggregator() {
    }

    public CompositeAggregator(Aggregator... aggregators) {
        this.aggregators = aggregators;
    }

    public ObjectSerializableCollection innerReduce(Collection<Object> values) {
        Collection partialResultList = new ArrayList(aggregators.length);
        for (Aggregator aggregator : aggregators) {
            final Object partialResult = aggregator.reduce(values);
            partialResultList.add(partialResult);
        }
        return new ObjectSerializableCollection(partialResultList);
    }

    public Collection collate(Collection<ObjectSerializableCollection> partialResults) {
        Collection results = new ArrayList(aggregators.length);
        Map<Aggregator, Collection> partialResultMap = new LinkedHashMap<Aggregator, Collection>();
        for (ObjectSerializableCollection partialResult : partialResults) {
            final Collection collection = partialResult.getCollection();
            int index = 0;
            for (Object o : collection) {
                final Aggregator aggregator = aggregators[index++];
                Collection aggregatorPartialResults = partialResultMap.get(aggregator);
                if (aggregatorPartialResults == null) {
                    aggregatorPartialResults = new ArrayList();
                    partialResultMap.put(aggregator, aggregatorPartialResults);
                }
                aggregatorPartialResults.add(o);
            }
        }
        for (Map.Entry<Aggregator, Collection> entry : partialResultMap.entrySet()) {
            final Aggregator aggregator = entry.getKey();
            final Collection coll = entry.getValue();
            results.add(aggregator.collate(coll));
        }

        return results;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(aggregators.length);
        for (Aggregator aggregator : aggregators) {
            out.writeObject(aggregator);
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        int length = in.readInt();
        aggregators = new Aggregator[length];
        for (int i=0; i<length; i++) {
            aggregators[i] = in.readObject();
        }
    }
}
