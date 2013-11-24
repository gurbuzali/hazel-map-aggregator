package co.gurbuz.hazel.mapaggregator.builtin;

import co.gurbuz.hazel.mapaggregator.Aggregator;
import co.gurbuz.hazel.mapaggregator.ReflectionExtractor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;


/**
 * @ali 23/11/13
 */
public class GroupAggregator extends AbstractAggregator<Object, ObjectSerializableMap, Map> {

    private String groupBy;
    private Aggregator aggregator;

    public GroupAggregator() {
    }

    public GroupAggregator(String groupBy, Aggregator aggregator) {
        this.groupBy = groupBy;
        this.aggregator = aggregator;
    }

    public ObjectSerializableMap innerReduce(Collection values) {
        Map<Object, Collection> map = new HashMap<Object, Collection>();
        Map<Object, Object> resultMap = new HashMap<Object, Object>();
        final ReflectionExtractor extractor = new ReflectionExtractor(groupBy);
        for (Object value : values) {
            final Object groupByValue = extractor.extract(value);
            Collection collection = map.get(groupByValue);
            if (collection == null) {
                collection = new LinkedList();
                map.put(groupByValue, collection);
            }
            collection.add(value);
        }
        for (Map.Entry<Object, Collection> entry : map.entrySet()) {
            resultMap.put(entry.getKey(), aggregator.reduce(entry.getValue()));
        }
        return new ObjectSerializableMap(resultMap);
    }

    public Map collate(Collection<ObjectSerializableMap> partialResults) {
        Map<Object, Collection> partialResultMap = new HashMap<Object, Collection>();

        for (ObjectSerializableMap partialResult : partialResults) {
            for (Map.Entry entry : partialResult.map.entrySet()) {
                final Object key = entry.getKey();
                Collection collection = partialResultMap.get(key);
                if (collection == null) {
                    collection = new LinkedList();
                    partialResultMap.put(key, collection);
                }
                collection.add(entry.getValue());
            }
        }
        Map<Object, Object> result = new HashMap<Object, Object>();
        for (Map.Entry<Object, Collection> entry : partialResultMap.entrySet()) {
            result.put(entry.getKey(), aggregator.collate(entry.getValue()));
        }
        return result;
    }


    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeUTF(groupBy);
        out.writeObject(aggregator);
    }

    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        groupBy = in.readUTF();
        aggregator = in.readObject();
    }
}
