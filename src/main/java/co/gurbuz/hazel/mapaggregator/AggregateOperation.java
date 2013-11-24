package co.gurbuz.hazel.mapaggregator;

import com.hazelcast.map.MapService;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

/**
 * @ali 22/11/13
 */
public class AggregateOperation extends AbstractNamedOperation {

    Predicate predicate;
    Aggregator aggregator;
    transient Object response = null;

    public AggregateOperation() {
    }

    public AggregateOperation(String name, Predicate predicate, Aggregator aggregator) {
        super(name);
        this.predicate = predicate;
        this.aggregator = aggregator;
    }

    public void run() throws Exception {
        final MapService service = getService();
        final SerializationService serializationService = getNodeEngine().getSerializationService();
        final RecordStore recordStore = service.getRecordStore(getPartitionId(), name);
        final Map<Data,Record> recordMap = recordStore.getReadonlyRecordMap();

        final Collection<Object> filtered = new LinkedList<Object>();

        for (Map.Entry<Data,Record> entry : recordMap.entrySet()) {
            final Data key = entry.getKey();
            final Record record = entry.getValue();
            final QueryEntry queryEntry = new QueryEntry(serializationService, key, key, record.getValue());
            if (predicate == null || predicate.apply(queryEntry)) {
                filtered.add(queryEntry.getValue());
            }
        }
        if (!filtered.isEmpty()){
            response = aggregator.reduce(filtered);
        }
    }

    public Object getResponse() {
        return response;
    }

}
