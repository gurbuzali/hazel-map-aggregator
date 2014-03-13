package co.gurbuz.hazel.mapaggregator;

import com.hazelcast.map.MapService;
import com.hazelcast.map.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @ali 24/11/13
 */
public class TargetAggregateOperation extends AbstractNamedOperation {

    Collection<Data> dataKeys;
    Data dataAggregator;
    transient Object response;

    public TargetAggregateOperation() {
    }

    public TargetAggregateOperation(String name, Collection<Data> dataKeys, Data dataAggregator) {
        super(name);
        this.dataKeys = dataKeys;
        this.dataAggregator = dataAggregator;
    }

    public void run() throws Exception {
        final NodeEngine nodeEngine = getNodeEngine();
        final Aggregator aggregator = nodeEngine.toObject(dataAggregator);
        final MapService service = getService();
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();

        Collection values = new ArrayList();
        for (Data dataKey : dataKeys) {
            final int partitionId = partitionService.getPartitionId(dataKey);
            final RecordStore recordStore = service.getRecordStore(partitionId, name);
            final Object value = recordStore.get(dataKey);
            values.add(value);
        }
        response = aggregator.reduce(values);
    }

    public Object getResponse() {
        return response;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        dataAggregator.writeData(out);
        out.writeInt(dataKeys.size());
        for (Data dataKey : dataKeys) {
            dataKey.writeData(out);
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        dataAggregator = new Data();
        dataAggregator.readData(in);
        int size = in.readInt();
        dataKeys = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            final Data data = new Data();
            data.readData(in);
            dataKeys.add(data);
        }
    }
}
