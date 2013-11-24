package co.gurbuz.hazel.mapaggregator;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

/**
 * @ali 22/11/13
 */
public class AggregateOperationFactory implements OperationFactory {

    String name;
    Predicate predicate;
    Aggregator aggregator;

    public AggregateOperationFactory() {
    }

    public AggregateOperationFactory(String name, Predicate predicate, Aggregator aggregator) {
        this.name = name;
        this.predicate = predicate;
        this.aggregator = aggregator;
    }

    public Operation createOperation() {
        return new AggregateOperation(name, predicate, aggregator);
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(predicate);
        out.writeObject(aggregator);
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        predicate = in.readObject();
        aggregator = in.readObject();
    }
}
