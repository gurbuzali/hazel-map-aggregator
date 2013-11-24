package co.gurbuz.hazel.mapaggregator.builtin;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * @ali 22/11/13
 */
public class PartialResult implements DataSerializable {

    double sum;
    long count;

    public PartialResult() {
    }

    public PartialResult(double sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeDouble(sum);
        out.writeLong(count);
    }

    public void readData(ObjectDataInput in) throws IOException {
        sum = in.readDouble();
        count = in.readLong();
    }
}
