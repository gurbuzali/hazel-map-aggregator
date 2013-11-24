package co.gurbuz.hazel.mapaggregator.builtin;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @ali 23/11/13
 */
public class ObjectSerializableCollection implements DataSerializable {

    Collection collection;

    public ObjectSerializableCollection() {
    }

    public ObjectSerializableCollection(Collection collection) {
        this.collection = collection;
    }

    public Collection getCollection() {
        return collection;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(collection.size());
        for (Object o : collection) {
            out.writeObject(o);
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        collection = new ArrayList();
        for (int i=0; i<size; i++) {
            collection.add(in.readObject());
        }
    }
}
