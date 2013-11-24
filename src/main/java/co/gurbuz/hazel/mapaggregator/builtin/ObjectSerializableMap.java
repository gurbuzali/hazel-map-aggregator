package co.gurbuz.hazel.mapaggregator.builtin;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @ali 23/11/13
 */
public class ObjectSerializableMap implements DataSerializable {

    Map<Object, Object> map;

    public ObjectSerializableMap() {
    }

    public ObjectSerializableMap(Map map) {
        this.map = map;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<Object, Object> objectCollectionEntry : map.entrySet()) {
            out.writeObject(objectCollectionEntry.getKey());
            out.writeObject(objectCollectionEntry.getValue());
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        int mapSize = in.readInt();
        map = new HashMap<Object, Object>(mapSize);
        for (int i=0; i<mapSize; i++) {
            final Object key = in.readObject();
            final Object value = in.readObject();
            map.put(key, value);
        }
    }
}
