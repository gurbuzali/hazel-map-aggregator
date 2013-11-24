package co.gurbuz.hazel.mapaggregator.builtin;

import java.util.Collection;
import java.util.HashSet;

/**
 * @ali 23/11/13
 */
public class DistinctValuesAggregator extends AbstractAggregator<Object, ObjectSerializableCollection, Collection> {

    public DistinctValuesAggregator() {
    }

    public DistinctValuesAggregator(String attribute) {
        super(attribute);
    }

    public ObjectSerializableCollection innerReduce(Collection<Object> values) {
        return new ObjectSerializableCollection(new HashSet<Object>(values));
    }

    public Collection collate(Collection<ObjectSerializableCollection> partialResults) {
        final HashSet<Object> objects = new HashSet<Object>();
        for (ObjectSerializableCollection partialResult : partialResults) {
            objects.addAll(partialResult.getCollection());
        }
        return objects;
    }
}
