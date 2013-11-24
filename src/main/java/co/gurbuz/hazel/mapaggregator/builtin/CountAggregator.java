package co.gurbuz.hazel.mapaggregator.builtin;

import java.util.Collection;

/**
 * @ali 23/11/13
 */
public class CountAggregator extends AbstractAggregator<Object, Integer, Integer> {

    public CountAggregator() {
    }

    public Integer innerReduce(Collection values) {
        return values.size();
    }

    public Integer collate(Collection<Integer> partialResults) {
        int count = 0;
        for (Integer partialResult : partialResults) {
            count += partialResult;
        }
        return count;
    }
}
