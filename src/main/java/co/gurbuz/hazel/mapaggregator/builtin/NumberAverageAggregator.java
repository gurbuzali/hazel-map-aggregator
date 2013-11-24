package co.gurbuz.hazel.mapaggregator.builtin;

import java.util.Collection;

/**
 * @ali 22/11/13
 */
public class NumberAverageAggregator extends AbstractAggregator<Number, PartialResult, Number> {

    public NumberAverageAggregator() {
    }

    public NumberAverageAggregator(String attribute) {
        super(attribute);
    }

    public PartialResult innerReduce(Collection<Number> values) {
        double sum = 0;
        long count = 0;
        for (Number value : values) {
            sum += value.doubleValue();
            count++;
        }
        return new PartialResult(sum, count);
    }

    public Number collate(Collection<PartialResult> partialResults) {
        double sum = 0;
        long count = 0;
        for (PartialResult partialResult : partialResults) {
            sum += partialResult.sum;
            count += partialResult.count;
        }
        if (count == 0){
            return sum;
        }
        return sum / count;
    }

}
