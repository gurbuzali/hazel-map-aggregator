package co.gurbuz.hazel.mapaggregator.builtin;

import java.util.Collection;

/**
 * @ali 23/11/13
 */
public class NumberSumAggregator extends AbstractAggregator<Number, Number, Number> {

    public NumberSumAggregator() {
    }

    public NumberSumAggregator(String attribute) {
        super(attribute);
    }

    public Number innerReduce(Collection<Number> values) {
        double sum = 0;
        for (Number value : values) {
            sum += value.doubleValue();
        }
        return sum;
    }

    public Number collate(Collection<Number> partialResults) {
        return innerReduce(partialResults);
    }
}
