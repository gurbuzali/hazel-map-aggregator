package co.gurbuz.hazel.mapaggregator.builtin;

/**
 * @ali 23/11/13
 */
public class NumberMaxAggregator extends NumberPeekAggregator {

    public NumberMaxAggregator() {
    }

    public NumberMaxAggregator(String attribute) {
        super(attribute);
    }

    public Double peek(Double max, double value) {
        return Math.max(max, value);
    }
}
