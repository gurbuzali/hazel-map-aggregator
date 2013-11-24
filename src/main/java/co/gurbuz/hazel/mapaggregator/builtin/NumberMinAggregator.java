package co.gurbuz.hazel.mapaggregator.builtin;

/**
 * @ali 23/11/13
 */
public class NumberMinAggregator extends NumberPeekAggregator {

    public NumberMinAggregator() {
    }

    public NumberMinAggregator(String attribute) {
        super(attribute);
    }

    public Double peek(Double min, double value) {
        return Math.min(min, value);
    }
}
