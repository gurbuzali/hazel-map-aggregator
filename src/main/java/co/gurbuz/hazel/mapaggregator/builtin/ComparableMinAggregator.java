package co.gurbuz.hazel.mapaggregator.builtin;

/**
 * @ali 23/11/13
 */
public class ComparableMinAggregator extends ComparablePeekAggregator {

    public ComparableMinAggregator() {
    }

    public ComparableMinAggregator(String attribute) {
        super(attribute);
    }

    public Comparable peek(Comparable peek, Comparable value) {
        if (peek.compareTo(value) < 0) {
            return peek;
        }
        return value;
    }
}
