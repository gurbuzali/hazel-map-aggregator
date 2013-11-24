package co.gurbuz.hazel.mapaggregator.builtin;

/**
 * @ali 23/11/13
 */
public class ComparableMaxAggregator extends ComparablePeekAggregator {

    public ComparableMaxAggregator() {
    }

    public ComparableMaxAggregator(String attribute) {
        super(attribute);
    }

    public Comparable peek(Comparable peek, Comparable value) {
        if (peek.compareTo(value) > 0) {
            return peek;
        }
        return value;
    }
}
