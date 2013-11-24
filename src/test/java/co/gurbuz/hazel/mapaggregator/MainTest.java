package co.gurbuz.hazel.mapaggregator;

import co.gurbuz.hazel.mapaggregator.builtin.*;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @ali 22/11/13
 */
@RunWith(HazelBlockJUnit4ClassRunner.class)
public class MainTest extends BaseTest {

    @Before
    @After
    public void reset(){
        Hazelcast.shutdownAll();
    }

    @Test
    public void testNumberAverage(){
        String name = "map";
        final Config config = new Config();
        config.getMapConfig(name).setInMemoryFormat(InMemoryFormat.OBJECT);
        final HazelcastInstance instance1 = newInstance(config);
        final HazelcastInstance instance2 = newInstance(config);
        final IMap<Integer, Student> map = instance1.getMap("map");

        String[] schools = {"Fatih", "Cumhuriyet", "Ataturk"};
        String[] classes = {"A", "B", "C", "D", "E"};

        int sum = 0;
        final Random random = new Random(System.currentTimeMillis());
        int id = 1;
        for (String school : schools) {
            for (String clazz : classes) {
                for (int i=1; i<21; i++) {
                    int note = random.nextInt(100)+1;
                    map.put(id++, new Student("name"+i, clazz, school, note));
                    if (school.equals("Fatih")){
                        sum += note;
                    }
                }
            }
        }
        System.err.println("average " + (sum/100));
        final MapAggregator mapAggregator = getMapAggregator(instance1, name);
        final Predicate predicate = Predicates.equal("schoolName", "Fatih");
        final Number average = mapAggregator.aggregate(predicate, new NumberAverageAggregator("note"));
        assertEquals(sum, (int) (average.doubleValue() * 100));

    }

    @Test
    public void testComparableMax(){
        String name = "map";
        final Config config = new Config();
        config.getMapConfig(name).setInMemoryFormat(InMemoryFormat.OBJECT);
        final HazelcastInstance instance1 = newInstance(config);
        final HazelcastInstance instance2 = newInstance(config);
        final IMap<Integer, Student> map = instance1.getMap("map");


        map.put(1, new Student("ali", "A", "Fatih", 98));
        map.put(2, new Student("veli", "A", "Ataturk", 73));
        map.put(3, new Student("deli", "B", "Cumhuriyet", 87));
        map.put(4, new Student("kedi", "C", "Fatih", 14));

        final MapAggregator mapAggregator = getMapAggregator(instance1, name);
        final Comparable maxNote = mapAggregator.aggregate((Predicate) null, new ComparableMaxAggregator("note"));
        assertEquals(98, maxNote);

    }

    @Test
    public void testComparableMin(){
        String name = "map";
        final Config config = new Config();
        config.getMapConfig(name).setInMemoryFormat(InMemoryFormat.OBJECT);
        final HazelcastInstance instance1 = newInstance(config);
        final HazelcastInstance instance2 = newInstance(config);
        final IMap<Integer, Student> map = instance1.getMap("map");


        map.put(1, new Student("ali", "A", "Fatih", 98));
        map.put(2, new Student("veli", "A", "Ataturk", 73));
        map.put(3, new Student("deli", "B", "Cumhuriyet", 87));
        map.put(4, new Student("kedi", "C", "Fatih", 14));

        final MapAggregator mapAggregator = getMapAggregator(instance1, name);
        final Comparable minNote = mapAggregator.aggregate(Predicates.equal("className","A"), new ComparableMinAggregator("note"));
        assertEquals(73, minNote);

    }

    @Test
    public void testComposite(){
        String name = "map";
        final Config config = new Config();
        config.getMapConfig(name).setInMemoryFormat(InMemoryFormat.OBJECT);
        final HazelcastInstance instance1 = newInstance(config);
        final HazelcastInstance instance2 = newInstance(config);
        final IMap<Integer, Student> map = instance1.getMap("map");


        map.put(1, new Student("ali", "A", "Fatih", 98));
        map.put(2, new Student("veli", "A", "Ataturk", 73));
        map.put(3, new Student("deli", "B", "Cumhuriyet", 87));
        map.put(4, new Student("kedi", "C", "Fatih", 14));

        final MapAggregator mapAggregator = getMapAggregator(instance1, name);
        final Collection aggregate = mapAggregator.aggregate((Predicate) null, new CompositeAggregator(new NumberSumAggregator("note"), new DistinctValuesAggregator("className")));
        final Iterator iterator = aggregate.iterator();
        assertEquals(272, ((Number)iterator.next()).intValue());
        assertEquals(3, ((Collection)iterator.next()).size() );
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testCompositeWithPredicate(){
        String name = "map";
        final Config config = new Config();
        config.getMapConfig(name).setInMemoryFormat(InMemoryFormat.OBJECT);
        final HazelcastInstance instance1 = newInstance(config);
        final HazelcastInstance instance2 = newInstance(config);
        final IMap<Integer, Student> map = instance1.getMap("map");


        map.put(1, new Student("ali", "A", "Fatih", 98));
        map.put(2, new Student("veli", "A", "Ataturk", 73));
        map.put(3, new Student("deli", "B", "Cumhuriyet", 87));
        map.put(4, new Student("kedi", "C", "Fatih", 14));

        final MapAggregator mapAggregator = getMapAggregator(instance1, name);
        final Predicate equal = Predicates.equal("schoolName", "Fatih");
        final Collection aggregate = mapAggregator.aggregate(equal, new CompositeAggregator(new NumberAverageAggregator("getNote"), new DistinctValuesAggregator("getClassName")));
        final Iterator iterator = aggregate.iterator();
        assertEquals(56, ((Number)iterator.next()).intValue());
        assertEquals(2, ((Collection)iterator.next()).size() );
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testCount() {
        String name = "map";
        final Config config = new Config();
        config.getMapConfig(name).setInMemoryFormat(InMemoryFormat.OBJECT);
        final HazelcastInstance instance1 = newInstance(config);
        final HazelcastInstance instance2 = newInstance(config);
        final IMap<Integer, Student> map = instance1.getMap("map");


        map.put(1, new Student("ali", "A", "Fatih", 98));
        map.put(2, new Student("veli", "A", "Ataturk", 73));
        map.put(3, new Student("deli", "B", "Cumhuriyet", 87));
        map.put(4, new Student("kedi", "C", "Fatih", 14));

        final MapAggregator mapAggregator = getMapAggregator(instance1, name);
        final Predicate equal = Predicates.equal("schoolName", "Fatih");
        final Integer fatih = mapAggregator.aggregate(equal, new CountAggregator());
        final Integer all = mapAggregator.aggregate((Predicate)null, new CountAggregator());
        assertEquals(2, (int)fatih);
        assertEquals(4, (int)all);
    }

    @Test
    public void testDistinctValues(){
        String name = "map";
        final Config config = new Config();
        config.getMapConfig(name).setInMemoryFormat(InMemoryFormat.OBJECT);
        final HazelcastInstance instance1 = newInstance(config);
        final HazelcastInstance instance2 = newInstance(config);
        final IMap<Integer, Student> map = instance1.getMap("map");


        map.put(1, new Student("ali", "A", "Fatih", 98));
        map.put(1, new Student("selim", "A", "Fatih", 5));
        map.put(2, new Student("veli", "A", "Ataturk", 73));
        map.put(3, new Student("deli", "B", "Cumhuriyet", 87));
        map.put(4, new Student("kedi", "C", "Fatih", 14));

        final MapAggregator mapAggregator = getMapAggregator(instance1, name);
        final Predicate equal = Predicates.equal("schoolName", "Fatih");
        final Collection fatihClasses = mapAggregator.aggregate(equal, new DistinctValuesAggregator("className"));
        assertEquals(2, fatihClasses.size());
        assertTrue(fatihClasses.contains("A"));
        assertTrue(fatihClasses.contains("C"));

        final Collection allClasses = mapAggregator.aggregate((Predicate)null, new DistinctValuesAggregator("getClassName"));
        assertEquals(3, allClasses.size());
        assertTrue(allClasses.contains("A"));
        assertTrue(allClasses.contains("B"));
        assertTrue(allClasses.contains("C"));

    }

    @Test
    public void testNumberMax() {
        String name = "map";
        final Config config = new Config();
        config.getMapConfig(name).setInMemoryFormat(InMemoryFormat.OBJECT);
        final HazelcastInstance instance1 = newInstance(config);
        final HazelcastInstance instance2 = newInstance(config);
        final IMap<Integer, Student> map = instance1.getMap("map");


        map.put(1, new Student("ali", "A", "Fatih", 66));
        map.put(2, new Student("veli", "A", "Ataturk", 73));
        map.put(3, new Student("deli", "B", "Cumhuriyet", 87));
        map.put(4, new Student("kedi", "C", "Fatih", 14));

        final MapAggregator mapAggregator = getMapAggregator(instance1, name);
        final Predicate equal = Predicates.equal("schoolName", "Fatih");
        final Number fatih = mapAggregator.aggregate(equal, new NumberMaxAggregator("note"));
        final Number all = mapAggregator.aggregate((Predicate)null, new NumberMaxAggregator("getNote"));
        assertEquals(66, fatih.intValue());
        assertEquals(87, all.intValue());
    }

    @Test
    public void testNumberMin() {
        String name = "map";
        final Config config = new Config();
        config.getMapConfig(name).setInMemoryFormat(InMemoryFormat.OBJECT);
        final HazelcastInstance instance1 = newInstance(config);
        final HazelcastInstance instance2 = newInstance(config);
        final IMap<Integer, Student> map = instance1.getMap("map");


        map.put(1, new Student("ali", "A", "Fatih", 66));
        map.put(2, new Student("veli", "A", "Ataturk", 45));
        map.put(3, new Student("deli", "B", "Cumhuriyet", 87));
        map.put(4, new Student("kedi", "C", "Fatih", 74));

        final MapAggregator mapAggregator = getMapAggregator(instance1, name);
        final Predicate equal = Predicates.equal("schoolName", "Fatih");
        final Number fatih = mapAggregator.aggregate(equal, new NumberMinAggregator("note"));
        final Number all = mapAggregator.aggregate((Predicate)null, new NumberMinAggregator("note"));
        assertEquals(66, fatih.intValue());
        assertEquals(45, all.intValue());
    }

    @Test
    public void testNumberSum(){
        String name = "map";
        final Config config = new Config();
        config.getMapConfig(name).setInMemoryFormat(InMemoryFormat.OBJECT);
        final HazelcastInstance instance1 = newInstance(config);
        final HazelcastInstance instance2 = newInstance(config);
        final IMap<Integer, Student> map = instance1.getMap("map");

        String[] schools = {"Fatih", "Cumhuriyet", "Ataturk"};
        String[] classes = {"A", "B", "C", "D", "E"};

        int sum = 0;
        final Random random = new Random(System.currentTimeMillis());
        int id = 1;
        for (String school : schools) {
            for (String clazz : classes) {
                for (int i=1; i<21; i++) {
                    int note = random.nextInt(100)+1;
                    map.put(id++, new Student("name"+i, clazz, school, note));
                    if (school.equals("Fatih")){
                        sum += note;
                    }
                }
            }
        }
        final MapAggregator mapAggregator = getMapAggregator(instance1, name);
        final Predicate predicate = Predicates.equal("schoolName", "Fatih");
        final Number aggregate = mapAggregator.aggregate(predicate, new NumberSumAggregator("note"));
        assertEquals(sum, aggregate.intValue());

    }

    @Test
    public void testGroupBy(){
        String name = "map";
        final Config config = new Config();
        config.getMapConfig(name).setInMemoryFormat(InMemoryFormat.OBJECT);
        final HazelcastInstance instance1 = newInstance(config);
        final HazelcastInstance instance2 = newInstance(config);
        final IMap<Integer, Student> map = instance1.getMap("map");

        String[] schools = {"Fatih", "Cumhuriyet", "Ataturk"};
        String[] classes = {"A", "B", "C", "D", "E"};

        int sum = 0;
        final Random random = new Random(System.currentTimeMillis());
        int id = 1;
        for (String school : schools) {
            for (String clazz : classes) {
                for (int i=1; i<21; i++) {
                    int note = random.nextInt(100)+1;
                    map.put(id++, new Student("name"+i, clazz, school, note));
                    if (school.equals("Fatih")){
                        sum += note;
                    }
                }
            }
        }
        final MapAggregator mapAggregator = getMapAggregator(instance1, name);
        final Predicate predicate = Predicates.equal("schoolName", "Fatih");
        final Map<String, Number> classSumMap = mapAggregator.aggregate(predicate, new GroupAggregator("className", new NumberSumAggregator("getNote")));
        assertEquals(5, classSumMap.size());
        int all = classSumMap.get("A").intValue() + classSumMap.get("B").intValue() + classSumMap.get("C").intValue() +
                classSumMap.get("D").intValue() + classSumMap.get("E").intValue();
        assertEquals(sum, all);

    }


}
