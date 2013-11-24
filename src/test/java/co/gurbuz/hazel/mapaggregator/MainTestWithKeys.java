package co.gurbuz.hazel.mapaggregator;

import co.gurbuz.hazel.mapaggregator.builtin.*;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.*;

import static org.junit.Assert.*;

/**
 * @ali 22/11/13
 */
@RunWith(HazelBlockJUnit4ClassRunner.class)
public class MainTestWithKeys extends BaseTest {

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
        Collection<Integer> keys = new ArrayList<Integer>();
        for (String school : schools) {
            for (String clazz : classes) {
                for (int i=1; i<21; i++) {
                    int note = random.nextInt(100)+1;
                    map.put(id++, new Student("name"+i, clazz, school, note));
                    if (school.equals("Fatih")){
                        sum += note;
                        keys.add(id-1);
                    }
                }
            }
        }
        System.err.println("average " + (sum/100));
        final MapAggregator mapAggregator = getMapAggregator(instance1, name);
        final Number average = mapAggregator.aggregate(keys, new NumberAverageAggregator("note"));
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
        final Comparable maxNote = mapAggregator.aggregate(map.keySet(), new ComparableMaxAggregator("note"));
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
        final HashSet keys = new HashSet();
        keys.add(1); keys.add(2);
        final Comparable minNote = mapAggregator.aggregate(keys, new ComparableMinAggregator("note"));
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
        final Collection aggregate = mapAggregator.aggregate(map.keySet(), new CompositeAggregator(new NumberSumAggregator("note"), new DistinctValuesAggregator("className")));
        final Iterator iterator = aggregate.iterator();
        assertEquals(272, ((Number)iterator.next()).intValue());
        assertEquals(3, ((Collection)iterator.next()).size() );
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testCompositeWithKeys(){
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
        final HashSet keys = new HashSet();
        keys.add(1); keys.add(4);
        final Collection aggregate = mapAggregator.aggregate(keys, new CompositeAggregator(new NumberAverageAggregator("getNote"), new DistinctValuesAggregator("getClassName")));
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
        final HashSet keys = new HashSet();
        keys.add(1); keys.add(4);
        final Integer fatih = mapAggregator.aggregate(keys, new CountAggregator());
        final Integer all = mapAggregator.aggregate(map.keySet(), new CountAggregator());
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


        map.put(0, new Student("ali", "A", "Fatih", 98));
        map.put(1, new Student("selim", "A", "Fatih", 5));
        map.put(2, new Student("veli", "A", "Ataturk", 73));
        map.put(3, new Student("deli", "B", "Cumhuriyet", 87));
        map.put(4, new Student("kedi", "C", "Fatih", 14));

        final MapAggregator mapAggregator = getMapAggregator(instance1, name);
        final HashSet keys = new HashSet();
        keys.add(1); keys.add(0); keys.add(4);
        final Collection fatihClasses = mapAggregator.aggregate(keys, new DistinctValuesAggregator("className"));
        assertEquals(2, fatihClasses.size());
        assertTrue(fatihClasses.contains("A"));
        assertTrue(fatihClasses.contains("C"));

        final Collection allClasses = mapAggregator.aggregate(map.keySet(), new DistinctValuesAggregator("getClassName"));
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
        final HashSet keys = new HashSet();
        keys.add(1); keys.add(4);
        final Number fatih = mapAggregator.aggregate(keys, new NumberMaxAggregator("note"));
        final Number all = mapAggregator.aggregate(map.keySet(), new NumberMaxAggregator("getNote"));
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
        final HashSet keys = new HashSet();
        keys.add(1); keys.add(4);
        final Number fatih = mapAggregator.aggregate(keys, new NumberMinAggregator("note"));
        final Number all = mapAggregator.aggregate(map.keySet(), new NumberMinAggregator("note"));
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
        Collection<Integer> keys = new ArrayList<Integer>();
        for (String school : schools) {
            for (String clazz : classes) {
                for (int i=1; i<21; i++) {
                    int note = random.nextInt(100)+1;
                    map.put(id++, new Student("name"+i, clazz, school, note));
                    if (school.equals("Fatih")){
                        sum += note;
                        keys.add(id-1);
                    }
                }
            }
        }
        final MapAggregator mapAggregator = getMapAggregator(instance1, name);
        final Number aggregate = mapAggregator.aggregate(keys, new NumberSumAggregator("note"));
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
        Collection keys = new ArrayList();
        for (String school : schools) {
            for (String clazz : classes) {
                for (int i=1; i<21; i++) {
                    int note = random.nextInt(100)+1;
                    map.put(id++, new Student("name"+i, clazz, school, note));
                    if (school.equals("Fatih")){
                        sum += note;
                        keys.add(id-1);
                    }
                }
            }
        }
        final MapAggregator mapAggregator = getMapAggregator(instance1, name);
        final Map<String, Number> classSumMap = mapAggregator.aggregate(keys, new GroupAggregator("className", new NumberSumAggregator("getNote")));
        assertEquals(5, classSumMap.size());
        int all = classSumMap.get("A").intValue() + classSumMap.get("B").intValue() + classSumMap.get("C").intValue() +
                classSumMap.get("D").intValue() + classSumMap.get("E").intValue();
        assertEquals(sum, all);

    }


}
