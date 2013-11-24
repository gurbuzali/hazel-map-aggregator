package co.gurbuz.hazel.mapaggregator;

import com.hazelcast.instance.GroupProperties;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * @ali 23/11/13
 */
public class HazelBlockJUnit4ClassRunner extends BlockJUnit4ClassRunner {

    static {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.socket.bind.any", "false");

        Random rand = new Random();
        int g1 = rand.nextInt(255);
        int g2 = rand.nextInt(255);
        int g3 = rand.nextInt(255);
        System.setProperty("hazelcast.multicast.group", "224." + g1 + "." + g2 + "." + g3);

    }

    public HazelBlockJUnit4ClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    protected List<FrameworkMethod> computeTestMethods() {
        List<FrameworkMethod> methods = super.computeTestMethods();
        Collections.shuffle(methods);
        return methods;
    }

    protected void runChild(FrameworkMethod method, RunNotifier notifier) {
        long start = System.currentTimeMillis();
        String testName = method.getMethod().getDeclaringClass().getSimpleName() + "." + method.getName();
        System.out.println("Started Running Test: " + testName);
        super.runChild(method, notifier);
        float took = (float) (System.currentTimeMillis() - start) / 1000;
        System.out.println(String.format("Finished Running Test: %s in %.3f seconds.", testName, took));
    }
}
