package co.gurbuz.hazel.mapaggregator;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.runner.RunWith;

/**
 * @ali 23/11/13
 */
@RunWith(HazelBlockJUnit4ClassRunner.class)
public abstract class BaseTest {

    public HazelcastInstance newInstance(Config config){
        final ServicesConfig servicesConfig = config.getServicesConfig();
        if (servicesConfig.getServiceConfig(MapAggregatorService.SERVICE_NAME) == null ){
            final ServiceConfig serviceConfig = new ServiceConfig();
            serviceConfig.setEnabled(true);
            serviceConfig.setClassName(MapAggregatorService.class.getName());
            serviceConfig.setName(MapAggregatorService.SERVICE_NAME);
            servicesConfig.addServiceConfig(serviceConfig);
        }
        return Hazelcast.newHazelcastInstance(config);
    }

    public MapAggregator getMapAggregator(HazelcastInstance instance, String name){
        return instance.getDistributedObject(MapAggregatorService.SERVICE_NAME, name);
    }
}
