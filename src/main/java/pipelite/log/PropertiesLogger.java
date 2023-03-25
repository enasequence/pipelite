package pipelite.log;

import lombok.extern.flogger.Flogger;
import org.springframework.boot.context.event.ApplicationPreparedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.PropertySource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/** A Spring application listener that logs all properties: application.addListeners(new PropertiesLogger()).
*/
 @Flogger
public class PropertiesLogger implements ApplicationListener<ApplicationPreparedEvent> {

    private boolean isLogged = false;

    @Override
    public void onApplicationEvent(ApplicationPreparedEvent event) {
        if (!isLogged) {
            log(event.getApplicationContext().getEnvironment());
        }
        isLogged = true;
    }

    public void log(ConfigurableEnvironment environment) {
        log.atInfo().log("Showing environment properties");
        for (EnumerablePropertySource propertySource : propertySources(environment)) {
            System.out.println("=".repeat(80));
            System.out.println("Property source: " + propertySource.getName());
            System.out.println("=".repeat(80));
            String[] propertyNames = propertySource.getPropertyNames();
            Arrays.sort(propertyNames);
            for (String propertyName : propertyNames) {
                String resolvedProperty = environment.getProperty(propertyName);
                String sourceProperty = propertySource.getProperty(propertyName).toString();
                if (resolvedProperty.equals(sourceProperty)) {
                    System.out.println(propertyName + "=" + resolvedProperty);
                } else {
                    System.out.println(propertyName + "=" + sourceProperty + " overridden to " + resolvedProperty);
                }
            }
        }
    }

    private List<EnumerablePropertySource> propertySources(ConfigurableEnvironment environment) {
        List<EnumerablePropertySource> propertySources = new ArrayList<>();
        for (PropertySource<?> propertySource : environment.getPropertySources()) {
            if (propertySource instanceof EnumerablePropertySource) {
                propertySources.add((EnumerablePropertySource) propertySource);
            }
        }
        return propertySources;
    }
}