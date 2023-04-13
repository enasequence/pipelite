/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.extern.flogger.Flogger;
import org.springframework.boot.context.event.ApplicationPreparedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.PropertySource;

/**
 * A Spring application listener that logs all properties: application.addListeners(new
 * PropertiesLogger()).
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
          System.out.println(
              propertyName + "=" + sourceProperty + " overridden to " + resolvedProperty);
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
