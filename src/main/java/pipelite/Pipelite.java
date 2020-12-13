/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite;

import java.util.Properties;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

public class Pipelite {

  private Pipelite() {}

  /**
   * Runs the pipelite services.
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    run(args);
  }

  /**
   * Runs the pipelite services.
   *
   * @param args command line arguments
   * @return Spring configurable application context
   */
  public static ConfigurableApplicationContext run(String[] args) {
    SpringApplication application = new SpringApplication(Application.class);
    application.setDefaultProperties(getDefaultProperties());
    return application.run(args);
  }

  /**
   * Returns application properties needed by pipelite.
   *
   * @return application properties needed by pipelite
   */
  private static Properties getDefaultProperties() {
    Properties properties = new Properties();
    properties.put("spring.mvc.static-path-pattern", "/static/**");
    properties.put("spring.resources.static-locations", "classpath:/static/");
    properties.put(
        "flogger.backend_factory",
        "com.google.common.flogger.backend.slf4j.Slf4jBackendFactory#getInstance");
    return properties;
  }
}
