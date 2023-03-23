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
package pipelite;

import java.util.Properties;
import java.util.function.Consumer;
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
    run(args, null);
  }

  /**
   * Runs the pipelite services.
   *
   * @param args command line arguments
   */
  public static void main(String[] args, Consumer<SpringApplication> applicationConsumer) {
    run(args, applicationConsumer);
  }

  /**
   * Runs the pipelite services.
   *
   * @param args command line arguments
   * @return Spring configurable application context
   */
  public static ConfigurableApplicationContext run(
      String[] args, Consumer<SpringApplication> applicationConsumer) {
    SpringApplication application = new SpringApplication(PipeliteApplication.class);
    application.setAdditionalProfiles("pipelite");
    if (applicationConsumer != null) {
      applicationConsumer.accept(application);
    }
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
    properties.put("management.endpoints.enabled-by-default", "false");
    properties.put("management.endpoint.health.enabled", "true");
    properties.put("management.endpoint.metrics.enabled", "true");
    properties.put("management.endpoints.web.exposure.include", "*");
    properties.put("management.health.defaults.enabled", "false");
    properties.put("management.metrics.enable.all", "false");
    properties.put("management.metrics.export.stackdriver.useSemanticMetricTypes", "true");
    properties.put("management.metrics.export.stackdriver.step", "5m");
    properties.put("spring.mvc.static-path-pattern", "/static/**");
    properties.put("spring.resources.static-locations", "classpath:/static/");
    properties.put(
        "flogger.backend_factory",
        "com.google.common.flogger.backend.slf4j.Slf4jBackendFactory#getInstance");
    // Since Spring Boot 2.6, circular references are not allowed by default.
    properties.put("spring.main.allow-circular-references", "true");
    // Spring Framework 5.3 added multiple matcher strategies. Use the one that was the only
    // available option in previous versions.
    properties.put("spring.mvc.pathmatch.matching-strategy", "ant_path_matcher");
    return properties;
  }
}
