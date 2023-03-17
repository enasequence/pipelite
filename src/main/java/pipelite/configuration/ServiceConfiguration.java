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
package pipelite.configuration;

import java.net.InetAddress;
import java.time.Duration;
import javax.annotation.PostConstruct;
import lombok.Data;
import lombok.extern.flogger.Flogger;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "pipelite.service")
@Flogger
/** Pipelite web service configuration. */
public class ServiceConfiguration {

  public static final String DEFAULT_CONTEXT_PATH = "/pipelite";
  public static final int DEFAULT_PORT = 8083;
  public static final Duration DEFAULT_SHUTDOWN_PERIOD = Duration.ofMinutes(1);
  public static final Duration MIN_SHUTDOWN_PERIOD = Duration.ofSeconds(10);
  public static final Duration MARGIN_SHUTDOWN_PERIOD = Duration.ofSeconds(5);

  public static String getCanonicalHostName() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @PostConstruct
  private void checkRequiredProperties() {
    boolean isValid = true;
    if (port == null) {
      log.atSevere().log("Missing required pipelite property: pipelite.web.port");
      isValid = false;
    }
    if (contextPath == null) {
      log.atSevere().log("Missing required pipelite property: pipelite.web.contextPath");
      isValid = false;
    }
    if (username == null) {
      log.atSevere().log("Missing required pipelite property: pipelite.web.username");
      isValid = false;
    }
    if (password == null) {
      log.atSevere().log("Missing required pipelite property: pipelite.web.password");
      isValid = false;
    }
    if (!isValid) {
      throw new IllegalArgumentException("Missing required pipelite properties: pipelite.web.*");
    }
  }

  /** The unique service name. */
  private String name;

  /** The pipelite web server port number. */
  private Integer port = DEFAULT_PORT;

  /** The pipelite web server context path. */
  private String contextPath = DEFAULT_CONTEXT_PATH;

  /** The pipelite web server username. */
  private String username = "pipelite";

  /** The pipelite web server password. */
  private String password = "pipelite";

  /**
   * The pipelite service shutdown period to allow the service to finish gracefully. The minimum
   * shutdown period is 10 seconds and the default shutdown period is 1 minute.
   */
  private Duration shutdownPeriod = DEFAULT_SHUTDOWN_PERIOD;

  /**
   * Forces the pipelite service to start by removing all service locks and by updating service
   * names attached to schedules if necessary.
   */
  private boolean force;

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    if (name != null && !name.trim().isEmpty()) {
      return name;
    }
    return ServiceConfiguration.getCanonicalHostName() + ":" + port;
  }

  public String getUrl() {
    return ServiceConfiguration.getCanonicalHostName()
        + ":"
        + port
        + "/"
        + contextPath.replaceAll("^/*", "").replaceAll("/*$", "");
  }

  public Duration getShutdownPeriod() {
    if (shutdownPeriod.compareTo(MIN_SHUTDOWN_PERIOD) < 0) {
      return MIN_SHUTDOWN_PERIOD;
    }
    return shutdownPeriod;
  }

  public Duration getShutdownPeriodWithMargin() {
    return getShutdownPeriod().minus(MARGIN_SHUTDOWN_PERIOD);
  }
}
