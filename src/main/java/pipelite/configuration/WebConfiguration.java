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
package pipelite.configuration;

import java.net.InetAddress;
import javax.annotation.PostConstruct;
import lombok.Data;
import lombok.extern.flogger.Flogger;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "pipelite.web")
@Flogger
/** Pipelite web service configuration. */
public class WebConfiguration {

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

  /** The pipelite web server port number. */
  private Integer port = 8082;

  /** The pipelite web server context path. */
  private String contextPath = "/pipelite";

  /** The pipelite web server password. */
  private String username = "pipelite";

  /** The pipelite web server password. */
  private String password = "pipelite";
}
