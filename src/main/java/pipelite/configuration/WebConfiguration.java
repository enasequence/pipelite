package pipelite.configuration;

import lombok.Data;
import lombok.extern.flogger.Flogger;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.net.InetAddress;

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
