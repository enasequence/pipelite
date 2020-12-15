package pipelite.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@IdClass(ServerEntity.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@NamedNativeQuery(
    name = "ServerEntity.findServers",
    query = "SELECT DISTINCT HOST, PORT, CONTEXT_PATH FROM PIPELITE_LAUNCHER_LOCK",
    resultClass = ServerEntity.class)
public class ServerEntity implements Serializable {
  @Id
  @Column(name = "HOST")
  private String host;

  @Id
  @Column(name = "PORT")
  private Integer port;

  @Column(name = "CONTEXT_PATH")
  private String contextPath;
}
