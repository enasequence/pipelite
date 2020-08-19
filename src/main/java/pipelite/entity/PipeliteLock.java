package pipelite.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Entity
@Table(
    name = "PIPELITE_LOCK",
    uniqueConstraints = @UniqueConstraint(columnNames = {"PIPELINE_NAME", "ALLOCATOR_NAME"}))
@IdClass(PipeliteLockId.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PipeliteLock {

  // TODO: change column name to LAUNCHER_NAME
  @Column(name = "ALLOCATOR_NAME")
  private String launcherName;

  // TODO: change column name to PROCESS_NAME
  @Id
  @Column(name = "PIPELINE_NAME")
  private String processName;

  @Id
  @Column(name = "LOCK_ID")
  private String lockId;
}
