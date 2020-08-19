package pipelite.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import pipelite.entity.PipeliteLock;
import pipelite.entity.PipeliteLockId;

import java.util.Optional;

@Repository
public interface PipeliteLockRepository extends CrudRepository<PipeliteLock, PipeliteLockId> {
    Optional<PipeliteLock> findByProcessNameAndLockId(String processName, String lockId);

    void deleteByLauncherNameAndProcessName(String launcherName, String processName);
}
