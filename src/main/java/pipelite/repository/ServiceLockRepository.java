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
package pipelite.repository;

import java.time.ZonedDateTime;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import pipelite.entity.ServiceLockEntity;

@Repository
public interface ServiceLockRepository extends JpaRepository<ServiceLockEntity, String> {
  Optional<ServiceLockEntity> findByServiceName(String serviceName);

  Long deleteByServiceName(String serviceName);

  Long deleteByServiceNameAndExpiryLessThanEqual(String serviceName, ZonedDateTime expiry);
}
