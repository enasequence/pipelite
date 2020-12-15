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
package pipelite.repository;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.stream.Stream;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import pipelite.entity.LauncherLockEntity;
import pipelite.entity.ProcessEntity;
import pipelite.entity.ServerEntity;
import pipelite.launcher.ProcessLauncherType;

import javax.persistence.Column;
import javax.persistence.ColumnResult;
import javax.persistence.ConstructorResult;
import javax.persistence.SqlResultSetMapping;

@Repository
public interface LauncherLockRepository extends CrudRepository<LauncherLockEntity, String> {
  List<LauncherLockEntity> findByLauncherName(String launcherName);

  List<LauncherLockEntity> findByExpiryLessThan(ZonedDateTime expiry);
}
