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
package pipelite.service;

import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import pipelite.entity.ServerEntity;
import pipelite.repository.ServerRepository;

import java.util.List;

@Service
@Transactional
@Flogger
public class ServerService {

  private final ServerRepository serverRepository;

  public ServerService(@Autowired ServerRepository serverRepository) {
    this.serverRepository = serverRepository;
  }

  public List<ServerEntity> getServers() {
    return serverRepository.findServers();
  }
}
