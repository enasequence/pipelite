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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Service;
import pipelite.launcher.PipeliteLauncher;
import pipelite.launcher.PipeliteScheduler;

@Service
public class LauncherService {

  /** Registered PipeliteSchedulers. */
  private PipeliteScheduler pipeliteScheduler;

  private final List<PipeliteLauncher> pipeliteLaunchers = new ArrayList<>();

  public PipeliteScheduler getPipeliteScheduler() {
    return pipeliteScheduler;
  }

  public boolean isPipeliteScheduler() {
    return pipeliteScheduler != null;
  }

  public void clearPipeliteScheduler() {
    this.pipeliteScheduler = null;
  }

  public void setPipeliteScheduler(PipeliteScheduler pipeliteScheduler) {
    this.pipeliteScheduler = pipeliteScheduler;
  }

  public List<PipeliteLauncher> getPipeliteLaunchers() {
    return pipeliteLaunchers;
  }

  public Optional<PipeliteLauncher> getPipeliteLauncher(String pipelineName) {
    return pipeliteLaunchers.stream()
        .filter(e -> e.getPipelineName().equals(pipelineName))
        .findFirst();
  }

  public void clearPipeliteLaunchers() {
    this.pipeliteLaunchers.clear();
  }

  public void addPipeliteLauncher(PipeliteLauncher pipeliteLauncher) {
    this.pipeliteLaunchers.add(pipeliteLauncher);
  }
}
