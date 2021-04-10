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
package pipelite.runner.process;

import java.util.concurrent.atomic.AtomicLong;

public class ProcessRunnerResult {
  private final AtomicLong stageSuccessCount = new AtomicLong();
  private final AtomicLong stageFailedCount = new AtomicLong();

  public ProcessRunnerResult incrementStageSuccess() {
    this.stageSuccessCount.incrementAndGet();
    return this;
  }

  public ProcessRunnerResult incrementStageFailed() {
    this.stageFailedCount.incrementAndGet();
    return this;
  }

  public long getStageSuccessCount() {
    return stageSuccessCount.get();
  }

  public long getStageFailedCount() {
    return stageFailedCount.get();
  }
}
