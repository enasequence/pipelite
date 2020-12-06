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
package pipelite.launcher;

import java.util.concurrent.atomic.AtomicLong;
import lombok.Data;

@Data
public class ProcessRunnerResult {
  private long processExecutionCount;
  private long processExceptionCount;
  private AtomicLong stageSuccessCount = new AtomicLong();
  private AtomicLong stageFailedCount = new AtomicLong();
  private AtomicLong stageExceptionCount = new AtomicLong();

  public void addStageSuccessCount(long count) {
    this.stageSuccessCount.addAndGet(count);
  }

  public void addStageFailedCount(long count) {
    this.stageFailedCount.addAndGet(count);
  }

  public void addStageExceptionCount(long count) {
    this.stageExceptionCount.addAndGet(count);
  }

  public long getStageSuccessCount() {
    return stageSuccessCount.get();
  }

  public long getStageFailedCount() {
    return stageFailedCount.get();
  }

  public long getStageExceptionCount() {
    return stageFailedCount.get();
  }
}
