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
package pipelite.executor.describe.context;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;

@Value
@NonFinal
@EqualsAndHashCode(callSuper = true)
public class AsyncTestRequestContext extends DefaultRequestContext {

  @EqualsAndHashCode.Exclude private final StageExecutorRequest request;
  @EqualsAndHashCode.Exclude private final ZonedDateTime startTime;
  @EqualsAndHashCode.Exclude private final Duration executionTime;

  @EqualsAndHashCode.Exclude
  private final Function<StageExecutorRequest, StageExecutorResult> callback;

  public AsyncTestRequestContext(
      String jobId,
      StageExecutorRequest request,
      ZonedDateTime startTime,
      Duration executionTime,
      Function<StageExecutorRequest, StageExecutorResult> callback) {
    super(jobId);
    this.request = request;
    this.startTime = startTime;
    this.executionTime = executionTime;
    this.callback = callback;
  }

  public StageExecutorRequest getRequest() {
    return request;
  }

  public ZonedDateTime getStartTime() {
    return startTime;
  }

  public Duration getExecutionTime() {
    return executionTime;
  }

  public Function<StageExecutorRequest, StageExecutorResult> getCallback() {
    return callback;
  }
}
