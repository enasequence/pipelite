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
package pipelite.executor.task;

import pipelite.stage.executor.StageExecutorResult;

import java.util.List;
import java.util.Map;

/**
 * Callback interface for a task executed using {@link RetryTaskAggregator}.
 *
 * @param <ExecutorContext> the execution context
 * @param <Request>> the request
 */
public interface RetryTaskAggregatorCallback<Request, ExecutorContext> {

  /**
   * Execute a {@link RetryTaskAggregator} task.
   *
   * @param requests the requests
   * @param executorContext execution context
   * @return a map or requests and results
   */
  Map<Request, StageExecutorResult> execute(List<Request> requests, ExecutorContext executorContext);
}
