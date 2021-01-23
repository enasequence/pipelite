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

import java.time.Duration;
import org.springframework.retry.support.RetryTemplate;

public class DefaultExpRetryTaskAggregator<Request, Result, ExecutorContext>
    extends RetryTaskAggregator<Request, Result, ExecutorContext> {

  public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofMinutes(10);
  public static final RetryTemplate DEFAULT_REQUEST_RETRY = RetryTask.DEFAULT_EXP;

  public DefaultExpRetryTaskAggregator(
      int requestLimit,
      ExecutorContext executorContext,
      RetryTaskAggregatorCallback<Request, Result, ExecutorContext> task) {
    super(DEFAULT_REQUEST_RETRY, DEFAULT_REQUEST_TIMEOUT, requestLimit, executorContext, task);
  }
}