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
package pipelite.executor.describe.cache;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import pipelite.executor.describe.DescribeJobs;
import pipelite.stage.executor.StageExecutor;
import pipelite.time.Time;

public abstract class DescribeJobsCache<
    RequestContext, ExecutorContext, CacheContext, Executor extends StageExecutor> {

  private static final Duration POLL_FREQUENCY = Duration.ofSeconds(5);

  private final Function<Executor, DescribeJobs> describeJobsFactory;
  private final Function<Executor, CacheContext> describeJobsContextFactory;
  private final Map<CacheContext, DescribeJobs> describeJobsCache = new ConcurrentHashMap<>();

  public DescribeJobsCache(
      Function<Executor, DescribeJobs> describeJobsFactory,
      Function<Executor, CacheContext> describeJobsContextFactory) {
    this.describeJobsFactory = describeJobsFactory;
    this.describeJobsContextFactory = describeJobsContextFactory;
    new Thread(
            () -> {
              while (true) {
                Time.wait(POLL_FREQUENCY);
                describeJobsCache.values().forEach(e -> e.makeRequests());
              }
            })
        .start();
  }

  public CacheContext getCacheContext(Executor executor) {
    return describeJobsContextFactory.apply(executor);
  }

  public DescribeJobs<RequestContext, ExecutorContext> getDescribeJobs(Executor executor) {
    return describeJobsCache.computeIfAbsent(
        getCacheContext(executor), k -> describeJobsFactory.apply(executor));
  }
}
