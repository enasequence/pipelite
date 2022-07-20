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

import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PreDestroy;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pipelite.configuration.ServiceConfiguration;
import pipelite.executor.describe.cache.AsyncTestDescribeJobsCache;
import pipelite.executor.describe.cache.AwsBatchDescribeJobsCache;
import pipelite.executor.describe.cache.KubernetesDescribeJobsCache;
import pipelite.executor.describe.cache.LsfDescribeJobsCache;
import pipelite.utils.LazyFactory;

@Service
@Flogger
public class DescribeJobsService {

  private final AtomicReference<LsfDescribeJobsCache> lsf = new AtomicReference<>();
  private final AtomicReference<AwsBatchDescribeJobsCache> awsBatch = new AtomicReference<>();
  private final AtomicReference<KubernetesDescribeJobsCache> kubernetes = new AtomicReference<>();
  private final AtomicReference<AsyncTestDescribeJobsCache> test = new AtomicReference<>();

  private final ServiceConfiguration serviceConfiguration;
  private final InternalErrorService internalErrorService;

  public DescribeJobsService(
      @Autowired ServiceConfiguration serviceConfiguration,
      @Autowired InternalErrorService internalErrorService) {
    this.serviceConfiguration = serviceConfiguration;
    this.internalErrorService = internalErrorService;
  }

  @PreDestroy
  private void shutdown() {
    if (lsf.get() != null) lsf.get().shutdown();
    if (awsBatch.get() != null) awsBatch.get().shutdown();
    if (kubernetes.get() != null) kubernetes.get().shutdown();
    if (test.get() != null) test.get().shutdown();
  }

  public LsfDescribeJobsCache lsf() {
    return LazyFactory.get(
        lsf, () -> new LsfDescribeJobsCache(serviceConfiguration, internalErrorService));
  }

  public AwsBatchDescribeJobsCache awsBatch() {
    return LazyFactory.get(
        awsBatch, () -> new AwsBatchDescribeJobsCache(serviceConfiguration, internalErrorService));
  }

  public KubernetesDescribeJobsCache kubernetes() {
    return LazyFactory.get(
        kubernetes,
        () -> new KubernetesDescribeJobsCache(serviceConfiguration, internalErrorService));
  }

  public AsyncTestDescribeJobsCache test() {
    return LazyFactory.get(
        test, () -> new AsyncTestDescribeJobsCache(serviceConfiguration, internalErrorService));
  }
}
