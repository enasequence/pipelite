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
package pipelite.service;

import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pipelite.executor.describe.cache.*;

@Service
@Flogger
public class DescribeJobsService {

  private final AsyncTestDescribeJobsCache asyncTestDescribeJobsCache;
  private final AwsBatchDescribeJobsCache awsBatchDescribeJobsCache;
  private final KubernetesDescribeJobsCache kubernetesDescribeJobsCache;
  private final LsfDescribeJobsCache lsfDescribeJobsCache;
  private final SlurmDescribeJobsCache slurmDescribeJobsCache;

  public DescribeJobsService(
      @Autowired AsyncTestDescribeJobsCache asyncTestDescribeJobsCache,
      @Autowired AwsBatchDescribeJobsCache awsBatchDescribeJobsCache,
      @Autowired KubernetesDescribeJobsCache kubernetesDescribeJobsCache,
      @Autowired LsfDescribeJobsCache lsfDescribeJobsCache,
      @Autowired SlurmDescribeJobsCache slurmDescribeJobsCache) {
    this.asyncTestDescribeJobsCache = asyncTestDescribeJobsCache;
    this.awsBatchDescribeJobsCache = awsBatchDescribeJobsCache;
    this.kubernetesDescribeJobsCache = kubernetesDescribeJobsCache;
    this.lsfDescribeJobsCache = lsfDescribeJobsCache;
    this.slurmDescribeJobsCache = slurmDescribeJobsCache;
  }

  public LsfDescribeJobsCache lsf() {
    return lsfDescribeJobsCache;
  }

  public SlurmDescribeJobsCache slurm() {
    return slurmDescribeJobsCache;
  }

  public AwsBatchDescribeJobsCache awsBatch() {
    return awsBatchDescribeJobsCache;
  }

  public KubernetesDescribeJobsCache kubernetes() {
    return kubernetesDescribeJobsCache;
  }

  public AsyncTestDescribeJobsCache test() {
    return asyncTestDescribeJobsCache;
  }
}
