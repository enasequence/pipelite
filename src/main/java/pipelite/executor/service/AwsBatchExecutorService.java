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
package pipelite.executor.service;

import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.AWSBatchClientBuilder;
import com.amazonaws.services.batch.model.*;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import org.springframework.context.annotation.Lazy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import pipelite.executor.task.RetryTask;
import pipelite.executor.task.RetryTaskAggregator;
import pipelite.time.Time;

@Service
@Lazy
@Flogger
public class AwsBatchExecutorService implements ExecutorService {

  private static final Duration POLL_FREQUENCY = Duration.ofSeconds(10);
  private static final Duration POLL_TIMEOUT = Duration.ofSeconds(30);
  private static final int DESCRIBE_JOB_AGGREGATOR_LIMIT = 100;
  private static final RetryTemplate retryTemplate = RetryTask.fixed(Duration.ofSeconds(5), 3);

  @Value
  public static class Client {
    private final AWSBatch awsBatch;

    @EqualsAndHashCode.Exclude
    private final RetryTaskAggregator<String, JobDetail> describeJobAggregator =
        new RetryTaskAggregator<>(
            this::describeJobCallback, retryTemplate, DESCRIBE_JOB_AGGREGATOR_LIMIT);

    public SubmitJobResult submitJob(SubmitJobRequest request) {
      return retryTemplate.execute(r -> awsBatch.submitJob(request));
    }

    public void terminateJob(String jobId) {
      retryTemplate.execute(
          r ->
              awsBatch.terminateJob(
                  new TerminateJobRequest()
                      .withJobId(jobId)
                      .withReason("Job terminated by pipelite")));
    }

    public JobDetail describeJob(String jobId) {
      Optional<JobDetail> result = describeJobAggregator.getResult(jobId);
      if (result == null) {
        describeJobAggregator.addRequest(jobId);
      } else if (result.isPresent()) {
        describeJobAggregator.removeRequest(jobId);
        return result.get();
      }
      ZonedDateTime waitUntil = ZonedDateTime.now().plus(POLL_TIMEOUT);
      while (true) {
        Time.waitUntil(POLL_FREQUENCY, waitUntil);
        result = describeJobAggregator.getResult(jobId);
        if (result != null && result.isPresent()) {
          describeJobAggregator.removeRequest(jobId);
          return result.get();
        }
      }
    }

    public void makeAggregatorRequests() {
      describeJobAggregator.makeRequests();
    }

    public Map<String, JobDetail> describeJobCallback(List<String> jobIds) {
      Map<String, JobDetail> result = new HashMap<>();
      DescribeJobsResult jobResult =
          awsBatch.describeJobs(new DescribeJobsRequest().withJobs(jobIds));
      jobResult.getJobs().forEach(j -> result.put(j.getJobId(), j));
      return result;
    }
  }

  @Value
  private class ClientKey {
    private final String region;
  }

  private final Map<ClientKey, Client> clientCache = new ConcurrentHashMap<>();

  /**
   * Returns a cached client or creates a new one.
   *
   * @parem region the optional AWS region
   * @return the cached client
   */
  public Client client(String region) {
    return clientCache.computeIfAbsent(
        new ClientKey(region),
        k -> {
          AWSBatchClientBuilder builder = AWSBatchClientBuilder.standard();
          if (region != null) {
            builder.setRegion(region);
          }
          return new Client(builder.build());
        });
  }

  @Scheduled(fixedDelay = 10000) // Every 10 seconds
  public void makeAggregatorRequests() {
    clientCache.values().forEach(client -> client.makeAggregatorRequests());
  }
}
