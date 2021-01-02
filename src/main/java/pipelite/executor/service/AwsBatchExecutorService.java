package pipelite.executor.service;

import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.AWSBatchClientBuilder;
import com.amazonaws.services.batch.model.*;
import lombok.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import pipelite.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

@Service
@Lazy
public class AwsBatchExecutorService implements ExecutorService {

  private static final Duration POLL_FREQUENCY = Duration.ofSeconds(10);

  @Value
  public class Client {
    private final String region;

    public SubmitJobResult submitJob(SubmitJobRequest request) {
      // TODO: exception handling
      return awsBatch(this).submitJob(request);
    }

    public void terminateJob(String jobId) {
      // TODO: exception handling
      awsBatch(this)
          .terminateJob(
              new TerminateJobRequest().withJobId(jobId).withReason("Job terminated by pipelite"));
    }

    public JobDetail describeJob(String jobId) {
      JobDetailCacheKey key = new JobDetailCacheKey(jobId, this);
      Optional<JobDetail> jobDetail = jobDetailCache.get(key);
      if (jobDetail != null && jobDetail.isPresent()) {
        jobDetailCache.remove(key);
        return jobDetail.get();
      }
      jobDetailCache.put(key, Optional.empty());
      while (true) {
        Time.wait(POLL_FREQUENCY);
        jobDetail = jobDetailCache.get(key);
        if (jobDetail != null && jobDetail.isPresent()) {
          jobDetailCache.remove(key);
          return jobDetail.get();
        }
      }
    }
  }

  @Value
  public class ClientCacheValue {
    private final Client client;
    private final AWSBatch awsBatch;
  }

  @Value
  public class JobDetailCacheKey {
    private final String jobId;
    private final Client client;
  }

  private final Map<Client, ClientCacheValue> clientCache = new ConcurrentHashMap<>();

  private final Map<JobDetailCacheKey, Optional<JobDetail>> jobDetailCache =
      new ConcurrentHashMap<>();

  private AWSBatch awsBatch(Client client) {
    return clientCache.get(client).getAwsBatch();
  }

  /**
   * Caches clients for pipeline name and stage name.
   *
   * @return the cached client
   */
  public Client client() {
    return client(null);
  }

  /**
   * Caches clients for pipeline name and stage name.
   *
   * @param region the region
   * @return the cached client
   */
  public Client client(String region) {
    ClientCacheValue clientCacheValue =
        clientCache.computeIfAbsent(this.new Client(region), this::createClientCacheValue);
    return clientCacheValue.client;
  }

  private ClientCacheValue createClientCacheValue(Client client) {
    AWSBatchClientBuilder builder = AWSBatchClientBuilder.standard();
    if (client.getRegion() != null) {
      builder.setRegion(client.getRegion());
    }
    return new ClientCacheValue(client, builder.build());
  }

  @Scheduled(fixedDelay = 10000) // Every 10 seconds
  public void run() {
    // TODO: exception handling
    Map<Client, List<JobDetailCacheKey>> byClient =
        jobDetailCache.entrySet().stream()
            .filter(e -> !e.getValue().isPresent())
            .map(e -> e.getKey())
            .collect(groupingBy(JobDetailCacheKey::getClient));

    byClient.forEach(
        (client, describeJobs) -> {
          List<String> jobIds =
              describeJobs.stream().map(j -> j.getJobId()).collect(Collectors.toList());
          DescribeJobsResult jobResult =
              awsBatch(client).describeJobs(new DescribeJobsRequest().withJobs(jobIds));
          jobResult
              .getJobs()
              .forEach(
                  j -> {
                    JobDetailCacheKey jobDetailCacheKey =
                        new JobDetailCacheKey(j.getJobId(), client);
                    Optional<JobDetail> jobDetail = jobDetailCache.get(jobDetailCacheKey);
                    if (jobDetail != null) {
                      jobDetailCache.put(jobDetailCacheKey, Optional.of(j));
                    }
                  });
        });
  }
}
