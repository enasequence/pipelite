package pipelite.executor.service;

import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.AWSBatchClientBuilder;
import com.amazonaws.services.batch.model.*;
import lombok.Value;
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
public class AwsBatchExecutorService implements ExecutorService {

  private static final Duration POLL_FREQUENCY = Duration.ofSeconds(10);

  @Value
  public class Client {
    private final String region;

    public SubmitJobResult submitJob(SubmitJobRequest request) {
      // TODO: exception handling
      return clientCache.get(this).submitJob(request);
    }

    public void terminateJob(String jobId) {
      // TODO: exception handling
      clientCache
          .get(this)
          .terminateJob(
              new TerminateJobRequest().withJobId(jobId).withReason("Job terminated by pipelite"));
    }

    public JobDetail describeJob(String jobId) {
      Optional<JobDetail> jobDetail = jobDetailCache.get(jobId);
      if (jobDetail != null && jobDetail.isPresent()) {
        jobDetailCache.remove(jobId);
        return jobDetail.get();
      }
      jobDetailCache.put(new DescribeJob(jobId, this), Optional.empty());
      while (true) {
        Time.wait(POLL_FREQUENCY);
        jobDetail = jobDetailCache.get(jobId);
        if (jobDetail != null && jobDetail.isPresent()) {
          jobDetailCache.remove(jobId);
          return jobDetail.get();
        }
      }
    }
  }

  @Value
  public class DescribeJob {
    private final String jobId;
    private final Client client;
  }

  private final Map<Client, AWSBatch> clientCache = new ConcurrentHashMap<>();

  private final Map<DescribeJob, Optional<JobDetail>> jobDetailCache = new ConcurrentHashMap<>();

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
    Client client = this.new Client(region);
    clientCache.computeIfAbsent(client, this::addClient);
    return client;
  }

  private AWSBatch addClient(Client client) {
    AWSBatchClientBuilder builder = AWSBatchClientBuilder.standard();
    if (client.getRegion() != null) {
      builder.setRegion(client.getRegion());
    }
    return builder.build();
  }

  @Scheduled(fixedDelay = 10000) // Every 10 seconds
  public void task() {
    // TODO: exception handling
    Map<Client, List<DescribeJob>> byClient =
        jobDetailCache.entrySet().stream()
            .filter(e -> !e.getValue().isPresent())
            .map(e -> e.getKey())
            .collect(groupingBy(DescribeJob::getClient));

    byClient.forEach(
        (client, describeJobs) -> {
          List<String> jobIds =
              describeJobs.stream().map(j -> j.getJobId()).collect(Collectors.toList());
          DescribeJobsResult jobResult =
              clientCache.get(client).describeJobs(new DescribeJobsRequest().withJobs(jobIds));
          jobResult
              .getJobs()
              .forEach(
                  j -> {
                    DescribeJob describeJob = new DescribeJob(j.getJobId(), client);
                    Optional<JobDetail> jobDetail = jobDetailCache.get(describeJob);
                    if (jobDetail != null) {
                      jobDetailCache.put(describeJob, Optional.of(j));
                    }
                  });
        });
  }
}
