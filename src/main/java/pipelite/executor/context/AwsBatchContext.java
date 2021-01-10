package pipelite.executor.context;

import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.model.JobDetail;
import pipelite.executor.AwsBatchExecutor;
import pipelite.executor.task.RetryTask;
import pipelite.executor.task.RetryTaskAggregator;
import pipelite.time.Time;

import java.time.Duration;
import java.util.Optional;

/** Contexts for operations that can be shared between AWSBatch executors. */
public class AwsBatchContext extends SharedExecutorContext<AWSBatch> implements Runnable {
  private static final Duration DESCRIBE_JOB_REQUEST_TIMEOUT = Duration.ofMinutes(10);
  private static final Duration DESCRIBE_JOB_REQUEST_FREQUENCY = Duration.ofSeconds(15);
  private static final int DESCRIBE_JOB_REQUEST_LIMIT = 100;

  private final DescribeJobs describeJobs;

  public static class DescribeJobs extends RetryTaskAggregator<String, JobDetail, AWSBatch> {
    public DescribeJobs(AWSBatch awsBatch) {
      super(
          RetryTask.DEFAULT_FIXED,
          DESCRIBE_JOB_REQUEST_TIMEOUT,
          DESCRIBE_JOB_REQUEST_LIMIT,
          awsBatch,
          AwsBatchExecutor::describeJobs);
    }
  }

  public AwsBatchContext(AWSBatch awsBatch) {
    super(awsBatch);
    describeJobs = new DescribeJobs(awsBatch);
    new Thread(this).start();
  }

  public Optional<JobDetail> describeJob(String jobId) {
    return describeJobs.getResult(jobId);
  }

  @Override
  public void run() {
    while (true) {
      Time.wait(DESCRIBE_JOB_REQUEST_FREQUENCY);
      describeJobs.makeRequests();
    }
  }
}
