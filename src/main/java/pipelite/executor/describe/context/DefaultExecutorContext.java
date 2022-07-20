package pipelite.executor.describe.context;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Value;
import lombok.experimental.NonFinal;
import pipelite.executor.describe.DescribeJobsPollRequests;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.DescribeJobsResults;
import pipelite.stage.executor.StageExecutorResult;

@Value
@NonFinal
public class DefaultExecutorContext<RequestContext extends DefaultRequestContext> {

  @JsonIgnore private final String executorName;
  @JsonIgnore private final PollJobsCallback<RequestContext> pollJobsCallback;
  @JsonIgnore private final RecoverJobCallback<RequestContext> recoverJobCallback;

  @FunctionalInterface
  public interface PollJobsCallback<RequestContext extends DefaultRequestContext> {
    DescribeJobsResults<RequestContext> pollJobs(DescribeJobsPollRequests<RequestContext> requests);
  }

  @FunctionalInterface
  public interface RecoverJobCallback<RequestContext extends DefaultRequestContext> {
    DescribeJobsResult<RequestContext> recoverJob(RequestContext request);
  }

  public DefaultExecutorContext(
      String executorName,
      PollJobsCallback<RequestContext> pollJobsCallback,
      RecoverJobCallback<RequestContext> recoverJobCallback) {
    this.executorName = executorName;
    this.pollJobsCallback = pollJobsCallback;
    this.recoverJobCallback = recoverJobCallback;
  }

  public String executorName() {
    return executorName;
  }

  public DescribeJobsResults<RequestContext> pollJobs(
      DescribeJobsPollRequests<RequestContext> requests) {
    return pollJobsCallback.pollJobs(requests);
  }

  public DescribeJobsResult<RequestContext> recoverJob(RequestContext request) {
    if (recoverJobCallback != null) {
      return recoverJobCallback.recoverJob(request);
    }
    return DescribeJobsResult.create(request, StageExecutorResult.error());
  }
}
