package pipelite.executor.describe;

import pipelite.exception.PipeliteException;
import pipelite.executor.describe.context.DefaultRequestContext;
import pipelite.stage.executor.StageExecutorResult;

/** Job result. */
public class DescribeJobsResult<RequestContext extends DefaultRequestContext> {
  public final RequestContext request;
  public final StageExecutorResult result;

  private DescribeJobsResult(RequestContext request, StageExecutorResult result) {
    this.request = request;
    this.result = result;
  }

  /**
   * Creates a job result. Throws a PipeliteException if the job request is null.
   *
   * @param request the job request.
   * @param result the stage execution result. If the stage execution results is null then no result
   *     is available for the request.
   * @return the job result.
   * @throws pipelite.exception.PipeliteException n if the job request is null.
   */
  public static <RequestContext extends DefaultRequestContext> DescribeJobsResult create(
      RequestContext request, StageExecutorResult result) {
    if (request == null) {
      throw new PipeliteException("Missing job request");
    }
    return new DescribeJobsResult(request, result);
  }

  /**
   * Creates a job result. Throws a PipeliteException if the job request is null.
   *
   * @param requests the job requests.
   * @param jobId the job id.
   * @param result the stage execution result. If the stage execution results is null then no result
   *     is available for the request.
   * @return the job result.
   * @throws pipelite.exception.PipeliteException n if the job request is null.
   */
  public static <RequestContext extends DefaultRequestContext> DescribeJobsResult create(
      DescribeJobsPollRequests<RequestContext> requests, String jobId, StageExecutorResult result) {
    return create(requests.requests.get(jobId), result);
  }
}
