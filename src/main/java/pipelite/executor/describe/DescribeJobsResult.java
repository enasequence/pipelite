package pipelite.executor.describe;

import pipelite.executor.describe.context.DefaultRequestContext;
import pipelite.stage.executor.StageExecutorResult;

/**
 * Job result.
 */
public class DescribeJobsResult<RequestContext extends DefaultRequestContext> {
    public final RequestContext request;
    public final StageExecutorResult result;

    public DescribeJobsResult(RequestContext request, StageExecutorResult result) {
        this.request = request;
        this.result = result;
    }

    public DescribeJobsResult(
            DescribeJobsPollRequests<RequestContext> requests, String jobId, StageExecutorResult result) {
        this(requests.requests.get(jobId), result);
    }
}
