package pipelite.executor.describe;

import pipelite.executor.describe.context.DefaultRequestContext;

import java.util.ArrayList;
import java.util.List;

/** Job results. */
public class DescribeJobsResults<RequestContext extends DefaultRequestContext> {
  /** Jobs found by the executor backend. */
  public final List<DescribeJobsResult<RequestContext>> found = new ArrayList<>();

  /** Jobs not found by the executor backend. */
  public final List<RequestContext> notFound = new ArrayList<>();

  /**
   * Adds a job result. If the stage execution result is null then the job is considered not to have
   * been found by the executor backend.
   */
  public void add(DescribeJobsResult<RequestContext> result) {
    if (result == null) {
      return;
    }
    if (result.result != null) {
      found.add(result); // A job result is available.
    } else {
      notFound.add(result.request); // A job result is not available.
    }
  }
}
