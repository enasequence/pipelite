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
