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
package pipelite.executor.describe;

import java.util.HashMap;
import lombok.extern.flogger.Flogger;
import pipelite.exception.PipeliteException;
import pipelite.executor.describe.context.request.DefaultRequestContext;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.executor.StageExecutorState;

/** Job result. */
@Flogger
public class DescribeJobsResult<RequestContext extends DefaultRequestContext> {
  public final RequestContext request;
  public final StageExecutorResult result;

  private DescribeJobsResult(RequestContext request, StageExecutorResult result) {
    if (request == null) {
      throw new PipeliteException("Missing job request");
    }
    if (result == null) {
      throw new PipeliteException("Missing job result");
    }
    if (request.jobId() == null) {
      throw new PipeliteException("Missing job id");
    }
    this.request = request;
    this.result = result;
  }

  public String jobId() {
    return request.jobId();
  }

  /**
   * Creates a job result.
   *
   * @param request the job request.
   * @param result the stage execution result. If the result is null then no result is available for
   *     the request.
   * @return the job result.
   * @throws pipelite.exception.PipeliteException n if the job request is null.
   */
  public static <RequestContext extends DefaultRequestContext> DescribeJobsResult create(
      RequestContext request, StageExecutorResult result) {
    return new DescribeJobsResult(request, result);
  }

  public static <RequestContext extends DefaultRequestContext> Builder<RequestContext> builder(
      RequestContext request) {
    return new Builder<>(request);
  }

  public static <RequestContext extends DefaultRequestContext> Builder<RequestContext> builder(
      DescribeJobsRequests<RequestContext> requests, String jobId) {
    if (requests == null) {
      throw new PipeliteException("Missing job requests");
    }
    if (jobId == null) {
      throw new PipeliteException("Missing job id");
    }
    return new Builder<>(requests.get(jobId));
  }

  public static class Builder<RequestContext extends DefaultRequestContext> {
    private final RequestContext request;
    private final String jobId;
    private final HashMap<String, String> attributes = new HashMap<>();
    private StageExecutorResult result;
    private Integer exitCode;

    private Builder(RequestContext request) {
      if (request == null) {
        throw new PipeliteException("Missing job request");
      }
      if (request.jobId() == null) {
        throw new PipeliteException("Missing job id");
      }
      this.request = request;
      this.jobId = request.jobId();
    }

    public String jobId() {
      return jobId;
    }

    public Builder<RequestContext> attribute(String key, Object value) {
      if (key == null || value == null) {
        return this;
      }
      this.attributes.put(key, value.toString());
      return this;
    }

    public Builder<RequestContext> result(StageExecutorState state) {
      this.result = StageExecutorResult.create(state);
      return this;
    }

    public Builder<RequestContext> result(StageExecutorState state, Integer exitCode) {
      this.result = StageExecutorResult.create(state);
      this.exitCode = exitCode;
      return this;
    }

    public Builder<RequestContext> active() {
      this.result = StageExecutorResult.active();
      this.exitCode = null;
      return this;
    }

    public Builder<RequestContext> success() {
      this.result = StageExecutorResult.success();
      this.exitCode = Integer.valueOf(0);
      return this;
    }

    public Builder<RequestContext> timeoutError() {
      this.result = StageExecutorResult.timeoutError();
      this.exitCode = null;
      return this;
    }

    public Builder<RequestContext> lostError() {
      this.result = StageExecutorResult.lostError();
      this.exitCode = null;
      return this;
    }

    public Builder<RequestContext> executionError() {
      this.result = StageExecutorResult.executionError();
      this.exitCode = null;
      return this;
    }

    public Builder<RequestContext> executionError(Integer exitCode) {
      this.result = StageExecutorResult.executionError();
      this.exitCode = exitCode;
      return this;
    }

    public Builder<RequestContext> executionError(String exitCode) {
      this.result = StageExecutorResult.executionError();
      this.exitCode = null;
      if (exitCode != null && !exitCode.trim().isEmpty()) {
        try {
          this.exitCode = Integer.valueOf(exitCode);
        } catch (NumberFormatException ex) {
          throw new PipeliteException("Invalid exit code: " + exitCode);
        }
      }
      return this;
    }

    public boolean isCompleted() {
      return result != null && result.isCompleted();
    }

    public DescribeJobsResult<RequestContext> build() {
      if (result == null) {
        throw new PipeliteException("Missing job result");
      }
      if (attributes != null) {
        attributes.forEach((key, value) -> result.attribute(key, value));
      }
      if (jobId != null) {
        result.attribute(StageExecutorResultAttribute.JOB_ID, jobId);
      }
      if (exitCode != null) {
        result.attribute(StageExecutorResultAttribute.EXIT_CODE, String.valueOf(exitCode));
      }
      return DescribeJobsResult.create(request, result);
    }
  }
}
