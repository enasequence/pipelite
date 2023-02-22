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

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Stream;
import pipelite.exception.PipeliteException;
import pipelite.executor.describe.context.request.DefaultRequestContext;

/** Job results. */
public class DescribeJobsResults<RequestContext extends DefaultRequestContext> {

  private final ArrayList<DescribeJobsResult<RequestContext>> results = new ArrayList<>();

  private int foundCount = 0;
  private int lostCount = 0;

  /** Adds a job result. */
  public void add(DescribeJobsResult<RequestContext> result) {
    if (result == null) {
      throw new PipeliteException("Missing job result");
    }
    if (result.result.isLostError()) {
      lostCount++;
    } else {
      foundCount++;
    }
    results.add(result);
  }

  /**
   * Returns all describe job results.
   *
   * @return all describe job results.
   */
  public Collection<DescribeJobsResult<RequestContext>> get() {
    return results;
  }

  /**
   * Returns found describe job results.
   *
   * @return found describe job results.
   */
  public Stream<DescribeJobsResult<RequestContext>> found() {
    return results.stream().filter(r -> !r.result.isLostError());
  }

  /**
   * Returns lost describe job results.
   *
   * @return lost describe job results.
   */
  public Stream<DescribeJobsResult<RequestContext>> lost() {
    return results.stream().filter(r -> r.result.isLostError());
  }

  public int foundCount() {
    return foundCount;
  }

  public int lostCount() {
    return lostCount;
  }
}
