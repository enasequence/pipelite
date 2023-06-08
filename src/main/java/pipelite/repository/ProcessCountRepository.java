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
package pipelite.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ProcessCountRepository {

  private final JdbcTemplate jdbcTemplate;

  public ProcessCountRepository(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  /**
   * Finds process backlog count.
   *
   * @param pipelineName the pipeline name
   * @return the process backlog count.
   */
  public int findProcessBacklogCount(String pipelineName) {
    String sql =
        "select count(1) from pipelite2_process where state in ('PENDING', 'ACTIVE') and\n"
            + "pipeline_name = ?";
    return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> rs.getInt(1), pipelineName);
  }

  /**
   * Finds process failed count.
   *
   * @param pipelineName the pipeline name
   * @return the process failed count.
   */
  public int findProcessFailedCount(String pipelineName) {
    String sql =
        "select count(1) from pipelite2_process where state in ('FAILED') and\n"
            + "pipeline_name = ?";
    return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> rs.getInt(1), pipelineName);
  }
}
