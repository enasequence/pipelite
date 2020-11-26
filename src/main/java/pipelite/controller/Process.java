package pipelite.controller;

import lombok.Builder;
import lombok.Value;

import java.time.LocalDateTime;

@Value
@Builder
public class Process {
  private String pipelineName;
  private String processId;
  private LocalDateTime currentExecutionStartTime;
  private String currentExecutionTime;
}
