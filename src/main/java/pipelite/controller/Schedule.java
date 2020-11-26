package pipelite.controller;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Schedule {
  private String pipelineName;
  private String cron;
  private String description;
}
