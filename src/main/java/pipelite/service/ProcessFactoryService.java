package pipelite.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pipelite.process.ProcessFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ProcessFactoryService {

  private final Map<String, ProcessFactory> map = new HashMap<>();

  public ProcessFactoryService(@Autowired List<ProcessFactory> factories) {
    for (ProcessFactory factory : factories) {
      if (map.containsKey(factory.getPipelineName())) {
        throw new ProcessFactoryServiceException(
            "Non-unique pipeline: " + factory.getPipelineName());
      }
      map.put(factory.getPipelineName(), factory);
    }
  }

  /**
   * Creates a process factory.
   *
   * @param pipelineName the pipeline name. A pipeline is identified by its name.
   * @return the process factory for the pipeline.
   * @throws ProcessFactoryServiceException if the pipeline is not supported by this factory or if
   *     the pipeline name is null or empty.
   */
  public ProcessFactory create(String pipelineName) {
    if (pipelineName == null || pipelineName.trim().isEmpty()) {
      throw new ProcessFactoryServiceException("Missing pipeline name");
    }
    if (!map.containsKey(pipelineName)) {
      throw new ProcessFactoryServiceException("Unknown pipeline: " + pipelineName);
    }
    return map.get(pipelineName);
  }
}