package pipelite.task.result.resolver;

/*
@Value
public class ExecutionResultValueResolver<T> extends ExecutionResultResolver<T> {

  private static final Logger logger = LoggerFactory.getLogger(ExecutionResultValueResolver.class);

  private final Map<T, ExecutionResult> map;
  private final List<ExecutionResult> list;

  @Override
  public ExecutionResult resolveError(T cause) {
    for (Map.Entry<T, ExecutionResult> entry : map.entrySet()) {
      if (entry.getKey().equals(cause)) {
        return entry.getValue();
      }
    }
    logger.error("Internal error. No execution result for cause: {}.", cause);
    return ExecutionResult.internalError();
  }

  @Override
  public List<ExecutionResult> results() {
    return list;
  }

  public static <T> Builder<T> builder(Class<T> causeType) {
    return new Builder<T>(causeType);
  }

  public static class Builder<T> {
    private Map<T, ExecutionResult> map = new HashMap<>();
    private List<ExecutionResult> list = new ArrayList<>();

    public Builder(Class<T> causeType) {}

    public Builder<T> success(T cause) {
      ExecutionResult result = ExecutionResult.success();
      this.map.put(cause, result);
      this.list.add(result);
      return this;
    }

    public Builder<T> transientError(T cause, String resultName) {
      ExecutionResult result = ExecutionResult.transientError(resultName);
      this.map.put(cause, result);
      this.list.add(result);
      return this;
    }

    public Builder<T> permanentError(T cause, String resultName) {
      ExecutionResult result = ExecutionResult.permanentError(resultName);
      this.map.put(cause, result);
      this.list.add(result);
      return this;
    }

    public ExecutionResultValueResolver<T> build() {
      return new ExecutionResultValueResolver(map, list);
    }
  }
}
*/
