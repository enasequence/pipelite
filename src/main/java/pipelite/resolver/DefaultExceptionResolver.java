package pipelite.resolver;

public class DefaultExceptionResolver extends ExceptionResolver {

  public DefaultExceptionResolver() {
    super(ExceptionResolver.builder().build());
  }
}
