package rossio.util;

public interface Handler<TYPE> {
	public boolean handle(TYPE resource) throws Exception;
}
