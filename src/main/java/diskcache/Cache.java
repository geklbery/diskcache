package diskcache;

import java.io.Serializable;

/**
 * Created by admin on 23.06.2016.
 */
public interface Cache<T extends Serializable> {
    int putToCache(T data) throws Exception;
    T getFromCache(int id) throws Exception;
}
