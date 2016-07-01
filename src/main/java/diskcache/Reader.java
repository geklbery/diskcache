package diskcache;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Reads elements from 2-level cache infinitely.
 * The keys to read are taken from controlling map. Checks data read against controlling map.
 */
public class Reader implements Runnable {
    /* controlling map of saved key-value for check */
    private final ConcurrentMap<Integer, CacheObject> savedKeys;
    private final MyCache cache;

    public Reader(MyCache cache, ConcurrentMap savedKeys) {
        this.savedKeys = savedKeys; /* keys to read from cache */
        this.cache = cache;
        ThreadLocalRandom.current();
    }

    @Override
    public void run() {

        while(true) {
            List<Integer> theKeys = new ArrayList<>(savedKeys.keySet());
            Collections.shuffle(theKeys);

            for (Integer id : theKeys) {
                CacheObject data=null;
                try {
                    data =  (CacheObject)cache.getFromCache(id);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.printf("Exiting\n");
                    System.exit(1);
                }
                CacheObject savedData = savedKeys.get(id);

                if (!(data.getIntField() == savedData.getIntField() && data.getStringField().equals(savedData.getStringField()))) {
                    System.out.printf("!!! READER ALARM !!! - HAVE READ MALICIOUS DATA. EXITING \n");
                    System.exit(1);
                }

                System.out.printf("Read from cache: id = %d, data = %s\n", id, data);
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
