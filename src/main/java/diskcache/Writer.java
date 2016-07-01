package diskcache;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Writes number of elements to 2-level cache saving them into controlling map for check
 */
public class Writer implements Runnable {
    /* controlling map of saved key-value for check */
    ConcurrentMap<Integer, CacheObject> savedKeys;
    private final MyCache<CacheObject> cache;
    private final int numObjectsToWrite;

    /* character string generation */
    private final int lettersSize = 'Z' -'A' +1;
    private final char[] letters;
    private int stringFieldSize;


    public Writer(MyCache<CacheObject> cache, ConcurrentMap savedKeys, int numObjectsToWrite, int stringFieldSize) {
        this.cache = cache;
        this.savedKeys = savedKeys;
        this.numObjectsToWrite = numObjectsToWrite > 0 ? numObjectsToWrite : 10;
        this.stringFieldSize = stringFieldSize > 0 ? stringFieldSize : 10;
        ThreadLocalRandom.current();
        letters = new char[lettersSize];
        int counter=0;
        for (char i='A'; i<='Z'; i++) {
            letters[counter++] = i;
        }
    }

    @Override
    public void run() {
        for (int i = 0; i < numObjectsToWrite; i++) {
            CacheObject data = new CacheObject();
            StringBuilder dataStr = new StringBuilder();
            int dataInt = ThreadLocalRandom.current().nextInt();

            for (int j = 0; j < stringFieldSize; j++) {
                int lettersIdx = ThreadLocalRandom.current().nextInt(0, lettersSize);
                char letter = letters[lettersIdx];
                dataStr.append(letter);
            }
            data.setStringField(dataStr.toString());
            data.setIntField(dataInt);

            int id = 0;
            try {
                id = cache.putToCache(data);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.printf("Exiting\n");
                System.exit(1);
            }
            savedKeys.put(id, data);
            System.out.printf("Saved to cache: id = %d, data = %s\n", id, data);
        }
    }
}
