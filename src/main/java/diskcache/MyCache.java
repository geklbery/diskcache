package diskcache;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is a test task Cache implementation which makes some technique of 2-level caching.
 * We consider caching solution for generic Serializable objects, which keep some payload data, identified by integer keys
 * The basic prerequisites are:
 * - 1 level is a bounded memory hash Map
 *
 * - 2 level is emulation of external (disk) storage provided with swapIn(int key, T data), swapOut(int key) methods, which make a object-per-time
 *     swapping into/from uniquely named file in the specified directory location via Serializable API. Swap file name is the same as object key.
 *     NOTE: this is far from realistic interface of external cache storage, which would typically operate with memory blocks instead of
 *     single elements of cause. But for the sake of test task simplicity we choose that one.
 *
 * - the cycle of element in/out swapping is as follows - a) as soon as memory cache is out of space, a single slot element (object) is extruded from
 *   memory to disk and new one is put into 1-level memory cache; b) when searching for object by key if the element is not found in memory cache it is being
 *   brought from disk 2-level and pushed into memory 1-level cache according the same (a) algorithm above, the backing file is deleted
 *   NOTE: object requested from cache could be not present neither in memory nor in disk storage (if the key we call was not cached before) - that cache get
 *   call will return null pointer. We use internal 'swapTable' collection to exactly track were is each object located at every moment.
 *
 * - we use two kinds of configurable swapping strategy: the LRU and RAND policy for least-recently-used and random object extruding correspondingly
 *
 * - to get a more realistic model we will try to implement a variant of a concurrent cache, where multiple threads are congesting for cache storage
 *   and try to optimize processing in multi-thread environment, particularly using read buffer for processing cache access calls.
 *   Few configurable parameters from conf.properties can be used to change size of buffers, etc. (see comments in conf.properties)
 */

public class MyCache<T extends Serializable> implements Cache<T> {

    Random random = new Random();

    /* external disk storage */
    private final String swapDirectory = "swap";
    private final int swapMaxSize;
    private int swapCurrentSize=0;

    /* memory cache */
    private final int cacheMaxSize;
    private final ConcurrentMap<Integer, T> cache = new ConcurrentHashMap<>();

    /* read buffer for cache access calls deffered processing */
    private final int readBufferSize;
    private final int[] readBuffer;
    private int readBufferFreeIdx = 0;  /* index of next free element in read buffer */

    /* key sequence generator */
    private final AtomicInteger keySequence = new AtomicInteger(1);

    /* lru queue, strategy */
    //private Deque<Integer> lru = new ConcurrentLinkedDeque<>();
    private final LinkedList<Integer> lru = new LinkedList<>();
    private final Strategy lruStrategy;

    /* swap flags collection, shows if object with given key is swapped
     * Note: swapTable keeps jointly all objects in memory and on disk
      *      so if the key is absent from swapTable it is not in cache
     */
    private final Map<Integer, Boolean> swapTable = new HashMap<>();


    public MyCache(int cacheMaxSize, int swapMaxSize, int readBufferSize, Strategy strategy) throws Exception {
        this.cacheMaxSize = cacheMaxSize > 0 ? cacheMaxSize : 50;
        this.swapMaxSize = swapMaxSize > 0 ? swapMaxSize : 100;
        this.readBufferSize = readBufferSize > 0 ? readBufferSize: 5;
        this.lruStrategy = strategy;
        this.readBuffer = new int[this.readBufferSize];
    }

    /**
     * Put data element into cache
     * @param data
     * @return
     */
    public int putToCache(T data) throws Exception {
        return doPutToCache(-1, data);
    }


    /**
     * Perform the actual data saving into cache
     * @param theKey key if parameter > 0, otherwise the key is auto-generated from key sequence
     * @param data
     * @return the key under which the data is stored in cache
     */
    private int doPutToCache(int theKey, T data) throws Exception {
        int key;
        if (theKey > 0) {
            key = theKey;
        } else {
            key = keySequence.getAndIncrement();
        }

        synchronized (cache) {
            if (cache.size() == cacheMaxSize) {
                /* need to swap out */

                /* get least-recently-used id */
                int keyToSwap = -1;
                T dataToSwap = null;
                do {
                    switch (lruStrategy) {
                        case RAND:
                            synchronized (readBuffer) {
                                keyToSwap = lru.get(random.nextInt(lru.size()));
                            }
                            break;
                        case LRU:
                        default:
                            keyToSwap = lru.poll();
                            break;
                    }
                    dataToSwap = cache.get(keyToSwap);
                } while (keyToSwap == key || dataToSwap == null);

                synchronized (swapTable) {
                    /* do swap out */
                    Boolean flagOld = swapTable.get(keyToSwap);
                    synchronized (flagOld) {
                        swapOut(keyToSwap, dataToSwap);
                        cache.remove(keyToSwap);
                    }

                    /* update cache */
                    cache.put(key, data);

                    /* update swapTable */
                    flagOld = new Boolean(true);
                    swapTable.put(keyToSwap, flagOld);
                    Boolean flagNew = new Boolean(false);
                    swapTable.put(key, flagNew);
                }
            } else {
                /* enough place in cache */
                cache.put(key, data);
                swapTable.put(key, false);
            }
        }

        /* update lru */
        buffer(key);

        return key;
    }

    /**
     * Get data element from cache
     * @param key key
     * @return data element
     */
    public T getFromCache(int key) throws Exception {
        T data = null;
        boolean swapIn = false;


        Boolean flag = null;
        synchronized (swapTable) {
            flag = swapTable.get(key);

            /* object is not in cache */
            if (flag == null) {
                return null;
            }

            /* mark object to swap in */
            if (flag) {
                flag = new Boolean("false");
                swapTable.put(key, flag);
                swapIn=true;
            }
        }

        synchronized (flag) {
            if (swapIn) {
            /* swap in object from level_1 <= level_2 */
                data = swapIn(key);
                doPutToCache(key, data);
            } else {
            /* object is in level_1 */
                data = cache.get(key);
            }
        }

        return data;
    }

    /**
     * Save to read buffer keys for later batch processing, which triggers when read buffer is full
     * @param key key
     */
    private void buffer(int key) {
        synchronized (readBuffer) {
            readBuffer[readBufferFreeIdx++] = key;
            if (readBufferFreeIdx == readBuffer.length) {
                processBuffer();
            }
        }
    }

    /**
     * Process keys in read buffer: reinsert each into lru
     */
    private void processBuffer() {
        for (int i = 0; i < readBufferFreeIdx; i++) {
            int id = readBuffer[i];
            ((Queue)lru).remove(id);
            if (cache.containsKey(id)) {
                lru.add(id);
            }
        }
        readBufferFreeIdx = 0;
    }

    /**
     * get object from filesystem
     * @param key element key
     * @return data or null if not found
     *
     */
    private T swapIn(int key) throws Exception {
        T result;
        if (!Files.exists(Paths.get(swapDirectory + "\\" + key))) {
            throw new Exception("Swap file not found: " + swapDirectory + "\\" + key);
        }
        ObjectInputStream is=null;
        try {
            is = new ObjectInputStream(new FileInputStream(swapDirectory + "\\" + key));
            result = (T) is.readObject();
        } finally {
            if (is != null) {
                is.close();
            }
        }
        Files.delete(Paths.get(swapDirectory + "\\" + key));
        /*try {

        } catch (Exception e) {
            e.printStackTrace();
        }*/
        swapCurrentSize--;
        return result;
    }

    /**
     * save object to filesystem
     * @param key element key
     * @param data data
     *
     */
    private void swapOut(int key, T data) throws Exception {
        if (swapCurrentSize == swapMaxSize) {
            throw new Exception("Swap disk size exceeded\n");
        }
        ObjectOutputStream os=null;
        try {
            os = new ObjectOutputStream(new FileOutputStream(swapDirectory + "\\" + key));
            os.writeObject(data);
        } finally {
            if (os != null) {
                os.close();
            }
        }
        swapCurrentSize++;
    }

}
