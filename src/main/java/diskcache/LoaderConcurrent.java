package diskcache;

import java.io.FileInputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Create a set of Writer and a set of Reader threads
 * Writers going to write new entries into 2-level cache and save every element they wrote into a controlling map 'savedObjects'
 * Readers will be reading the keys to read from 'savedObjects' and perform reading infinitely, checking if the data they have read is the same
 * that is stored in controlling map. In case reader finds divergence, it stops the program.
 * We will wait until all writers are done and then exit
 * NOTE: in case of disk buffer size overflow we throw Exception and the program terminates
 */
public class LoaderConcurrent {

    private final MyCache<CacheObject> cache;

    /* helper collections */
    private final ConcurrentMap<Integer, CacheObject> savedObjects = new ConcurrentHashMap<>();

    private final int lettersSize = 'Z' -'A' +1;
    private final char[] letters = new char[lettersSize];


    public LoaderConcurrent(MyCache<CacheObject> cache) {
        this.cache = cache;

        /* init array for string generation */
        int counter=0;
        for (char i='A'; i<='Z'; i++) {
            letters[counter++] = i;
        }
    }


    public void execute(int writeThreadNum, int readThreadNum, int numObjectsToWrite, int stringFieldSize) throws Exception {

        System.out.printf("Loader: start loading with \n");
        System.out.printf("Write threads: %s\n", writeThreadNum);
        System.out.printf("Read threads: %s\n", readThreadNum);
        System.out.printf("Number of Objects to write per threads: %s\n", numObjectsToWrite > 0 ? numObjectsToWrite : "default (10)");
        System.out.printf("Object String field size: %s\n", stringFieldSize > 0 ? stringFieldSize : "default (10)");
        System.out.printf("-------------------------------------------\n");

        Thread writers[] = new Thread[writeThreadNum];
        Thread readers[] = new Thread[readThreadNum];
        for (int i = 0; i < writeThreadNum; i++) {
            Writer writer = new Writer(cache, savedObjects, numObjectsToWrite, stringFieldSize);
            writers[i] = new Thread(writer);
            writers[i].start();
        }
        for (int i = 0; i < readThreadNum; i++) {
            Reader reader = new Reader(cache, savedObjects);
            readers[i] = new Thread(reader);
            readers[i].start();
        }

        for (int i = 0; i <writers.length; i++) {
            try {
                writers[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.printf("=============== ALL WRITERS FINISHED ====================\n");
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        /* read config.properties , create cache */
        int cacheMaxSize;
        int swapMaxSize;
        int readBufferSize;
        String swapStrategyStr;
        Strategy swapStrategy;
        int numWrites;
        int stringFieldSize;
        FileInputStream in=null;
        try {
            in = new FileInputStream("config.properties");
            Properties prop = new Properties();
            prop.load(in);
            cacheMaxSize = Integer.parseInt(prop.getProperty("cache_max_size", "0"));
            swapMaxSize = Integer.parseInt(prop.getProperty("swap_max_size", "0"));
            readBufferSize = Integer.parseInt(prop.getProperty("read_buffer_size", "0"));
            swapStrategyStr = prop.getProperty("swap_strategy", "lru").toLowerCase();
            switch (swapStrategyStr) {
                case "lru":
                    swapStrategy = Strategy.LRU;
                    break;
                case "rand":
                    swapStrategy = Strategy.RAND;
                    break;
                default:
                    throw new Exception("Properties: unsupported strategy. Use (LRU | RAND)");
            }
            numWrites = Integer.parseInt(prop.getProperty("num_writes_per_thread", "0"));
            stringFieldSize = Integer.parseInt(prop.getProperty("string_field_size", "0"));
        } finally {
            if (in != null) {
                in.close();
            }
        }
        MyCache<CacheObject> cache = new MyCache<>(cacheMaxSize, swapMaxSize, readBufferSize, swapStrategy);
        System.out.printf("Loader: create cache with\n");
        System.out.printf("max size: %s\n", cacheMaxSize > 0 ? cacheMaxSize : "default (50)");
        System.out.printf("max size: %s\n", swapMaxSize > 0 ? swapMaxSize : "default (100)");
        System.out.printf("max size: %s\n", readBufferSize > 0 ? readBufferSize : "default (5)");
        System.out.printf("max size: %s\n\n", swapStrategy );

        /* start loader */
        LoaderConcurrent loader = new LoaderConcurrent(cache);
        loader.execute(5, 5, numWrites, stringFieldSize);
    }
}
