import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class ReleaseYearCountSpark {

    public static void main(String[] args) {

        if (args.length < 1) {
            System.out.println("Usage: ReleaseYearCountSpark <inputfile>");
            System.exit(1);
        }

        long start = System.nanoTime();

        SparkConf conf = new SparkConf()
                .setAppName("Release Year Count (Spark)")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> data = sc.textFile(args[0]);

        // Skip header
        JavaRDD<String> clean = data.filter(new org.apache.spark.api.java.function.Function<String, Boolean>() {
            public Boolean call(String s) {
                return !s.toLowerCase().contains("tool_name");
            }
        });

        // Count number of records
        final long recordCount = clean.count();

        // Extract (year, 1)
        JavaPairRDD<String, Integer> mapped = clean.flatMapToPair(
                new org.apache.spark.api.java.function.PairFlatMapFunction<String, String, Integer>() {

                    public Iterable<Tuple2<String, Integer>> call(String line) {

                        String[] f = line.split(",", -1);

                        if (f.length > 9) {
                            String year = f[9].trim();
                            if (year.length() > 0) {
                                return Arrays.asList(new Tuple2<String, Integer>(year, 1));
                            }
                        }

                        return Arrays.asList();
                    }
                });

        // Reduce: sum counts
        JavaPairRDD<String, Integer> reduced = mapped.reduceByKey(
                new org.apache.spark.api.java.function.Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer a, Integer b) {
                        return a + b;
                    }
                });

        // Print results
        System.out.println("=== Release Year Count ===");
        for (Tuple2<String, Integer> t : reduced.collect()) {
            System.out.println(t._1 + " : " + t._2);
        }

        long end = System.nanoTime();
        double seconds = (end - start) / 1e9;

        // Calculate throughput
        double throughput = recordCount / seconds;

        // Memory info
        MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heap = memBean.getHeapMemoryUsage();
        MemoryUsage nonheap = memBean.getNonHeapMemoryUsage();

        System.out.println("\n=== Performance Metrics ===");
        System.out.println("Execution Time (s): " + seconds);
        System.out.println("Records Processed: " + recordCount);
        System.out.println("Throughput (records/s): " + throughput);
        System.out.println("Heap Used (MB): " + heap.getUsed() / (1024 * 1024));
        System.out.println("Non-Heap Used (MB): " + nonheap.getUsed() / (1024 * 1024));

        sc.close();
    }
}



