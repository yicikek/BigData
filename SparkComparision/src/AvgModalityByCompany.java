import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class AvgModalityByCompany {

    public static void main(String[] args) {

        if (args.length < 1) {
            System.out.println("Usage: AvgModalityByCompany <inputfile>");
            System.exit(1);
        }

        long start = System.nanoTime();

        SparkConf conf = new SparkConf()
                .setAppName("Avg Modality By Company")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> data = sc.textFile(args[0]);

        // skip header
        JavaRDD<String> clean = data.filter(new org.apache.spark.api.java.function.Function<String, Boolean>() {
            public Boolean call(String s) {
                return !s.toLowerCase().contains("tool_name");
            }
        });

        long recordCount = clean.count();  // COUNT records here

        // (company, modalityCount)
        JavaPairRDD<String, Tuple2<Integer, Integer>> mapped = clean.flatMapToPair(
                new org.apache.spark.api.java.function.PairFlatMapFunction<String, String, Tuple2<Integer, Integer>>() {

                    public Iterable<Tuple2<String, Tuple2<Integer, Integer>>> call(String line) {
                        String[] f = line.split(",", -1);

                        if (f.length > 21) {
                            try {
                                String company = f[1].trim();
                                int modality = Integer.parseInt(f[21].trim());
                                return Arrays.asList(
                                        new Tuple2<String, Tuple2<Integer, Integer>>(company,
                                                new Tuple2<Integer, Integer>(modality, 1)));
                            } catch (Exception e) {
                            }
                        }
                        return Arrays.asList();
                    }
                });

        // Reduce: sum modality + count
        JavaPairRDD<String, Tuple2<Integer, Integer>> reduced = mapped.reduceByKey(
                new org.apache.spark.api.java.function.Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                        return new Tuple2<Integer, Integer>(a._1 + b._1, a._2 + b._2);
                    }
                });

        // Compute average
        JavaPairRDD<String, Double> avg = reduced.mapToPair(
                new org.apache.spark.api.java.function.PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, Double>() {

                    public Tuple2<String, Double> call(Tuple2<String, Tuple2<Integer, Integer>> x) {
                        double average = (double) x._2._1 / x._2._2;
                        return new Tuple2<String, Double>(x._1, average);
                    }
                });

        // Display results
        System.out.println("=== Avg Modality by Company ===");
        for (Tuple2<String, Double> t : avg.collect()) {
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
        System.out.println("Throughput (records/sec): " + throughput);
        System.out.println("Heap Used (MB): " + heap.getUsed() / (1024 * 1024));
        System.out.println("Non-Heap Used (MB): " + nonheap.getUsed() / (1024 * 1024));

        sc.close();
    }
}



