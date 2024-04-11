import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Random;
import java.util.*;

public class G045HW1 {
    public static void main(String[] args) {
        // Initialize Spark configuration and context
        SparkConf conf = new SparkConf().setAppName("RCounting").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the input data as an RDD of edges
        JavaRDD<String> input = sc.textFile("facebook_large.txt");
        int C = 1; // number of colors
        int R = 1; // Number of repeat


        JavaPairRDD<Integer, Integer> edges = input.mapToPair(line -> {
            String[] parts = line.split(",");
            int u = Integer.parseInt(parts[0]);
            int v = Integer.parseInt(parts[1]);
            return new Tuple2<>(u, v);
        });



        long[] totalTime_color = new long[20];
        long[] t_final_color = new long[20];
        for (int i=0;i<R;i++) {
            long startTime1 = System.currentTimeMillis();
            //count number of triangles by coloring
            t_final_color[i]=MR_ApproxTCwithNodeColors(edges, C);
            long endTime1 = System.currentTimeMillis();
            totalTime_color[i] = endTime1 - startTime1;
        }

        double median_t_final_color=getMedian(t_final_color,R);

        long startTime2 = System.currentTimeMillis();
        //count number of triangles by Partitions
        Long t_final_partition =MR_ApproxTCwithSparkPartitions(edges, C);
        long endTime2 = System.currentTimeMillis();
        long totalTime_Partitions  = endTime2 - startTime2;

        long numEdges = edges.count();
        System.out.println("Dataset = " + input.toString());
        System.out.println("Number of Edges = " + numEdges);
        System.out.println("Number of Colors = " + C);
        System.out.println("Number of Repetitions = " + R);

        // Print results of MR_ApproxTCwithNodeColors
        System.out.println("Approximation through node coloring");
        System.out.println("- Number of triangles (median over " + R + " runs) = " + (int)median_t_final_color);
        System.out.println("- Running time (average over " + R + " runs) = " + Arrays.stream(totalTime_color).average() + " ms");
        // Print result of MR_ApproxTCwithSparkPartitions
        System.out.println("Approximation through Spark partitions");
        System.out.println("- Number of triangles = " + t_final_partition);
        System.out.println("- Running time = " + totalTime_Partitions + " ms");

        sc.stop();
    }

    //C : number of colors
    //count number of triangles by coloring
    public static Long MR_ApproxTCwithNodeColors(JavaPairRDD<Integer, Integer> edges,int C)
    {
        Random rand = new Random();
        int p = 8191; // prime number
        int a = rand.nextInt(p)+1;/// random integer in [1, p-1]
        int b = rand.nextInt(p); // random integer in [0, p-1]

        // Partition the edges by color using the hash function
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> coloredEdges = edges.flatMapToPair(edge -> {
            int u = edge._1();
            int v = edge._2();
            int color = ((a * u + b) % p) % C;
            if (color == ((a * v + b) % p) % C) {
                return Collections.singletonList(new Tuple2<>(color, new Tuple2<>(u, v))).iterator();
            } else {
                return Collections.emptyIterator();
            }
        }).partitionBy(new HashPartitioner(C));

        // Compute the number of triangles for each color
        JavaPairRDD<Integer, Long> triangleCounts = coloredEdges.groupByKey().mapValues(edgeList -> {
            // Create an adjacency list for the vertices in the edge set
            Map<Integer, Set<Integer>> adjacencyLists = new HashMap<>();
            for (Tuple2<Integer, Integer> edge : edgeList) {
                int u = edge._1();
                int v = edge._2();
                adjacencyLists.computeIfAbsent(u, k -> new HashSet<>()).add(v);
                adjacencyLists.computeIfAbsent(v, k -> new HashSet<>()).add(u);
            }

            // Count the number of triangles in the edge set
            long numTriangles = 0;
            for (int u : adjacencyLists.keySet()) {
                Set<Integer> uNeighbors = adjacencyLists.get(u);
                for (int v : uNeighbors) {
                    if (v > u) {
                        Set<Integer> vNeighbors = adjacencyLists.get(v);
                        for (int w : vNeighbors) {
                            if (w > v && uNeighbors.contains(w)) {
                                numTriangles++;
                            }
                        }
                    }
                }
            }

            return numTriangles;
        });

        // Print the triangle counts for each color
        Long t=0l;
        List<Tuple2<Integer, Long>> counts = triangleCounts.collect();
        for (Tuple2<Integer, Long> count : counts) {
            t += count._2();
            //System.out.println("Color " + count._1() + ": " + count._2());
        }
        Long t_final = (C*C)*t;

        return  t_final;
    }

    //C : number of partitions
    //count number of triangles by partitioning
    public static long MR_ApproxTCwithSparkPartitions(JavaPairRDD<Integer, Integer> edges,int C)
    {
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> partitionedEdges = edges.mapToPair(edge -> {
            Random rand = new Random();
            int partitionID = rand.nextInt(C);
            return new Tuple2<>(partitionID, edge);
        }).cache();

        // Compute the number of triangles formed by edges of each subset
        JavaPairRDD<Integer, Long> triangleCounts = partitionedEdges.groupByKey().mapValues(edgeList -> {
            // Create an adjacency list for the vertices in the edge set
            Map<Integer, Set<Integer>> adjacencyLists = new HashMap<>();
            for (Tuple2<Integer, Integer> edge : edgeList) {
                int u = edge._1();
                int v = edge._2();
                adjacencyLists.computeIfAbsent(u, k -> new HashSet<>()).add(v);
                adjacencyLists.computeIfAbsent(v, k -> new HashSet<>()).add(u);
            }

            // Count the number of triangles in the edge set
            long numTriangles = 0;
            for (int u : adjacencyLists.keySet()) {
                Set<Integer> uNeighbors = adjacencyLists.get(u);
                for (int v : uNeighbors) {
                    if (v > u) {
                        Set<Integer> vNeighbors = adjacencyLists.get(v);
                        for (int w : vNeighbors) {
                            if (w > v && uNeighbors.contains(w)) {
                                numTriangles++;
                            }
                        }
                    }
                }
            }

            return numTriangles;
        });


        // Print the triangle counts for each color
        Long t=0l;
        List<Tuple2<Integer, Long>> counts = triangleCounts.collect();
        for (Tuple2<Integer, Long> count : counts) {
            t += count._2();
        }
        Long t_final = (C*C)*t;
        return  t_final;
    }


    public static double getMedian(long[] arr,int R) {
        // Sort the array in ascending order.
        Arrays.sort(arr,0,R);

        // Find the last non-zero element in the sorted array.
        int i = R - 1;
        while (i >= 0 && arr[i] == 0) {
            i--;
        }
        // If there are no non-zero elements, return 0 as the median.
        if (i < 0) {
            return 0;
        }
        // If there is only one non-zero element, return that element as the median.
        if (i == 0) {
            return arr[0];
        }
        // If there are an odd number of non-zero elements, return the middle element as the median.
        if (i % 2 == 0) {
            int mid = i / 2;
            return arr[mid];
        }
        // If there are an even number of non-zero elements, return the average of the two middle elements as the median.
        else {
            int mid1 = i / 2;
            int mid2 = mid1 + 1;
            return (arr[mid1] + arr[mid2]) / 2.0;
        }
    }

}

