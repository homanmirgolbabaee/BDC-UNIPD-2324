import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.Math;
import java.util.*;

public class G012HW1 {
    public static void main(String[] args) throws IOException {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // Parameters are: <path_to_file>  D M K L
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        if (args.length != 5) {
            throw new IllegalArgumentException("USAGE: file_path D M K L");
        }

        System.out.println(args[0] + " D=" + args[1] + " M=" + args[2] + " K=" + args[3] + " L=" + args[4]);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        SparkConf conf = new SparkConf(true).setAppName("G012HW1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // float D, and 3 integers M,K,L
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Read number of partitions
        float D = Float.parseFloat(args[1]);
        int M = Integer.parseInt(args[2]);
        int K = Integer.parseInt(args[3]);
        int L = Integer.parseInt(args[4]);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // Reads the input points into an RDD of strings (called rawData)
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        JavaRDD<String> rawData = sc.textFile(args[0]).cache();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // Transforms it into an RDD of points (called inputPoints), represented as pairs of floats, subdivided into L partitions.
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        JavaPairRDD<Float, Float> inputPoints =
                rawData.flatMapToPair((document) -> {
                    String[] tokens = document.split(",");
                    ArrayList<Tuple2<Float, Float>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        pairs.add(new Tuple2<>(Float.parseFloat(tokens[0]), Float.parseFloat(tokens[1])));
                    }
                    return pairs.iterator();
                }).repartition(L).cache();

        long numpoints = inputPoints.count() / 2;

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // Prints the total number of points.
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        System.out.println("Number of points = " + numpoints);

        if (numpoints <= 200000){
            List <Tuple2<Float,Float>> listOfPoints = inputPoints.collect();
            long startTimeExactOutliers = System.currentTimeMillis();
            ExactOutliers(listOfPoints, D, M, K);
            long endTimeExactOutliers = System.currentTimeMillis();
            System.out.println("Running time of ExactOutliers = " + (endTimeExactOutliers - startTimeExactOutliers) + " ms");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // Stub for the MRApproxOutliers function.
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        /*long startTimeMRApproxOutliers = System.currentTimeMillis();
        MRApproxOutliers(inputPoints, D, M, K);
        long endTimeMRApproxOutliers = System.currentTimeMillis();
        System.out.println("Running time of MRApproxOutliers = " + (endTimeMRApproxOutliers - startTimeMRApproxOutliers) + " ms");*/

    }

    public static void ExactOutliers(List <Tuple2<Float,Float>> listOfPoints, float D, float M, float K){
        long numOfOutliers = 0;
        List <Tuple2<Long, Tuple2<Float,Float>>> listOfOutliers = new ArrayList<>();
        for (int i = 0; i < listOfPoints.size() / 2; i++) {
            long numOfNeighboursForPoint = 0;
            Tuple2<Float,Float> firstPoint = listOfPoints.get(i);
            for (int j = 0; j < listOfPoints.size() / 2; j++) {
                Tuple2<Float,Float> secondPoint = listOfPoints.get(j);
                if (Math.sqrt(Math.pow((firstPoint._1 - secondPoint._1), 2) + Math.pow((firstPoint._2 - secondPoint._2), 2)) <= D)
                    numOfNeighboursForPoint++;
            }
            if (numOfNeighboursForPoint <= M){
                numOfOutliers++;
                listOfOutliers.add(new Tuple2<>(numOfNeighboursForPoint, firstPoint));
            }
        }
        System.out.println("Number of Outliers = " + numOfOutliers);

        Collections.sort(listOfOutliers, Comparator.comparingLong(Tuple2::_1));

        float numOfPrintedOutliers = (numOfOutliers > K) ? K : numOfOutliers;

        for (int i = 0; i < Math.floor(numOfPrintedOutliers); i++) {
            System.out.println("Point: (" + listOfOutliers.get(i)._2._1 + "," + listOfOutliers.get(i)._2._2 + ")");
        }

    }


}

