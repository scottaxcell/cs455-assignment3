package cs455.hadoop;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {
    public static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("HH:mm:ss");
    private static boolean debug = true;

    public static void out(Object o) {
        System.out.print(o);
    }

    public static void info(Object o) {
        System.out.println("\nINFO: " + o);
    }

    public static void debug(Object o) {
        if (debug)
            System.out.println(String.format("DEBUG: [%s] %s", SIMPLE_DATE_FORMAT.format(new Date()), o));
    }

    public static void error(Object o) {
        System.err.println("\nERROR: " + o);
    }

    public static boolean isValidString(String str) {
        return str != null && !str.isEmpty() && !str.trim().isEmpty();
    }

    public static boolean isGreaterThanZero(Double number) {
        return number != null && number > 0;
    }

    /**
     * This method computes the median of the values in the
     * input array.
     * http://pages.cs.wisc.edu/~cs302-5/resources/examples/MeanMedianMode_Methods.java
     *
     * @param arr - an array of ints
     * @return median - the median of the input array
     */
    public static Double calculateMedian(Double[] arr)
    {

        // Sort our array
        Double[] sortedArr = arr;// bubbleSort(arr);

        double median = 0;

        // If our array's length is even, then we need to find the average of the two centered values
        if (arr.length % 2 == 0)
        {
            int indexA = (arr.length - 1) / 2;
            int indexB = arr.length / 2;

            median = (sortedArr[indexA] + sortedArr[indexB]) / 2;
        }
        // Else if our array's length is odd, then we simply find the value at the center index
        else
        {
            int index = (sortedArr.length - 1) / 2;
            median = sortedArr[ index ];
        }

        // Print the values of the sorted array
//        for (double v : sortedArr)
//        {
//            System.out.println(v);
//        }

        return median;
    }
}
