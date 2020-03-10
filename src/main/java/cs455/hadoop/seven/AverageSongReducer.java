package cs455.hadoop.seven;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static cs455.hadoop.Constants.DELIMITER;

class AverageSongReducer extends Reducer<Text, Text, Text, Text> {
    private static final String START = "Start time average:";
    private static final String TIMBRE = "Timbre average:";
    private static final String PITCH = "Pitch average:";
    private static final String LOUDNESS_MAX = "Max loudness average:";
    private static final String LOUDNESS_MAX_TIME = "Max loudness time average:";
    private static final String LOUDNESS_START = "Start loudness average:";

    class Segment {
        int count = 0;
        double value = 0;
    }

    Map<String, Segment> segments;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        segments = new HashMap<>();
        segments.put(START, new Segment());
        segments.put(TIMBRE, new Segment());
        segments.put(PITCH, new Segment());
        segments.put(LOUDNESS_MAX, new Segment());
        segments.put(LOUDNESS_MAX_TIME, new Segment());
        segments.put(LOUDNESS_START, new Segment());
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] split = value.toString().split(DELIMITER);
            if (split.length != 7)
                return;

            segments.get(START).count++;
            segments.get(START).value += Double.parseDouble(split[1]);

            segments.get(TIMBRE).count++;
            segments.get(TIMBRE).value += Double.parseDouble(split[2]);

            segments.get(PITCH).count++;
            segments.get(PITCH).value += Double.parseDouble(split[3]);

            segments.get(LOUDNESS_MAX).count++;
            segments.get(LOUDNESS_MAX).value += Double.parseDouble(split[4]);

            segments.get(LOUDNESS_MAX_TIME).count++;
            segments.get(LOUDNESS_MAX_TIME).value += Double.parseDouble(split[5]);

            segments.get(LOUDNESS_START).count++;
            segments.get(LOUDNESS_START).value += Double.parseDouble(split[6]);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("QUESTION 7"), new Text());
        context.write(new Text("=========="), new Text());

        context.write(new Text(START), new Text(calculateAverage(START)));
        context.write(new Text(TIMBRE), new Text(calculateAverage(TIMBRE)));
        context.write(new Text(PITCH), new Text(calculateAverage(PITCH)));
        context.write(new Text(LOUDNESS_MAX), new Text(calculateAverage(LOUDNESS_MAX)));
        context.write(new Text(LOUDNESS_MAX_TIME), new Text(calculateAverage(LOUDNESS_MAX_TIME)));
        context.write(new Text(LOUDNESS_START), new Text(calculateAverage(LOUDNESS_START)));

        context.write(new Text(), new Text());

        super.cleanup(context);
    }

    private String calculateAverage(String key) {
        Segment segment = segments.get(key);
        return String.valueOf(segment.value / segment.count);
    }
}
