package cs455.hadoop.seven;

import cs455.hadoop.AnalysisConstants;
import cs455.hadoop.Constants;
import cs455.hadoop.Utils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * average each segment per song, add up each segment
 * start time, pitch, timbre, max loudness,
 * max loudness time, and start loudness
 */
class AnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] songData = value.toString().split(",");
        if (songData.length != AnalysisConstants.NUM_FIELDS)
            return;

        if (songData[AnalysisConstants.DUMMY_ZERO_INDEX].isEmpty())
            return;

        String songId = songData[AnalysisConstants.SONG_ID_INDEX];
        if (!Utils.isValidString(songId))
            return;

        String segmentsStartStr = songData[AnalysisConstants.SEGMENTS_START_INDEX];
        if (!Utils.isValidString(segmentsStartStr))
            return;
        Double startAvg = calculateAverage(segmentsStartStr);
        if (startAvg == null)
            return;

        String segmentsPitchStr = songData[AnalysisConstants.SEGMENTS_PITCHES_INDEX];
        if (!Utils.isValidString(segmentsPitchStr))
            return;
        Double pitchAvg = calculateAverage(segmentsPitchStr);
        if (pitchAvg == null)
            return;

        String segmentsTimbreStr = songData[AnalysisConstants.SEGMENTS_TIMBRE_INDEX];
        if (!Utils.isValidString(segmentsTimbreStr))
            return;
        Double timbreAvg = calculateAverage(segmentsTimbreStr);
        if (timbreAvg == null)
            return;

        String segmentsLoudnessMaxStr = songData[AnalysisConstants.SEGMENTS_LOUDNESS_MAX_INDEX];
        if (!Utils.isValidString(segmentsLoudnessMaxStr))
            return;
        Double loudnessMaxAvg = calculateAverage(segmentsLoudnessMaxStr);
        if (loudnessMaxAvg == null)
            return;

        String segmentsLoudnessmaxTimeStr = songData[AnalysisConstants.SEGMENTS_LOUDNESS_MAX_TIME_INDEX];
        if (!Utils.isValidString(segmentsLoudnessmaxTimeStr))
            return;
        Double loudnessMaxTimeAvg = calculateAverage(segmentsLoudnessmaxTimeStr);
        if (loudnessMaxTimeAvg == null)
            return;

        String segmentsLoudnessStartStr = songData[AnalysisConstants.SEGMENTS_LOUDNESS_START_INDEX];
        if (!Utils.isValidString(segmentsLoudnessStartStr))
            return;
        Double loudnessStartAvg = calculateAverage(segmentsLoudnessStartStr);
        if (loudnessStartAvg == null)
            return;

        StringBuilder stringBuilder = new StringBuilder(AnalysisConstants.MAPPER_TYPE);
        stringBuilder.append(String.format("%s%.3f", Constants.DELIMITER, startAvg.doubleValue()));
        stringBuilder.append(String.format("%s%.3f", Constants.DELIMITER, pitchAvg.doubleValue()));
        stringBuilder.append(String.format("%s%.3f", Constants.DELIMITER, timbreAvg.doubleValue()));
        stringBuilder.append(String.format("%s%.3f", Constants.DELIMITER, loudnessMaxAvg.doubleValue()));
        stringBuilder.append(String.format("%s%.3f", Constants.DELIMITER, loudnessMaxTimeAvg.doubleValue()));
        stringBuilder.append(String.format("%s%.3f", Constants.DELIMITER, loudnessStartAvg.doubleValue()));

        context.write(new Text(songId), new Text(stringBuilder.toString()));
    }

    private Double calculateAverage(String str) {
        String[] split = str.split(" ");
        if (split.length <= 0)
            return null;

        double average = 0;
        for (String s : split) {
            try {
                average += Double.parseDouble(s);
            }
            catch (NumberFormatException ignore) {
                return null;
            }
        }
        return average / split.length;
    }
}
