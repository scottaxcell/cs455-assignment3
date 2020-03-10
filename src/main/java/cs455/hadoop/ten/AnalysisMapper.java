package cs455.hadoop.ten;

import cs455.hadoop.AnalysisConstants;
import cs455.hadoop.Constants;
import cs455.hadoop.Utils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


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

        String tempoStr = songData[AnalysisConstants.TEMPO_INDEX];
        if (!Utils.isValidString(tempoStr))
            return;

        String loudnessStr = songData[AnalysisConstants.LOUDNESS_INDEX];
        if (!Utils.isValidString(loudnessStr))
            return;

        String durationStr = songData[AnalysisConstants.DURATION_INDEX];
        if (!Utils.isValidString(durationStr))
            return;

        StringBuilder stringBuilder = new StringBuilder(AnalysisConstants.MAPPER_TYPE);
        stringBuilder.append(String.format("%s%s", Constants.DELIMITER, tempoStr));
        stringBuilder.append(String.format("%s%s", Constants.DELIMITER, loudnessStr));
        stringBuilder.append(String.format("%s%s", Constants.DELIMITER, durationStr));

        context.write(new Text(songId), new Text(stringBuilder.toString()));
    }
}
