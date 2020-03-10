package cs455.hadoop.nine;

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

        String timeSignatureStr = songData[AnalysisConstants.TIME_SIGNATURE_INDEX];
        if (!Utils.isValidString(timeSignatureStr))
            return;

        String danceabilityStr = songData[AnalysisConstants.DANCEABILITY_INDEX];
        if (!Utils.isValidString(danceabilityStr))
            return;

        String durationStr = songData[AnalysisConstants.DURATION_INDEX];
        if (!Utils.isValidString(durationStr))
            return;

        String modeStr = songData[AnalysisConstants.MODE_INDEX];
        if (!Utils.isValidString(modeStr))
            return;

        String energyStr = songData[AnalysisConstants.ENERGY_INDEX];
        if (!Utils.isValidString(energyStr))
            return;

        String keyStr = songData[AnalysisConstants.KEY_INDEX];
        if (!Utils.isValidString(keyStr))
            return;

        String loudnessStr = songData[AnalysisConstants.LOUDNESS_INDEX];
        if (!Utils.isValidString(loudnessStr))
            return;

        String endOfFadeInStr = songData[AnalysisConstants.END_OF_FADE_IN_INDEX];
        if (!Utils.isValidString(endOfFadeInStr))
            return;

        String startOfFadeOutStr = songData[AnalysisConstants.START_OF_FADE_OUT_INDEX];
        if (!Utils.isValidString(startOfFadeOutStr))
            return;

        String hotttnesssStr = songData[AnalysisConstants.SONG_HOTTTNESSS_INDEX];
        if (!Utils.isValidString(hotttnesssStr))
            return;

        StringBuilder stringBuilder = new StringBuilder(AnalysisConstants.MAPPER_TYPE);
        stringBuilder.append(String.format("%s%s", Constants.DELIMITER, tempoStr));
        stringBuilder.append(String.format("%s%s", Constants.DELIMITER, timeSignatureStr));
        stringBuilder.append(String.format("%s%s", Constants.DELIMITER, danceabilityStr));
        stringBuilder.append(String.format("%s%s", Constants.DELIMITER, durationStr));
        stringBuilder.append(String.format("%s%s", Constants.DELIMITER, modeStr));
        stringBuilder.append(String.format("%s%s", Constants.DELIMITER, energyStr));
        stringBuilder.append(String.format("%s%s", Constants.DELIMITER, keyStr));
        stringBuilder.append(String.format("%s%s", Constants.DELIMITER, loudnessStr));
        stringBuilder.append(String.format("%s%s", Constants.DELIMITER, endOfFadeInStr));
        stringBuilder.append(String.format("%s%s", Constants.DELIMITER, startOfFadeOutStr));
        stringBuilder.append(String.format("%s%s", Constants.DELIMITER, hotttnesssStr));
        context.write(new Text(songId), new Text(stringBuilder.toString()));
    }
}
