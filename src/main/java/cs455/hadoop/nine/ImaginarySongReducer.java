package cs455.hadoop.nine;

import cs455.hadoop.AnalysisConstants;
import cs455.hadoop.Constants;
import cs455.hadoop.MetadataConstants;
import cs455.hadoop.Utils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class ImaginarySongReducer extends Reducer<Text, Text, Text, Text> {
    private static final Double MAX_POPULARITY = 1.0;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Double tempo = null;
        Integer timeSignature = null;
        Double danceability = null; // ignore 0
        Double duration = null;
        Integer mode = null;
        Double energy = null; // ignore 0
        Integer songKey = null;
        Double loudness = null;
        Double endOfFadeIn = null;
        Double startOfFadeOut = null;
        Double popularity = null; // ignore 0
        String artistName = null;
        String artistTerms = null;


        for (Text value : values) {
            String valueStr = value.toString();
            if (!Utils.isValidString(valueStr))
                continue;

            String[] songData = valueStr.split(Constants.DELIMITER);
            if (songData.length == 0)
                continue;
            String mapperType = songData[0];
            if (MetadataConstants.MAPPER_TYPE.equals(mapperType)) {
                if (songData.length != 3)
                    continue;

                String artistNameStr = songData[1];
                if (Utils.isValidString(artistNameStr))
                    artistName = artistNameStr;

                String artistTermsStr = songData[2];
                if (Utils.isValidString(artistTermsStr))
                    artistTerms = artistTermsStr;
            } else if (AnalysisConstants.MAPPER_TYPE.equals(mapperType)) {
                if (songData.length != 12)
                    continue;

                String popularityStr = songData[11];
                if (Utils.isValidString(popularityStr))
                    popularity = Double.valueOf(popularityStr);
                if (!Utils.isGreaterThanZero(popularity))
                    continue;

                String tempoStr = songData[1];
                if (Utils.isValidString(tempoStr))
                    tempo = Double.valueOf(tempoStr);

                String modeStr = songData[5];
                if (Utils.isValidString(modeStr))
                    mode = Integer.valueOf(modeStr);

                String loudnessStr = songData[8];
                if (Utils.isValidString(loudnessStr))
                    loudness = Double.valueOf(loudnessStr);

                String endOfFadeInStr = songData[9];
                if (Utils.isValidString(endOfFadeInStr))
                    endOfFadeIn = Double.valueOf(endOfFadeInStr);

                String startOfFadeOutStr = songData[10];
                if (Utils.isValidString(startOfFadeOutStr))
                    startOfFadeOut = Double.valueOf(startOfFadeOutStr);

                String durationStr = songData[4];
                if (Utils.isValidString(durationStr))
                    duration = Double.valueOf(durationStr);

                String danceabilityStr = songData[3];
                if (Utils.isValidString(danceabilityStr))
                    danceability = Double.valueOf(danceabilityStr);

                String energyStr = songData[6];
                if (Utils.isValidString(energyStr))
                    energy = Double.valueOf(energyStr);

                String timeSignatureStr = songData[2];
                if (Utils.isValidString(timeSignatureStr))
                    timeSignature = Integer.valueOf(timeSignatureStr);

                String keyStr = songData[7];
                if (Utils.isValidString(keyStr))
                    songKey = Integer.valueOf(keyStr);

            }
        }

        if (MAX_POPULARITY.equals(popularity)) {
            if (artistName == null
                    || artistTerms == null
                    || tempo == null
                    || mode == null
                    || loudness == null
                    || endOfFadeIn == null
                    || startOfFadeOut == null
                    || duration == null
                    || danceability == null
                    || energy == null
                    || timeSignature == null
                    || songKey == null)
                return;

            context.write(new Text("=============================="), new Text());
            context.write(new Text("Artist Name:"), new Text(artistName));
            context.write(new Text("Artist Terms:"), new Text(artistTerms));
            context.write(new Text("Tempo:"), new Text(String.valueOf(tempo)));
            context.write(new Text("Mode:"), new Text(String.valueOf(mode)));
            context.write(new Text("Loudness:"), new Text(String.valueOf(loudness)));
            context.write(new Text("End of Fade In:"), new Text(String.valueOf(endOfFadeIn)));
            context.write(new Text("Start of Fade Out:"), new Text(String.valueOf(startOfFadeOut)));
            context.write(new Text("Duration:"), new Text(String.valueOf(duration)));
            context.write(new Text("Danceability:"), new Text(String.valueOf(danceability)));
            context.write(new Text("Energy:"), new Text(String.valueOf(energy)));
            context.write(new Text("Time Signature:"), new Text(String.valueOf(timeSignature)));
            context.write(new Text("Key:"), new Text(String.valueOf(songKey)));
            context.write(new Text(), new Text());
        }
    }
}
