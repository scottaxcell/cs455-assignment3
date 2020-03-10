package cs455.hadoop.ten;

import cs455.hadoop.AnalysisConstants;
import cs455.hadoop.Constants;
import cs455.hadoop.MetadataConstants;
import cs455.hadoop.Utils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


class TenReducer extends Reducer<Text, Text, Text, Text> {
    Map<Integer, YearData> yearToData;

    class TotalAndCount {
        double total = 0;
        int count = 0;
    }

    class YearData {
        TotalAndCount loudness = new TotalAndCount();
        TotalAndCount tempo = new TotalAndCount();
        TotalAndCount duration = new TotalAndCount();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        yearToData = new HashMap<>();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Integer year = null;
        Double loudness = null;
        Double tempo = null;
        Double duration = null;

        for (Text value : values) {
            String valueStr = value.toString();
            if (!Utils.isValidString(valueStr))
                continue;

            String[] songData = valueStr.split(Constants.DELIMITER);
            if (songData.length == 0)
                continue;
            String mapperType = songData[0];
            if (MetadataConstants.MAPPER_TYPE.equals(mapperType)) {
                if (songData.length != 2)
                    continue;

                String yearStr = songData[1];
                if (Utils.isValidString(yearStr))
                    year = Integer.valueOf(yearStr);

            } else if (AnalysisConstants.MAPPER_TYPE.equals(mapperType)) {
                if (songData.length != 4)
                    continue;

                String tempoStr = songData[1];
                if (Utils.isValidString(tempoStr))
                    tempo = Double.valueOf(tempoStr);

                String loudnessStr = songData[2];
                if (Utils.isValidString(loudnessStr))
                    loudness = Double.valueOf(loudnessStr);

                String durationStr = songData[3];
                if (Utils.isValidString(durationStr))
                    duration = Double.valueOf(durationStr);
            }
        }

        if (year != null && year > 0 && tempo != null && loudness != null && duration != null) {
            cacheData(year, tempo, loudness, duration);
        }
    }

    private void cacheData(Integer year, Double tempo, Double loudness, Double duration) {
        YearData yearData = yearToData.computeIfAbsent(year, v -> new YearData());
        yearData.loudness.total += loudness;
        yearData.loudness.count++;
        yearData.tempo.total += tempo;
        yearData.tempo.count++;
        yearData.duration.total += duration;
        yearData.duration.count++;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        yearToData.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> {
                    YearData yearData = e.getValue();
                    TotalAndCount tempoTaC = yearData.tempo;
                    double avgTempo = tempoTaC.total / tempoTaC.count;
                    TotalAndCount loudnessTaC = yearData.loudness;
                    double avgLoudness = loudnessTaC.total / loudnessTaC.count;
                    TotalAndCount durationTaC = yearData.duration;
                    double avgDuration = durationTaC.total / durationTaC.count;
                    try {
                        context.write(new Text(String.format("%d,%.4f,%.4f,%.4f", e.getKey(), avgTempo, avgLoudness, avgDuration)), new Text());
                    } catch (IOException | InterruptedException ex) {
                        ex.printStackTrace();
                    }
                });

        super.cleanup(context);
    }
}
