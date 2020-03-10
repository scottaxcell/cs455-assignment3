package cs455.hadoop.monolithic;

import cs455.hadoop.AnalysisConstants;
import cs455.hadoop.Constants;
import cs455.hadoop.MetadataConstants;
import cs455.hadoop.Utils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

class MonolithicReducer extends Reducer<Text, Text, Text, Text> {
    // Q1
    private Map<String, Integer> artistToNumSongs;

    // Q2
    private Map<String, TotalAndCount> artistNameToLoudness;

    class TotalAndCount {
        double total = 0;
        int count = 0;
    }

    // Q3
    private Popularity popularity;

    class Popularity {
        double score;
        String title;
    }

    // Q4
    private Map<String, Double> artistToFading;

    // Q5
    private Map<String, Double> titleToDuration;

    // Q6
    private Map<String, Double> titleToEnergetic;

    // Q8
    private Map<String, Uniqueness> artistToUniqueness;

    class Uniqueness {
        Set<Integer> timeSignatures = new HashSet<>();
        Set<Integer> keys = new HashSet<>();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        artistToNumSongs = new HashMap<>();
        artistNameToLoudness = new HashMap<>();
        popularity = new Popularity();
        artistToFading = new HashMap<>();
        titleToDuration = new HashMap<>();
        titleToEnergetic = new HashMap<>();
        artistToUniqueness = new HashMap<>();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) {
        String artistName = null;
        Double loudness = null;
        String title = null;
        Double popularity = null;
        Double endOfFadeIn = null;
        Double startOfFadeOut = null;
        Double duration = null;
        Double danceability = null;
        Double energy = null;
        Integer timeSignature = null;
        Integer songKey = null;

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

                String titleStr = songData[1];
                if (Utils.isValidString(titleStr))
                    title = titleStr;

                String artistNameStr = songData[2];
                if (Utils.isValidString(artistNameStr))
                    artistName = artistNameStr;
            }
            else if (AnalysisConstants.MAPPER_TYPE.equals(mapperType)) {
                if (songData.length != 10)
                    continue;

                String loudnessStr = songData[1];
                if (Utils.isValidString(loudnessStr))
                    loudness = Double.valueOf(loudnessStr);

                String popularityStr = songData[2];
                if (Utils.isValidString(popularityStr))
                    popularity = Double.valueOf(popularityStr);

                String endOfFadeInStr = songData[3];
                if (Utils.isValidString(endOfFadeInStr))
                    endOfFadeIn = Double.valueOf(endOfFadeInStr);

                String startOfFadeOutStr = songData[4];
                if (Utils.isValidString(startOfFadeOutStr))
                    startOfFadeOut = Double.valueOf(startOfFadeOutStr);

                String durationStr = songData[5];
                if (Utils.isValidString(durationStr))
                    duration = Double.valueOf(durationStr);

                String danceabilityStr = songData[6];
                if (Utils.isValidString(danceabilityStr))
                    danceability = Double.valueOf(danceabilityStr);

                String energyStr = songData[7];
                if (Utils.isValidString(energyStr))
                    energy = Double.valueOf(energyStr);

                String timeSignatureStr = songData[8];
                if (Utils.isValidString(timeSignatureStr))
                    timeSignature = Integer.valueOf(timeSignatureStr);

                String keyStr = songData[9];
                if (Utils.isValidString(keyStr))
                    songKey = Integer.valueOf(keyStr);
            }
        }

        if (Utils.isValidString(artistName)) {
            handleQ1(artistName);

            if (loudness != null && loudness > 0)
                handleQ2(artistName, loudness);

            if (endOfFadeIn != null || startOfFadeOut != null)
                handleQ4(artistName, endOfFadeIn, startOfFadeOut);

            if (timeSignature != null && timeSignature > 0 && songKey != null && songKey > 0)
                handleQ8(artistName, timeSignature, songKey);
        }

        if (Utils.isValidString(title)) {
            if (popularity != null)
                handleQ3(title, popularity);

            if (duration != null && duration > 0)
                handleQ5(title, duration);

            if (danceability != null && energy != null && danceability > 0 && energy > 0)
                handleQ6(title, danceability, energy);
        }
    }

    private void handleQ8(String artistName, Integer timeSignature, Integer key) {
        Uniqueness uniqueness = artistToUniqueness.computeIfAbsent(artistName, v -> new Uniqueness());
        uniqueness.timeSignatures.add(timeSignature);
        uniqueness.keys.add(key);
    }

    private void handleQ6(String title, Double danceability, Double energy) {
        titleToEnergetic.put(title, danceability + energy);
    }

    private void handleQ5(String title, Double duration) {
        titleToDuration.put(title, duration);
    }

    private void handleQ4(String artistName, Double endOfFadeIn, Double startOfFadeOut) {
        Double totalFade = artistToFading.get(artistName);
        if (totalFade != null) {
            double addMe = 0;
            if (endOfFadeIn != null)
                addMe += endOfFadeIn;
            if (startOfFadeOut != null)
                addMe += startOfFadeOut;
            artistToFading.put(artistName, totalFade + addMe);
        }
        else
            artistToFading.put(artistName, endOfFadeIn + startOfFadeOut);
    }

    private void handleQ3(String title, Double popularity) {
        if (popularity > this.popularity.score) {
            this.popularity.title = title;
            this.popularity.score = popularity;
        }
    }

    private void handleQ2(String artistName, Double loudness) {
        TotalAndCount tac = artistNameToLoudness.computeIfAbsent(artistName, v -> new TotalAndCount());
        tac.count++;
        tac.total += loudness;
    }

    private void handleQ1(String artistName) {
        int count = artistToNumSongs.getOrDefault(artistName, 0);
        artistToNumSongs.put(artistName, count + 1);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        printQ1(context);
        printQ2(context);
        printQ3(context);
        printQ4(context);
        printQ5(context);
        printQ6(context);
        printQ8(context);
        super.cleanup(context);
    }

    private void printQ8(Context context) throws IOException, InterruptedException {
        context.write(new Text("QUESTION 8"), new Text());
        context.write(new Text("=========="), new Text());

        Map<String, Integer> artistToSummedUniqueness = artistToUniqueness.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                e -> e.getValue().timeSignatures.size() + e.getValue().keys.size()));

        String mostGenericArtist = artistToSummedUniqueness.entrySet().stream()
            .min(Comparator.comparingInt(Map.Entry::getValue))
            .map(Map.Entry::getKey)
            .get();
        context.write(new Text("Most generic artist:"), new Text(mostGenericArtist));

        String mostUniqueArtist = artistToSummedUniqueness.entrySet().stream()
            .max(Comparator.comparingInt(Map.Entry::getValue))
            .map(Map.Entry::getKey)
            .get();
        context.write(new Text("Most unique artist:"), new Text(mostUniqueArtist));

        context.write(new Text(), new Text());
    }

    private void printQ6(Context context) throws IOException, InterruptedException {
        context.write(new Text("QUESTION 6"), new Text());
        context.write(new Text("=========="), new Text());

        Map<String, Double> sorted = titleToEnergetic.entrySet().stream()
            .sorted(Map.Entry.comparingByValue())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (o, n) -> o, LinkedHashMap::new));
        List<String> topTen = sorted.values().stream()
            .limit(10)
            .map(String::valueOf)
            .collect(Collectors.toList());
        context.write(new Text("10 most energetic and danceable songs:"), new Text(String.join(", ", topTen)));

        context.write(new Text(), new Text());
    }

    private void printQ5(Context context) throws IOException, InterruptedException {
        context.write(new Text("QUESTION 5"), new Text());
        context.write(new Text("=========="), new Text());

        Double longestDuration = Collections.max(titleToDuration.values());
        Set<String> longestSongs = titleToDuration.keySet().stream()
            .filter(k -> longestDuration.equals(titleToDuration.get(k)))
            .collect(Collectors.toSet());
        context.write(new Text("Longest song(s):"), new Text(String.join(", ", longestSongs)));

        Double shortestDuration = Collections.min(titleToDuration.values());
        Set<String> shortestSongs = titleToDuration.keySet().stream()
            .filter(k -> shortestDuration.equals(titleToDuration.get(k)))
            .collect(Collectors.toSet());
        context.write(new Text("Shortest song(s):"), new Text(String.join(", ", shortestSongs)));

        Map<String, Double> sorted = titleToDuration.entrySet().stream()
            .sorted(Map.Entry.comparingByValue())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (o, n) -> o, LinkedHashMap::new));
        Double median = Utils.calculateMedian(sorted.values().toArray(new Double[0]));
        Set<String> medianSongs = titleToDuration.keySet().stream()
            .filter(k -> median.equals(titleToDuration.get(k)))
            .collect(Collectors.toSet());
        context.write(new Text("Median song(s):"), new Text(String.join(", ", medianSongs)));

        context.write(new Text(), new Text());
    }

    private void printQ4(Context context) throws IOException, InterruptedException {
        context.write(new Text("QUESTION 4"), new Text());
        context.write(new Text("=========="), new Text());

        String artistName = artistToFading.entrySet().stream()
            .max(Comparator.comparingDouble(Map.Entry::getValue))
            .map(Map.Entry::getKey)
            .get();
        context.write(new Text("Artist with highest total time fading:"), new Text(artistName));

        context.write(new Text(), new Text());
    }

    private void printQ3(Context context) throws IOException, InterruptedException {
        context.write(new Text("QUESTION 3"), new Text());
        context.write(new Text("=========="), new Text());

        context.write(new Text("Song with highest popularity score (" + popularity.score + "):"), new Text(popularity.title));

        context.write(new Text(), new Text());
    }

    private void printQ2(Context context) throws IOException, InterruptedException {
        context.write(new Text("QUESTION 2"), new Text());
        context.write(new Text("=========="), new Text());

        Map<String, Double> artistToAvg = new HashMap<>();
        for (Map.Entry<String, TotalAndCount> entry : artistNameToLoudness.entrySet()) {
            TotalAndCount tac = entry.getValue();
            artistToAvg.put(entry.getKey(), tac.total / tac.count);
        }
        String artistName = artistToAvg.entrySet().stream()
            .max(Comparator.comparingDouble(Map.Entry::getValue))
            .map(Map.Entry::getKey)
            .get();
        context.write(new Text("Artist with loudest songs:"), new Text(artistName));

        context.write(new Text(), new Text());
    }

    private void printQ1(Context context) throws IOException, InterruptedException {
        context.write(new Text("QUESTION 1"), new Text());
        context.write(new Text("=========="), new Text());

        String artistName = artistToNumSongs.entrySet().stream()
            .max(Comparator.comparingDouble(Map.Entry::getValue))
            .map(Map.Entry::getKey)
            .get();
        context.write(new Text("Artist with most songs:"), new Text(artistName));

        context.write(new Text(), new Text());
    }
}
