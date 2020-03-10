package cs455.hadoop;

public class AnalysisConstants {
    public static final String MAPPER_TYPE = "ANAL";
    public static final int NUM_FIELDS = 33;
    public static final int DUMMY_ZERO_INDEX = 0;
    public static final int SONG_ID_INDEX = 1;
    public static final int SONG_HOTTTNESSS_INDEX = 2;
    public static final int DANCEABILITY_INDEX = 4;
    public static final int DURATION_INDEX = 5;
    public static final int END_OF_FADE_IN_INDEX = 6;
    public static final int ENERGY_INDEX = 7;
    public static final int KEY_INDEX = 8;
    public static final int LOUDNESS_INDEX = 10;
    public static final int MODE_INDEX = 11;
    public static final int START_OF_FADE_OUT_INDEX = 13;
    public static final int TEMPO_INDEX = 14;
    public static final int TIME_SIGNATURE_INDEX = 15;
    public static final int SEGMENTS_START_INDEX = 18;
    public static final int SEGMENTS_PITCHES_INDEX = 20;
    public static final int SEGMENTS_TIMBRE_INDEX = 21;
    public static final int SEGMENTS_LOUDNESS_MAX_INDEX = 22;
    public static final int SEGMENTS_LOUDNESS_MAX_TIME_INDEX = 23;
    public static final int SEGMENTS_LOUDNESS_START_INDEX = 24;

    private AnalysisConstants() {
    }
}
