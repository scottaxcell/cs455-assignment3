package cs455.hadoop.monolithic;

import cs455.hadoop.Constants;
import cs455.hadoop.MetadataConstants;
import cs455.hadoop.Utils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class MetadataMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] songData = value.toString().split(",");
        if (songData.length != MetadataConstants.NUM_FIELDS)
            return;

        if (songData[MetadataConstants.DUMMY_ZERO_INDEX].isEmpty())
            return;

        String artistName = songData[MetadataConstants.ARTIST_NAME_INDEX];
        if (!Utils.isValidString(artistName))
            return;

        String songId = songData[MetadataConstants.SONG_ID_INDEX];
        if (!Utils.isValidString(songId))
            return;

        String title = songData[MetadataConstants.TITLE_INDEX];
        if (!Utils.isValidString(title))
            return;

        StringBuilder stringBuilder = new StringBuilder(MetadataConstants.MAPPER_TYPE);
        stringBuilder.append(String.format("%s%s", Constants.DELIMITER, title));
        stringBuilder.append(String.format("%s%s", Constants.DELIMITER, artistName));

        context.write(new Text(songId), new Text(stringBuilder.toString()));
    }
}
