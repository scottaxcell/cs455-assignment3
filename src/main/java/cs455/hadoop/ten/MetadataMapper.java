package cs455.hadoop.ten;

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

        String year = songData[MetadataConstants.YEAR_INDEX];
        if (!Utils.isValidString(year))
            return;

        String songId = songData[MetadataConstants.SONG_ID_INDEX];
        if (!Utils.isValidString(songId))
            return;

        context.write(new Text(songId), new Text(String.format("%s%s%s", MetadataConstants.MAPPER_TYPE, Constants.DELIMITER, year)));
    }
}
