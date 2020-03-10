Scott Axcell (827239918)
CS455 Homework 3: Programming Component
03-14-2019

Description:
    Using MapReduce To Analyze The Million Song Dataset

Build:
  gradle build

Run:
    runme.sh

Classes:
    monolithic.AnalysisMapper : maps various analysis data fields to the song id to answer Q1-Q6 and Q8
    monolithic.Job : configures and runs a MapReduce job for Q1-Q6 and Q8
    monolithic.MetadataMapper : maps various metadata data fields to the song id to answer Q1-Q6 and Q8
    monolithic.MonolithicReducer : merges analysis and metadata data on song ids to answer Q1-Q6 and Q8

    nine.AnalysisMapper : maps various analysis data fields to the song id to answer Q9
    nine.ImaginarySongReducer : merges analysis and metadata data on song ids to answer Q9
    nine.Job : configures and runs a MapReduce job for Q9
    nine.MetadataMapper : maps various metadata data fields to the song id to answer Q9

    seven.AnalysisMapper : maps various analysis data fields to the song id to answer Q7
    seven.AverageSongMapper : collects analysis data on song ids to answer Q7
    seven.Job : configures and runs a MapReduce job for Q7

    ten.AnalysisMapper : maps various analysis data fields to the song id to answer Q10
    ten.Job : configures and runs a MapReduce job for Q10
    ten.MetadataMapper : maps various metadata data fields to the song id to answer Q10
    ten.TenReducer : merges analysis and metadata data on song ids to answer Q10

    AnalysisConstants : indexes into comma separated analysis file lines
    Constants : application constants
    MetadataConstants : indexes into comma separated metadata file lines
    Utils : miscellaneous utility methods

Notes for TA:
The MapReduce jobs should function as spec'd in the assignment (fingers crossed).
