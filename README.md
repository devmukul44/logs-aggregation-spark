# Logs Aggregation Batch Job in Spark

`Spark Version 2.3.0 and Scala Version 2.11 is used for the development of this project.`

### This Spark application is written in Scala to `Generate` and `Analyse` Logs.

## This Application has 2 parts:
* Logs Generator: `src/main/scala/com/whiletruecurious/analysis/LogAnalysis.scala`
* Logs Aggregator: `src/main/scala/com/whiletruecurious/generate/LogsGenerator.scala`

## Resources can be found here:
* Generated Logs: `src/main/resources/logs/`
* Aggregated Logs: `src/main/resources/output/`

## output Schema

Columns:
1. user_id  ->  Unique Id for each user.
2. timestamp  ->  Timestamp in epoch (milliseconds)
3. session_id  ->  Unique session Id for each session. Whenever there is a time interval of greater than 4 hours; the new session is started.
4. session_count  ->  Contains the count of unique session ids in the sliding window (20 days)
5. engagement_tag  ->  {upu, pu, eu, au} depending on the `session_count`.

 