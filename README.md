
# MobiusCore: C# API for Spark

[MobiusCore](https://github.com/cite-sa/MobiusCore) is a .NET Core port of of [Mobius](https://github.com/microsoft/Mobius), the open source implementation of C# Apache Spark bindings. 

# Implementation

[MobiusCore](https://github.com/cite-sa/MobiusCore) replaces all the Delegate Function parameters in  [Mobius](https://github.com/microsoft/Mobius) Functions
with Linq Expressions in order to tackle the serialization problem that was introduced in .NET Core with Delegate Serialization. You can find more information for this approach [here](./implementation.md)

# Examples

The word count sample in Apache Spark can be implemented in C# as follows :

```c#
var lines = sparkContext.TextFile(@"hdfs://path/to/input.txt");  
var words = lines.FlatMap(s => s.Split(' '));
var wordCounts = words.Map(w => new Tuple<string, int>(w.Trim(), 1))  
                      .ReduceByKey((x, y) => x + y);  
var wordCountCollection = wordCounts.Collect();  
wordCounts.SaveAsTextFile(@"hdfs://path/to/wordcount.txt");  
```

A simple DataFrame application using TempTable may look like the following:

```c#
var reqDataFrame = sqlContext.TextFile(@"hdfs://path/to/requests.csv");
var metricDataFrame = sqlContext.TextFile(@"hdfs://path/to/metrics.csv");
reqDataFrame.RegisterTempTable("requests");
metricDataFrame.RegisterTempTable("metrics");
// C0 - guid in requests DataFrame, C3 - guid in metrics DataFrame  
var joinDataFrame = GetSqlContext().Sql(  
    "SELECT joinedtable.datacenter" +
         ", MAX(joinedtable.latency) maxlatency" +
         ", AVG(joinedtable.latency) avglatency " +
    "FROM (" +
       "SELECT a.C1 as datacenter, b.C6 as latency " +  
       "FROM requests a JOIN metrics b ON a.C0  = b.C3) joinedtable " +   
    "GROUP BY datacenter");
joinDataFrame.ShowSchema();
joinDataFrame.Show();
```

A simple DataFrame application using DataFrame DSL may look like the following:

```  c#
// C0 - guid, C1 - datacenter
var reqDataFrame = sqlContext.TextFile(@"hdfs://path/to/requests.csv")  
                             .Select("C0", "C1");    
// C3 - guid, C6 - latency   
var metricDataFrame = sqlContext.TextFile(@"hdfs://path/to/metrics.csv", ",", false, true)
                                .Select("C3", "C6"); //override delimiter, hasHeader & inferSchema
var joinDataFrame = reqDataFrame.Join(metricDataFrame, reqDataFrame["C0"] == metricDataFrame["C3"])
                                .GroupBy("C1");
var maxLatencyByDcDataFrame = joinDataFrame.Agg(new Dictionary<string, string> { { "C6", "max" } });
maxLatencyByDcDataFrame.ShowSchema();
maxLatencyByDcDataFrame.Show();
```

A simple Spark Streaming application that processes messages from Kafka using C# may be implemented using the following code:

```  c#
StreamingContext sparkStreamingContext = StreamingContext.GetOrCreate(checkpointPath, () =>
    {
      var ssc = new StreamingContext(sparkContext, slideDurationInMillis);
      ssc.Checkpoint(checkpointPath);
      var stream = KafkaUtils.CreateDirectStream(ssc, topicList, kafkaParams, perTopicPartitionKafkaOffsets);
      //message format: [timestamp],[loglevel],[logmessage]
      var countByLogLevelAndTime = stream
                                    .Map(kvp => Encoding.UTF8.GetString(kvp.Value))
                                    .Filter(line => line.Contains(","))
                                    .Map(line => line.Split(','))
                                    .Map(columns => new Tuple<string, int>(
                                                          string.Format("{0},{1}", columns[0], columns[1]), 1))
                                    .ReduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y,
                                                          windowDurationInSecs, slideDurationInSecs, 3)
                                    .Map(logLevelCountPair => string.Format("{0},{1}",
                                                          logLevelCountPair.Key, logLevelCountPair.Value));
      countByLogLevelAndTime.ForeachRDD(countByLogLevel => new StreamHelper().CollectAndPrint(countByLogLevel));
      return ssc;
    });
sparkStreamingContext.Start();
sparkStreamingContext.AwaitTermination();

```
For more code samples, refer to [MobiusCore\examples](./examples) directory or [MobiusCore\csharp\Samples](./csharp/Samples) directory.