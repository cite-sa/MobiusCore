﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Streaming;

namespace Microsoft.Spark.CSharp.Examples
{
    /// <summary>
    /// Sample Mobius application that processes events from Kafka in the format [timestamp],[loglevel],[logmessage]
    /// MessagePublisher class may be used to publish sample messages to Kafka to consume in this app
    /// </summary>
    class SparkClrKafkaExample
    {
        static void Main(string[] args)
        {
            var sparkContext = new SparkContext(new SparkConf().SetAppName("SparkCLRKafka Example"));
            const string topicName = "<topicName>";
            var topicList = new List<string> {topicName};
            var kafkaParams = new Dictionary<string, string> //refer to http://kafka.apache.org/documentation.html#configuration
            {
                {"metadata.broker.list", "<kafka brokers list>"},
                {"auto.offset.reset", "smallest"}
            };
            var perTopicPartitionKafkaOffsets = new Dictionary<string, long>();
            const int windowDurationInSecs = 5;
            const int slideDurationInSecs = 5;
            const string checkpointPath = "<hdfs path to spark checkpoint directory>";
            const string appOutputPath = "<hdfs path to app output directory>";


            const long slideDurationInMillis = 5000;
            StreamingContext sparkStreamingContext = StreamingContext.GetOrCreate(checkpointPath,
                () =>
                {
                    var ssc = new StreamingContext(sparkContext, slideDurationInMillis);
                    ssc.Checkpoint(checkpointPath);

                    var stream = KafkaUtils.CreateDirectStream(ssc, topicList, kafkaParams.Select(v => new Tuple<string, string>(v.Key, v.Value)), perTopicPartitionKafkaOffsets.Select(v => new Tuple<string, long>(v.Key, v.Value)));
                    var countByLogLevelAndTime = stream
                                                    .Map(tuple => Encoding.UTF8.GetString(tuple.Item2))
                                                    .Filter(line => line.Contains(","))
                                                    .Map(line => line.Split(new char[] { ',' }))
                                                    .Map(columns => new Tuple<string, int>(string.Format("{0},{1}", columns[0], columns[1]), 1))
                                                    .ReduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, windowDurationInSecs, slideDurationInSecs, 3)
                                                    .Map(logLevelCountPair => string.Format("{0},{1}", logLevelCountPair.Item1, logLevelCountPair.Item2));

                    countByLogLevelAndTime.ForeachRDD(countByLogLevel => new SparkClrKafkaExample().ForEachHelper(countByLogLevel, appOutputPath));

                    return ssc;
                });

            sparkStreamingContext.Start();
            sparkStreamingContext.AwaitTermination();
        }

		public void ForEachHelper(RDD<string> countByLogLevel, String appOutputPath)
		{
			countByLogLevel.SaveAsTextFile(string.Format("{0}/{1}", appOutputPath, Guid.NewGuid()));
			foreach (var logCount in countByLogLevel.Collect())
			{
				Console.WriteLine(logCount);
			}
		}
    }
}
