﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Streaming;
using Moq;
using NUnit.Framework;

namespace AdapterTest
{
    [TestFixture]
    public class DStreamTest
    {
        [Test]
        public void TestDStreamMapReduce()
        {
            var ssc = new StreamingContext(new SparkContext("", ""), 1000L);
            Assert.IsNotNull((ssc.streamingContextProxy as MockStreamingContextProxy));

            var lines = ssc.TextFileStream(Path.GetTempPath());
            Assert.IsNotNull(lines.DStreamProxy);

            var words = lines.FlatMap(l => l.Split(new char[] { ' ' })).Filter(w => w != "The").Repartition(1);

            words.Slice(DateTime.MinValue, DateTime.MaxValue);
            words.Cache();
            words.Checkpoint(1000);
            words.Window(1, 1);

            words.Count().ForeachRDD((time, rdd) => new TestDStreamMapReduceHelper().Count(time, rdd));

            words.CountByValue().ForeachRDD((time, rdd) => new TestDStreamMapReduceHelper().CountByValue(time, rdd));

            words.CountByValueAndWindow(1, 1).ForeachRDD((time, rdd) => new TestDStreamMapReduceHelper().CountByValueAndWindow(time, rdd));

            words.CountByWindow(1).ForeachRDD((time, rdd) => new TestDStreamMapReduceHelper().CountByWindow(time, rdd));

            words.Union(words).ForeachRDD((time, rdd) => new TestDStreamMapReduceHelper().Union(time, rdd));

            words.Glom().ForeachRDD((time, rdd) => new TestDStreamMapReduceHelper().Glom(time, rdd));
        }

        public class TestDStreamMapReduceHelper
        {
            public void Count(double time, RDD<dynamic> rdd)
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 1);
                Assert.AreEqual((int)taken[0], 178);
            }

            public void CountByValue(double time, RDD<dynamic> rdd)
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 8);

                foreach (object record in taken)
                {
                    Tuple<string, long> countByWord = (Tuple<string, long>)record;
                    Assert.AreEqual(countByWord.Item2, countByWord.Item1 == "The" || countByWord.Item1 == "dog" || countByWord.Item1 == "lazy" ? 23 : 22);
                }
            }

            public void CountByValueAndWindow(double time, RDD<dynamic> rdd)
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken[0], 8);
            }

            public void CountByWindow(double time, RDD<dynamic> rdd)
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 1);
                Assert.AreEqual((int)taken[0], 356);
            }

            public void Union(double time, RDD<dynamic> rdd)
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 356);
            }

            public void Glom(double time, RDD<dynamic> rdd)
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 1);
                Assert.AreEqual((taken[0] as string[]).Length, 178);
            }
        }

        [Test]
        public void TestDStreamTransform()
        {
            var ssc = new StreamingContext(new SparkContext("", ""), 1000L);
            Assert.IsNotNull((ssc.streamingContextProxy as MockStreamingContextProxy));

            var lines = ssc.TextFileStream(Path.GetTempPath());
            Assert.IsNotNull(lines.DStreamProxy);

            var words = lines.FlatMap(l => l.Split(new char[] { ' ' }));

            var pairs = words.Map(w => new Tuple<string, int>(w, 1));

            var wordCounts = pairs.PartitionBy().ReduceByKey((x, y) => x + y);

            wordCounts.ForeachRDD((time, rdd) => new TestDStreamTransformHelper().ForeachRDD(time, rdd));

            var wordLists = pairs.GroupByKey();

            wordLists.ForeachRDD((time, rdd) => new TestDStreamTransformHelper().ForeachRDD1(time, rdd));

            var wordCountsByWindow = pairs.ReduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, 1);

            wordCountsByWindow.ForeachRDD((time, rdd) => new TestDStreamTransformHelper().ForeachRDD2(time, rdd));
        }

        public class TestDStreamTransformHelper
        {
            public void ForeachRDD(double time, RDD<dynamic> rdd)
            {
                var taken = rdd.Collect();

                Assert.AreEqual(taken.Length, 9);

                foreach (object record in taken)
                {
                    Tuple<string, int> countByWord = (Tuple<string, int>)record;
                    Assert.AreEqual(countByWord.Item2, countByWord.Item1 == "The" || countByWord.Item1 == "dog" || countByWord.Item1 == "lazy" ? 23 : 22);
                }
            }

            public void ForeachRDD1(double time, RDD<dynamic> rdd)
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 9);

                foreach (object record in taken)
                {
                    Tuple<string, List<int>> countByWord = (Tuple<string, List<int>>)record;
                    Assert.AreEqual(countByWord.Item2.Count, countByWord.Item1 == "The" || countByWord.Item1 == "dog" || countByWord.Item1 == "lazy" ? 23 : 22);
                }
            }

            public void ForeachRDD2(double time, RDD<dynamic> rdd)
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 9);

                foreach (object record in taken)
                {
                    Tuple<string, int> countByWord = (Tuple<string, int>)record;
                    Assert.AreEqual(countByWord.Item2, countByWord.Item1 == "The" || countByWord.Item1 == "dog" || countByWord.Item1 == "lazy" ? 46 : 44);
                }
            }
        }

        [Test]
        public void TestDStreamJoin()
        {
            var ssc = new StreamingContext(new SparkContext("", ""), 1000L);
            Assert.IsNotNull((ssc.streamingContextProxy as MockStreamingContextProxy));

            var lines = ssc.TextFileStream(Path.GetTempPath());
            Assert.IsNotNull(lines.DStreamProxy);

            var words = lines.FlatMap(l => l.Split(new char[] { ' ' }));

            var pairs = words.Map(w => new Tuple<string, int>(w, 1));

            var wordCounts = pairs.ReduceByKey((x, y) => x + y);

            var left = wordCounts.Filter(x => x.Item1 != "quick" && x.Item1 != "lazy");
            var right = wordCounts.Filter(x => x.Item1 != "brown");

            var groupWith = left.GroupWith(right);
            groupWith.ForeachRDD((time, rdd) => new TestDStreamJoinHelper().GroupByHelper(time, rdd));

            var innerJoin = left.Join(right);
            innerJoin.ForeachRDD((time, rdd) => new TestDStreamJoinHelper().InnerJoinHelper(time, rdd));

            var leftOuterJoin = left.LeftOuterJoin(right);
            leftOuterJoin.ForeachRDD((time, rdd) => new TestDStreamJoinHelper().LeftOuterJoinHelper(time, rdd));

            var rightOuterJoin = left.RightOuterJoin(right);
            rightOuterJoin.ForeachRDD(rdd => new TestDStreamJoinHelper().RightOuterJoinHelper(rdd));

            var fullOuterJoin = left.FullOuterJoin(right);
            fullOuterJoin.ForeachRDD(rdd => new TestDStreamJoinHelper().FullOuterJoinHelper(rdd));
        }

        public class TestDStreamJoinHelper
        {
            public void GroupByHelper(double time, RDD<dynamic> rdd)
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 9);

                foreach (object record in taken)
                {
                    Tuple<string, Tuple<List<int>, List<int>>> countByWord = (Tuple<string, Tuple<List<int>, List<int>>>)record;
                    if (countByWord.Item1 == "quick" || countByWord.Item1 == "lazy")
                        Assert.AreEqual(countByWord.Item2.Item1.Count, 0);
                    else if (countByWord.Item1 == "brown")
                        Assert.AreEqual(countByWord.Item2.Item2.Count, 0);
                    else
                    {
                        Assert.AreEqual(countByWord.Item2.Item1[0], countByWord.Item1 == "The" || countByWord.Item1 == "dog" ? 23 : 22);
                        Assert.AreEqual(countByWord.Item2.Item2[0], countByWord.Item1 == "The" || countByWord.Item1 == "dog" || countByWord.Item1 == "lazy" ? 23 : 22);
                    }
                }
            }

            public void InnerJoinHelper(double time, RDD<dynamic> rdd)
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 6);

                foreach (object record in taken)
                {
                    Tuple<string, Tuple<int, int>> countByWord = (Tuple<string, Tuple<int, int>>)record;
                    Assert.AreEqual(countByWord.Item2.Item1, countByWord.Item1 == "The" || countByWord.Item1 == "dog" ? 23 : 22);
                    Assert.AreEqual(countByWord.Item2.Item2, countByWord.Item1 == "The" || countByWord.Item1 == "dog" ? 23 : 22);
                }
            }

            public void LeftOuterJoinHelper(double time, RDD<dynamic> rdd)
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 7);

                foreach (object record in taken)
                {
                    Tuple<string, Tuple<int, Option<int>>> countByWord = (Tuple<string, Tuple<int, Option<int>>>)record;
                    Assert.AreEqual(countByWord.Item2.Item1, countByWord.Item1 == "The" || countByWord.Item1 == "dog" ? 23 : 22);
                    Assert.IsTrue(countByWord.Item1 == "The" || countByWord.Item1 == "dog" ?
                        countByWord.Item2.Item2.IsDefined == true && countByWord.Item2.Item2.GetValue() == 23 : (countByWord.Item1 == "brown" ?
                        countByWord.Item2.Item2.IsDefined == true == false : countByWord.Item2.Item2.IsDefined == true && countByWord.Item2.Item2.GetValue() == 22));
                }
            }

            public void RightOuterJoinHelper(RDD<Tuple<string, Tuple<Option<int>, int>>> rdd)
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 8);

                foreach (object record in taken)
                {
                    Tuple<string, Tuple<Option<int>, int>> countByWord = (Tuple<string, Tuple<Option<int>, int>>)record;
                    Assert.IsTrue(countByWord.Item1 == "The" || countByWord.Item1 == "dog" ?
                        countByWord.Item2.Item1.IsDefined == true && countByWord.Item2.Item1.GetValue() == 23 :
                        (countByWord.Item1 == "quick" || countByWord.Item1 == "lazy" ? countByWord.Item2.Item1.IsDefined == false :
                        countByWord.Item2.Item1.IsDefined == true && countByWord.Item2.Item1.GetValue() == 22));
                    Assert.AreEqual(countByWord.Item2.Item2, countByWord.Item1 == "The" || countByWord.Item1 == "dog" || countByWord.Item1 == "lazy" ? 23 : 22);
                }
            }

            public void FullOuterJoinHelper(RDD<Tuple<string, Tuple<Option<int>, Option<int>>>> rdd)
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 9);

                foreach (object record in taken)
                {
                    Tuple<string, Tuple<Option<int>, Option<int>>> countByWord = (Tuple<string, Tuple<Option<int>, Option<int>>>)record;
                    Assert.IsTrue(countByWord.Item1 == "The" || countByWord.Item1 == "dog" ?
                        countByWord.Item2.Item1.IsDefined == true && countByWord.Item2.Item1.GetValue() == 23 :
                        (countByWord.Item1 == "quick" || countByWord.Item1 == "lazy" ? countByWord.Item2.Item1.IsDefined == false :
                        countByWord.Item2.Item1.IsDefined == true && countByWord.Item2.Item1.GetValue() == 22));

                    Assert.IsTrue(countByWord.Item1 == "The" || countByWord.Item1 == "dog" || countByWord.Item1 == "lazy" ?
                        countByWord.Item2.Item2.IsDefined == true && countByWord.Item2.Item2.GetValue() == 23 :
                        (countByWord.Item1 == "brown" ? countByWord.Item2.Item2.IsDefined == false : countByWord.Item2.Item2.IsDefined == true && countByWord.Item2.Item2.GetValue() == 22));
                }
            }
        }

        [Test]
        public void TestDStreamGroupByKeyAndWindow()
        {
            var ssc = new StreamingContext(new SparkContext("", ""), 1000L);
            Assert.IsNotNull((ssc.streamingContextProxy as MockStreamingContextProxy));

            var lines = ssc.TextFileStream(Path.GetTempPath());
            Assert.IsNotNull(lines.DStreamProxy);

            var words = lines.FlatMap(l => l.Split(new char[] { ' ' }));

            var pairs = words.Map(w => new Tuple<string, int>(w, 1));

            var doubleCounts = pairs.GroupByKeyAndWindow(1000, 0).FlatMapValues(vs => vs).ReduceByKey((x, y) => x + y);
            doubleCounts.ForeachRDD((time, rdd) => new TestDStreamGroupByKeyAndWindowHelper().DoubleCountsHelper(time, rdd));
        }

        public class TestDStreamGroupByKeyAndWindowHelper
        {
            public void DoubleCountsHelper(double time, RDD<dynamic> rdd)
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 9);

                foreach (object record in taken)
                {
                    Tuple<string, int> countByWord = (Tuple<string, int>)record;
                    Assert.AreEqual(countByWord.Item1 == "The" || countByWord.Item1 == "dog" || countByWord.Item1 == "lazy" ? 2 * 23 : 2 * 22, countByWord.Item2);
                }
            }
        }

        [Test]
        public void TestDStreamUpdateStateByKey()
        {
            var ssc = new StreamingContext(new SparkContext("", ""), 1000L);
            Assert.IsNotNull((ssc.streamingContextProxy as MockStreamingContextProxy));

            var lines = ssc.TextFileStream(Path.GetTempPath());
            Assert.IsNotNull(lines.DStreamProxy);

            var words = lines.FlatMap(l => l.Split(new char[] { ' ' }));

            var pairs = words.Map(w => new Tuple<string, int>(w, 1));

            var doubleCounts = pairs.GroupByKey().FlatMapValues(vs => vs).MapValues(v => 2 * v).ReduceByKey((x, y) => x + y);
            doubleCounts.ForeachRDD((time, rdd) => new TestDStreamUpdateStateByKeyHelper().ForeachRDDHelper1(time, rdd));

            // disable pipeline to UpdateStateByKey which replys on checkpoint mock proxy doesn't support
            pairs.Cache();

            var initialStateRdd = ssc.SparkContext.Parallelize(new[] { "AAA" }).Map(w => new Tuple<string, int>("AAA", 22));
            var state = pairs.UpdateStateByKey<string, int, int>((v, s) => s + v.Count(), initialStateRdd);
            state.ForeachRDD((time, rdd) => new TestDStreamUpdateStateByKeyHelper().ForeachRDDHelper2(time, rdd));

            // test when initialStateRdd is not provided
            var state2 = pairs.UpdateStateByKey<string, int, int>((v, s) => s + v.Count());
            state2.ForeachRDD((time, rdd) => new TestDStreamUpdateStateByKeyHelper().ForeachRDDHelper3(time, rdd));
        }

        public class TestDStreamUpdateStateByKeyHelper
        {
            public void ForeachRDDHelper1(double time, RDD<dynamic> rdd)
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 9);

                foreach (object record in taken)
                {
                    Tuple<string, int> countByWord = (Tuple<string, int>)record;
                    Assert.AreEqual(countByWord.Item1 == "The" || countByWord.Item1 == "dog" || countByWord.Item1 == "lazy" ? 2 * 23 : 2 * 22, countByWord.Item2);
                }
            }

            public void ForeachRDDHelper2(double time, RDD<dynamic> rdd)
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 10);

                foreach (object record in taken)
                {
                    Tuple<string, int> countByWord = (Tuple<string, int>)record;
                    Assert.AreEqual(countByWord.Item1 == "The" || countByWord.Item1 == "dog" || countByWord.Item1 == "lazy" ? 23 : 22, countByWord.Item2);
                }
            }

            public void ForeachRDDHelper3(double time, RDD<dynamic> rdd)
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 9);

                foreach (object record in taken)
                {
                    Tuple<string, int> countByWord = (Tuple<string, int>)record;
                    Assert.AreEqual(countByWord.Item1 == "The" || countByWord.Item1 == "dog" || countByWord.Item1 == "lazy" ? 23 : 22, countByWord.Item2);
                }
            }
        }

        [Test]
        public void TestDStreamMapWithState()
        {
            var mapwithStateDStreamProxy = new Mock<IDStreamProxy>();
            var streamingContextProxy = new Mock<IStreamingContextProxy>();
            streamingContextProxy.Setup(p =>
                p.CreateCSharpStateDStream(It.IsAny<IDStreamProxy>(), It.IsAny<byte[]>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
                .Returns(mapwithStateDStreamProxy.Object);

            var sparkContextProxy = new Mock<ISparkContextProxy>();

            var sparkConfProxy = new Mock<ISparkConfProxy>();

            var sparkClrProxy = new Mock<ISparkCLRProxy>();
            sparkClrProxy.Setup(p => p.StreamingContextProxy).Returns(streamingContextProxy.Object);
            sparkClrProxy.Setup(p => p.SparkContextProxy).Returns(sparkContextProxy.Object);
            sparkClrProxy.Setup(p => p.CreateSparkContext(It.IsAny<ISparkConfProxy>())).Returns(sparkContextProxy.Object);
            sparkClrProxy.Setup(p => p.CreateSparkConf(It.IsAny<bool>())).Returns(sparkConfProxy.Object);

            // reset sparkCLRProxy for after test completes
            var originalSparkCLRProxy = SparkCLREnvironment.SparkCLRProxy;
            try
            {
                SparkCLREnvironment.SparkCLRProxy = sparkClrProxy.Object;

                var sparkConf = new SparkConf(false);
                var ssc = new StreamingContext(new SparkContext(sparkContextProxy.Object, sparkConf), 10000L);

                var dstreamProxy = new Mock<IDStreamProxy>();
                var pairDStream = new DStream<Tuple<string, int>>(dstreamProxy.Object, ssc);

                var stateSpec = new StateSpec<string, int, int, int>((k, v, s) => v);
                var stateDStream = pairDStream.MapWithState(stateSpec);
                var snapshotDStream = stateDStream.StateSnapshots();

                Assert.IsNotNull(stateDStream);
                Assert.IsNotNull(snapshotDStream);
            }
            finally
            {
                SparkCLREnvironment.SparkCLRProxy = originalSparkCLRProxy;
            }
        }

        [Test]
        public void TestDStreamMapWithStateMapWithStateHelper()
        {
            // test when initialStateRdd is null
            var stateSpec = new StateSpec<string, int, int, int>((k, v, s) => v).NumPartitions(2).Timeout(TimeSpan.FromSeconds(100));
            var helper = new MapWithStateHelper<string, int, int, int>((t, rdd) => rdd, stateSpec);

            var sparkContextProxy = new Mock<ISparkContextProxy>();
            var sc = new SparkContext(sparkContextProxy.Object, null);

            var pairwiseRddProxy = new Mock<IRDDProxy>();
            sparkContextProxy.Setup(p => p.CreatePairwiseRDD(It.IsAny<IRDDProxy>(), It.IsAny<int>(), It.IsAny<long>())).Returns(pairwiseRddProxy.Object);

            var pipelinedRddProxy = new Mock<IRDDProxy>();
            pipelinedRddProxy.Setup(p => p.Union(It.IsAny<IRDDProxy>())).Returns(new Mock<IRDDProxy>().Object);

            sparkContextProxy.Setup(p =>
                p.CreateCSharpRdd(It.IsAny<IRDDProxy>(), It.IsAny<byte[]>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<List<string>>(), It.IsAny<bool>(), It.IsAny<List<Broadcast>>(), It.IsAny<List<byte[]>>()))
                .Returns(pipelinedRddProxy.Object);

            var valueRddProxy = new Mock<IRDDProxy>();
            var valuesRdd = new RDD<dynamic>(valueRddProxy.Object, sc);

            var resultRdd = helper.Execute(DateTime.UtcNow.Millisecond, null, valuesRdd);

            Assert.IsNotNull(resultRdd);

            // test when initialStateRdd is not null
            var initialStateRdd = new RDD<Tuple<string, int>>(new Mock<IRDDProxy>().Object, null);
            var stateSpec2 = new StateSpec<string, int, int, int>((k, v, s) => v).InitialState(initialStateRdd).NumPartitions(2);
            var helper2 = new MapWithStateHelper<string, int, int, int>((t, rdd) => rdd, stateSpec2);

            var resultRdd2 = helper2.Execute(DateTime.UtcNow.Millisecond, null, valuesRdd);

            Assert.IsNotNull(resultRdd2);
        }

        [Test]
        public void TestDStreamMapWithStateUpdateStateHelper()
        {
            var ticks = DateTime.UtcNow.Ticks;
            var helper = new UpdateStateHelper<string, int, int, int>(
                                (k, v, state) => UpdateState(k, v, state),

                ticks, true, TimeSpan.FromSeconds(10));

            var input = new dynamic[4];

            var preStateRddRecord = new MapWithStateRDDRecord<string, int, int>(ticks - TimeSpan.FromSeconds(2).Ticks, new[] { new Tuple<string, int>("1", 1), new Tuple<string, int>("2", 2) });
            preStateRddRecord.stateMap.Add("expired", new KeyedState<int>(0, ticks - TimeSpan.FromSeconds(60).Ticks));

            input[0] = preStateRddRecord;
            input[1] = new Tuple<string, int>("1", -1);
            input[2] = new Tuple<string, int>("2", 2);
            input[3] = new Tuple<string, int>("3", 3);

            var result = helper.Execute(1, input).GetEnumerator();
            Assert.IsNotNull(result);
            Assert.IsTrue(result.MoveNext());

            MapWithStateRDDRecord<string, int, int> stateRddRecord = result.Current;

            Assert.IsNotNull(stateRddRecord);
            Assert.AreEqual(stateRddRecord.mappedData.Count, 4); // timedout record also appears in return results
            Assert.AreEqual(stateRddRecord.stateMap.Count, 2);
        }

        public static int UpdateState(String k, int v, State<int> state)
        {
            if (v < 0 && state.Exists())
            {
                state.Remove();
            }
            else if (!state.IsTimingOut())
            {
                state.Update(v + state.Get());
            }

            return v;
        }

        [Test]
        public void TestConstantInputDStream()
        {
            var sc = new SparkContext("", "");
            var rdd = sc.Parallelize(Enumerable.Range(0, 10), 1);
            var ssc = new StreamingContext(sc, 1000L);

            // test when rdd is null
            Assert.Throws<ArgumentNullException>(() => new ConstantInputDStream<int>(null, ssc));

            var constantInputDStream = new ConstantInputDStream<int>(rdd, ssc);
            Assert.IsNotNull(constantInputDStream);
            Assert.AreEqual(ssc, constantInputDStream.streamingContext);
        }

        [Test]
        public void TestCSharpInputDStream()
        {
            // test create CSharpInputDStream
            var sc = new SparkContext("", "");
            var ssc = new StreamingContext(sc, 1000L);
            Expression<Func<double, int, IEnumerable<string>>> func =
                (double time, int pid) =>
                    new List<string>() { string.Format("PluggableInputDStream-{0}-{1}", pid, time) }.AsEnumerable();
            const int numPartitions = 5;
            var inputDStream = CSharpInputDStreamUtils.CreateStream<string>(
                ssc,
                numPartitions,
                func);
            Assert.IsNotNull(inputDStream);
            Assert.AreEqual(ssc, inputDStream.streamingContext);

            // test CSharpInputDStreamMapPartitionWithIndexHelper
            int[] array = new int[numPartitions];
            int partitionIndex = 0;
            new CSharpInputDStreamMapPartitionWithIndexHelper<string>(0.0, func).Execute(partitionIndex, array.AsEnumerable());

            // test CSharpInputDStreamGenerateRDDHelper
            new CSharpInputDStreamGenerateRDDHelper<string>(numPartitions, func).Execute(0.0);
        }
    }
}
