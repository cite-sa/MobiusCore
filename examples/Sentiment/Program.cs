using Microsoft.Spark.CSharp.Core;
using System;
using Sentiment.Analyzers;
using System.Diagnostics;
using System.IO;
using SentimentAnalysisConsoleApp.DataStructures;
using System.Collections.Generic;
using System.Linq;

namespace Sentiment
{
	class Program
	{
		static void Main(string[] args)
		{
			var inputScriptFolder = args[0];
			var modelPath = args[1];

			var sparkContext = new SparkContext(new SparkConf());
			var analyzer = new SentimentAnalyzer(modelPath);
			var gotScripts = Directory.EnumerateFiles(inputScriptFolder, "*.txt", SearchOption.AllDirectories);
			RDD<string> lines = sparkContext.TextFile(String.Join(",", gotScripts));

			RDD<string> daenerysLines = lines
				.Filter(line => line.StartsWith("DAENERYS:", StringComparison.OrdinalIgnoreCase)
							|| line.StartsWith("DAENERYS TARGARYEN:", StringComparison.OrdinalIgnoreCase));

			long daenerysLinesTotalCount = daenerysLines
				.Count();

			var negativeDaenerysLines = daenerysLines
				.Map(line => analyzer.Predict(line))
				.Filter(eval => eval.IsToxic)
				.Collect()
				.OrderByDescending(x => x.ToxicityPropability);

			var negativeDaenerysLinesCount = negativeDaenerysLines.Count();

			var negativeLinesPercentage = Math.Round((double)(100 * negativeDaenerysLinesCount) / daenerysLinesTotalCount, 2);
			Console.WriteLine($"Negative Lines Percentage: { negativeLinesPercentage } %");
			Console.WriteLine($"Negative Lines:\n { String.Join("\n", negativeDaenerysLines) } ");
			sparkContext.Stop();
		}
	}
}
