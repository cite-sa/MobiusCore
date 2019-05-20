using Microsoft.ML;
using SentimentAnalysisConsoleApp.DataStructures;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.Serialization;
using System.Text;
using static Microsoft.ML.DataOperationsCatalog;

namespace Sentiment.Analyzers
{
	[Serializable]
	[DataContract]
	public class SentimentAnalyzer
	{
		[DataMember]
		private readonly string _modelPath;
		private PredictionEngine<SentimentIssue, SentimentPrediction> _engine;

		public SentimentAnalyzer() { }
		public SentimentAnalyzer(String modelPath)
		{
			this._modelPath = modelPath;
		}

		public PredictionEngine<SentimentIssue, SentimentPrediction> GetEngine()
		{
			var mlContext = new MLContext(seed: 1);

			ITransformer trainedModel = mlContext.Model.Load(_modelPath, out var modelInputSchema);
			if (_engine != null)
			{				
				return _engine;
			}
			_engine = mlContext.Model.CreatePredictionEngine<SentimentIssue, SentimentPrediction>(trainedModel);			
			return _engine;
		}

		public SentimentEvaluation Predict(string text)
		{
			SentimentIssue sampleStatement = new SentimentIssue { Text = text };
			var affinResultprediction = this.GetEngine().Predict(sampleStatement);

			return new SentimentEvaluation
			{
				Text = text,
				ToxicityPropability = affinResultprediction.Probability,
				IsToxic = affinResultprediction.Prediction
			};
		}
	}
}
