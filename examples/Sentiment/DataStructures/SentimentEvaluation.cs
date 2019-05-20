using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace SentimentAnalysisConsoleApp.DataStructures
{
	[Serializable]
	public class SentimentEvaluation
	{
		public String Text { get; set; }

		public bool IsToxic { get; set; }

		public double ToxicityPropability { get; set; }

		public override string ToString()
		{
			var builder = new StringBuilder();
			builder.AppendLine($"Text: {Text}");
			builder.AppendLine($"Toxicity Prediction: {(Convert.ToBoolean(IsToxic) ? "Toxic" : "Non Toxic")} sentiment | Probability of being toxic: {ToxicityPropability}");
			return builder.ToString();
		}
	}
}
