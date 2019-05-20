using Serialize.Linq.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace SerializationHelpers.Context
{
	public class WorkerAssemblyLoader : IAssemblyLoader
	{
		private readonly List<Assembly> assemblies = new List<Assembly>();

		public IEnumerable<Assembly> GetAssemblies()
		{
			if (assemblies.Count == 0) ResolveAssemblies();
			return assemblies;
		}

		private void ResolveAssemblies()
		{
			var files = Directory.EnumerateFiles(AppDomain.CurrentDomain.BaseDirectory, "*", SearchOption.AllDirectories).Select(Path.GetFullPath).ToArray();

			var asseblyNames = AppDomain.CurrentDomain.GetAssemblies().Select(x => x.GetName().Name).ToList();
			var dlls = files.Where(x => x.EndsWith(".dll") /*asseblyNames.All(y=> !x.StartsWith(y))*/);
			foreach (var dll in dlls)
			{
				try
				{
					var assembly = Assembly.LoadFrom(dll);
					assemblies.Add(assembly);
				}
				catch (Exception ex)
				{
				}
			}
		}
	}
}
