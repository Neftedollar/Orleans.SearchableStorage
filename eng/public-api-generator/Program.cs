using System;
using System.IO;
using Orleans.SearchableStorage;
using Orleans.SearchableStorage.ApiContract;

var manifest = PublicApiManifest.Generate(typeof(SearchableStorageOptions).Assembly);
if (args.Length == 0)
{
    Console.Write(manifest);
}
else if (args.Length == 1)
{
    File.WriteAllText(args[0], manifest);
}
else
{
    throw new ArgumentException("Pass zero arguments for stdout or one output path.");
}
