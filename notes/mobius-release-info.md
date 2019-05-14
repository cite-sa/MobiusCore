# GitHub Release
The [release in GitHub](https://github.com/cite-sa/MobiusCore/releases) is a zip file. When you unzip that file, you will see a directory layout as follows:

````    
|-- mobius-release-info.md
|-- runtime
    |-- bin
        |-- .NET binaries and its dependencies used by MobiusCore applications
    |-- dependencies    
        |-- jar files Mobius depends on for functionality like CSV parsing, Kafka message processing etc.        
    |-- lib
        |-- MobiusCore jar file
    |-- repl
        |-- .NET binaries and its dependencies used by C# REPL shell in MobiusCore
    |-- scripts
        |-- MobiusCore job submission scripts
|-- examples
    |-- Example MobiusCore applications in C#
    |-- fsharp
        |-- Example MobiusCore applications in F#
|-- samples
    |-- C# Spark driver samples for MobiusCore API 
    |-- data    
        |-- Data files used by the samples
```` 

Instructions on running a MobiusCore app is available at https://github.com/cite-sa/MobiusCore/blob/master/notes/running-mobius-app.md

MobiusCore samples do not have any external dependencies. The dependent jar files and data files used by samples are included in the release. Instructions to run samples are available at
* **Windows**: https://github.com/cite-sa/MobiusCore/blob/master/notes/windows-instructions.md#running-samples
* **Linux**: https://github.com/cite-sa/MobiusCore/blob/master/notes/linux-instructions.md#running-mobius-samples-in-linux

Mobius examples under "examples" folder may have external dependencies and may need configuration settings to those dependencies before they can be run. Refer to [Running Examples](https://github.com/cite-sa/MobiusCore/blob/master/notes/running-mobius-app.md#running-mobius-examples-in-local-mode) for details on how to run each example.
