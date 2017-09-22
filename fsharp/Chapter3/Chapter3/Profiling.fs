namespace Chapter3
module Profiling = 
    open System.Diagnostics

    let PERF_COUNTER_CATEGORY = "SiteIndexer" 
    type Counters = 
        | WordProcessed 
        | WordProcessedAvgTime         

    let getIndex c = match c with
        | WordProcessed -> 0
        | WordProcessedAvgTime -> 1
        
    let private create() =         
        PerformanceCounterCategory.Create(
            PERF_COUNTER_CATEGORY, 
            "Site Indexer Counters", 
            PerformanceCounterCategoryType.MultiInstance, 
            new CounterCreationDataCollection(
                [|                     
                    new CounterCreationData("WordProcessedAvgTime", "avg. time to index a word", PerformanceCounterType.AverageTimer32)
                    new CounterCreationData("WordProcessed", "total words indexed", PerformanceCounterType.AverageBase)
                |])).GetCounters()

    let private watch = new Stopwatch()
    let mutable private data = Array.empty
    let init() = 
        data <- match PerformanceCounterCategory.Exists(PERF_COUNTER_CATEGORY) with 
            | false -> create()
            | true -> (new PerformanceCounterCategory(PERF_COUNTER_CATEGORY)).GetCounters() 
        data |> Array.iter ( fun c -> 
            c.ReadOnly <- false 
            c.InstanceName <- "PID: " + System.AppDomain.CurrentDomain.FriendlyName
        )

    let beginSnapshot() = watch.Restart()
    let endSnapshot() = 
        watch.Stop()
        watch.ElapsedMilliseconds
    
    let wordProcessed() = 
        let baseC = data.[(getIndex WordProcessed)]
        let baseAvgTime = data.[(getIndex WordProcessedAvgTime)]
        // increment by miliseconds elapsed
        endSnapshot() |> baseAvgTime.IncrementBy |> ignore
        baseC.Increment() 
    