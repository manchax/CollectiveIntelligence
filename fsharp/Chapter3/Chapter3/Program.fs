namespace Chapter3

open System
open System.Diagnostics
open SiteIndexer
module Main =
    let totalCount (wordEntries:List<WordMatches>) =
        query { for w in wordEntries do 
                select w.Positions.Length } |> Seq.sum   

    [<EntryPoint>]
    let main argv = 
        let timer = new Stopwatch()    
        timer.Start()
        let wordIndex = readSites "\\Sites.xml"
        timer.Stop()
        printfn "Catalogued Words: %i in %.2f seconds" wordIndex.Count timer.Elapsed.TotalSeconds    
        query { for w in wordIndex do    
                let count = (totalCount w.Value)
                //where ( count < (uint16 100) )
                sortByDescending count
                select (w, count)
                take 50 } |> 
        Seq.iter ( fun (w, c) ->                 
            printfn "%s : %d" w.Key c )
        
        Console.ReadLine() |> ignore
        0 // return an integer exit code