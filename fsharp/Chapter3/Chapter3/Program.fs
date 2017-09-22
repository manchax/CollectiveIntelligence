namespace Chapter3

open System
open System.Diagnostics
open SiteIndexer

module Main =
    let totalCount (wordEntries:List<WordMatches>) =
        query { for w in wordEntries do 
                select w.Positions.Length } |> Seq.sum 

    let private print (word, count) =
        printfn "%s : %d" word count

    [<EntryPoint>]
    let main argv = 
        let timer = new Stopwatch()    
        timer.Start()
        //deleteData
        let wordIndex = readSites "\\Sites.xml" DateTime.Now
        timer.Stop()
        printfn "Catalogued Words: %i in %.2f seconds" wordIndex.Count timer.Elapsed.TotalSeconds    
        query { for w in wordIndex do    
                let count = (totalCount w.Value)
                //where ( count < (uint16 100) )
                sortByDescending count
                select (w.Key, count)
                take 50 } |> 
        Seq.iter print
        
        Console.ReadLine() |> ignore
        0 // return an integer exit code