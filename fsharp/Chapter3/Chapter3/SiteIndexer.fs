namespace Chapter3
module SiteIndexer =
    open System
    open System.Data
    open System.Linq
    open System.Xml.Linq
    open System.Diagnostics
    open System.IO
    open System.Text
    open System.Text.RegularExpressions
    open System.Threading.Tasks
    open System.Net
    open Microsoft.FSharp.Control.CommonExtensions
    open HtmlAgilityPack
    open Microsoft.FSharp.Data.TypeProviders
    open Microsoft.FSharp.Linq
    open Microsoft.FSharp.Control.WebExtensions
    open System.Collections.Concurrent
    type WordMatches = { Link: string; Positions: List<int> }
    type dbSchema = SqlDataConnection<"Data Source=MANCHAX-LAP;Initial Catalog=NewsIndex;Integrated Security=True; Pooling=False">
    let private putLimitTo = None
    //let private db = dbSchema.GetDataContext()
    let mutable private linkCache = new ConcurrentDictionary<string, int>()
    let mutable private wordCache = new ConcurrentDictionary<string, int>()
    
    let private findLinkID link =        
        if linkCache.ContainsKey(link) then
            Some(linkCache.[link])
        else
            sprintf "%s Looking into DB for %s" (DateTime.Now.ToShortTimeString()) link |> Debug.WriteLine
            use db = dbSchema.GetDataContext()
            let existing = db.DataContext.ExecuteQuery<int>("SELECT ID FROM Links WHERE Link = {0}", link) |> Seq.toArray
            match existing.Length with 
            | c when c > 0 -> 
                let id = existing.[0]
                let id = linkCache.AddOrUpdate(link, id, (fun _ oldValue -> oldValue) )
                Some(id)
            | _ -> None

    let private findWordID word = 
        use db = dbSchema.GetDataContext()
        if wordCache.ContainsKey(word) then
            Some(wordCache.[word])
        else
            let q = (query { for w in db.Words do
                            where ( w.Word = word )
                            select w.ID
                            take 1 }).FirstOrDefault()
            match q with 
            | id when id > 0 ->                 
                let id = wordCache.AddOrUpdate(word, id, (fun _ oldValue -> oldValue) ) 
                Some(id)
            | _ -> None
    
    let private ignoredWords = ["cómo"; "como"; "está"; "tica"; "ante"; "sobre"; "entre"; "qué"; "sin"; "ya"; "han"; "hemos"; "ha"; "tu"; "hace"; "tras"; "más"; "lo"; "y"; "si"; "no"; "de"; "del"; "para"; "pero"; "como"; "por"; "que"; "al"; "es"; "son"; "fue"; "ha"; "quien"; "en"; "la"; "el"; "los"; "las"; "un"; "uno"; "una"; "unos"; "su"; "sus"; "se"; "ser"; "si"; "este"; "esta"; "eso"; "esa"; "con"; 
    "from"; "to"; "and"; "of"; "the"; "a"; "in"; "at"; "as"; "on"; "an"; "not"; "for"; "or"; "has"; "have"; "had"; "i"; "you"; "he"; "she"; "it"; "we"; "they"; "with"; "by"; 
    "will"; "be"; "so"; "mine"; "his"; "her"; "your"; "our"; "ours"; "their"; "theirs"; "who"; "which"; "whose"; "what"; "when"; "why"; "would"; "let"; "but"; "many"; "much"; "this"; "that"; "those"; "these"; "how"; "do"; "does"; "did"; "was"; "were"; "is"; "are";
    "com"; "css"; "ago"; "http"; "form"; "more"; "since"; "www"; "me"; "him"; "her"; "us"; "them"] |> Set.ofList

    // Shorthand - let the compiler do the work (used for XName cast to string with operator !!)
    let inline private implicit arg =
      ( ^a : (static member op_Implicit : ^b -> ^a) arg)

    let private (!!) : string -> XName = implicit

    let private clean (text:string) : string = 
        let mutable out = WebUtility.HtmlDecode(text)    
        out <- Regex.Replace(out, @"(\r\n){2,}", "\r\n")
        out <- Regex.Replace(out, @"\n{2,}", "\n")
        out <- Regex.Replace(out, @"\s{2,}", " ")
        out <- Regex.Replace(out, @"\t", " ")
        out <- Regex.Replace(out, @"^ +", "", RegexOptions.Multiline) //starting line spaces
        out <- Regex.Replace(out, @" +$", "", RegexOptions.Multiline) //ending line spaces
        out <- Regex.Replace(out, @"<!--\s*.*\s*-->", "")                
        out <- Regex.Replace(out, @"([a-z])([A-Z])", "$1 $2") //contiguous words likeThis   
        out <- Regex.Replace(out, @"(-)(\w*)", "$2") //remove '-' from start of word
        out <- Regex.Replace(out, @"\b\d+\b", "")
        out

    let private addPosition (allWords:Map<string, List<int>>) (word:string) (position:int) =
        let wordL = word.ToLower().Trim()
        let wordT = word.Trim()
        let mutable words = allWords
        if not(ignoredWords.Contains(wordL)) then 
            let positions = match words.ContainsKey(wordT) with
                | true -> position :: words.[wordT]
                | false -> [position]
            words <- words.Add(wordT, positions)                 
        words

    let private parseWords text : Map<string,List<int>> = 
        let mutable wordIndex = Map.empty    
        //parse capital words to keep personal names together
        for m in Regex.Matches(text, @"([A-Z][\w']+\s?)+") do
            Profiling.beginSnapshot()
            let capital_words = new StringBuilder()
            let words = m.Value.Split(' ')
            let lastIndex = words.Length-1
            for i in 0..lastIndex do 
                let word = match i with 
                    | _ when (i = 0 || i = lastIndex) && not(ignoredWords.Contains(words.[i].ToLower())) -> 
                        words.[i] + " "
                    | _ -> " " 
                capital_words.Append(word.Trim()) |> ignore
            if capital_words.Length > 0 then
                wordIndex <- addPosition wordIndex (capital_words.ToString()) m.Index
            Debug.WriteLine(Profiling.wordProcessed())
        //continue indexing all other words
        for m in Regex.Matches(text, @"\b([a-z0-9][\w']{2,})\b") do
            Profiling.beginSnapshot()
            if not(ignoredWords.Contains(m.Value.ToLower())) then
                wordIndex <- addPosition wordIndex m.Value m.Index
            Debug.WriteLine(Profiling.wordProcessed())
        wordIndex

    let private addLink (matches:List<WordMatches>) link (positions:List<int>) = 
        let arr = matches |> List.toArray 
        let found = arr |> Array.tryFindIndex ( fun p -> p.Link = link )
        match found with 
            | None -> {Link = link; Positions = positions} :: matches
            | _ -> 
                [for i in 0..arr.Length-1 do
                    if i = found.Value then 
                        yield {Link = link; Positions = arr.[i].Positions @ positions }
                    else
                        yield arr.[i] ]
                                
    let private mergeToIndex (index:Map<string, List<WordMatches>>) (words:Map<string,List<int>>, link) =
        let mutable finalIndex = index
        for w in words do  
            let matches = match finalIndex.ContainsKey(w.Key) with 
                | true -> finalIndex.[w.Key]
                | false -> []
            let matches = addLink matches link w.Value
            finalIndex <- finalIndex.Add(w.Key, matches)
        finalIndex

    let private removeNodes (nodes:HtmlNodeCollection) = 
        if nodes <> null then
            for node in nodes do
                if node <> null then
                    node.ParentNode.RemoveChild(node) |> ignore

    let private parseBody (body:HtmlNode) (site, channel, uri:Uri) = 
        body.SelectNodes("//script|//style|//img|//comment()") |> removeNodes
        let bodyText = body.InnerText |> clean 
        async { 
            let dirName = Path.Combine("data", site, channel)
            if not(Directory.Exists(dirName)) then
                Directory.CreateDirectory(dirName) |> ignore
            let fileName = match Path.GetFileNameWithoutExtension(uri.LocalPath) + ".txt" with
                | name when File.Exists(Path.Combine(dirName, name)) -> name + "_" + Guid.NewGuid().ToString() + ".txt"
                | name -> name
            let fileName = Path.Combine(dirName, fileName)
            let buffer = UTF8Encoding.UTF8.GetBytes(bodyText)
            use file = File.Create(fileName)
            do! file.AsyncWrite(buffer)
        } |> Async.Start
        bodyText |> parseWords 

    let private saveLink url = 
        use db = dbSchema.GetDataContext()
        let id = findLinkID url
        match id with 
        | None -> 
            new dbSchema.ServiceTypes.Links(Link = url) |> db.Links.InsertOnSubmit
            try 
                db.DataContext.SubmitChanges()
                let id = db.DataContext.ExecuteQuery<int>("SELECT MAX(ID) FROM Links").First()
                linkCache.AddOrUpdate(url, id, (fun _ oldValue -> oldValue) )
            with ex -> 
                printfn "Error al guardar link. Message: %s" ex.Message; 0
        | Some(value) -> value

    let private parseHtml (site, channel) (url:string) =    
        let webClient = new WebClient() 
        webClient.Headers.["user-agent"] <- "Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko"
        let uri = new Uri(url)    
        let linkID = saveLink url
        async {
            try 
                let doc = new HtmlDocument()
                let! data = webClient.AsyncDownloadString(uri)                
                let enc = match doc.DetectEncodingHtml(data) with
                    | null -> webClient.Encoding
                    | e when e <> doc.DeclaredEncoding -> doc.DeclaredEncoding
                    | e -> e                
                webClient.Encoding.GetBytes(data) |> enc.GetString |> doc.LoadHtml
                let words = match doc.DocumentNode.SelectSingleNode("//body") with  
                    | null -> Map.empty<string, List<int>>
                    | b -> parseBody b (site, channel, uri)                                              
                return (words, linkID)
            with | ex -> 
                printfn "Error reading HTML from %s. Error: %s." (uri.ToString()) ex.Message
                return (Map.empty<string, List<int>>, linkID)
        }

    let private createWordIndex (site, channel) (i:XElement) =   
        let link = i.Element(!!"link").Value        
        async {            
            let! (pageWords, linkID) = parseHtml (site, channel) link 
            use db = dbSchema.GetDataContext()
            for kv in pageWords do 
                let word = kv.Key                        
                let wordID = match (findWordID word) with
                    | Some(value) -> value 
                    | None ->                         
                        new dbSchema.ServiceTypes.Words(Word = word) |> db.Words.InsertOnSubmit
                        try 
                            db.DataContext.SubmitChanges()
                            match findWordID word with
                                | Some(id) -> id
                                | None -> failwith "Error al guardar Palabra"
                        with ex -> 
                            printfn "Error al guardar Palabra %s. Message: %s" word ex.Message; 0  

                let parent = 
                    new dbSchema.ServiceTypes.WordsLinks(   LinkID = linkID, 
                                                            WordID = wordID, 
                                                            Count = kv.Value.Length )                                 
                for p in kv.Value do
                    parent.WordsLinksPositions.Add(
                        new dbSchema.ServiceTypes.WordsLinksPositions(
                            Position = p                            
                        )
                    )

                parent |> db.WordsLinks.InsertOnSubmit                                                                                   
                //arent.WordsLinksPositions |> db.WordsLinksPositions.InsertAllOnSubmit

                try 
                    db.DataContext.SubmitChanges()                    
                with ex -> 
                    printfn "Error al guardar alguna o varias Posiciones de Palabra %s en Sitio %s. Message: %s" kv.Key link ex.Message; 0  

            return (pageWords, link)

        }

    let private readChannel site (e:XElement) =   
        let url = e.Attribute(!!"url").Value 
        let name = match e.Attribute(!!"name") with
                    | null -> "default"
                    | value -> value.Value          
        async {  
            //use reference cell instead of mutable because its within async block
            //NOTE: Use operator ! to get ref's value
            let wordIndex = ref(Map.empty<string, List<WordMatches>>)            
            let doc = 
                try Some(XDocument.Load(url))
                with | ex -> printfn "Exception loading URL: %s. Message: %s" url ex.Message; None   

            if doc.IsSome then                
                let items = doc.Value.Descendants(!!"item")
                let items = 
                    match items.Count() with 
                        | count when putLimitTo.IsSome && count > putLimitTo.Value -> 
                            items |> Seq.take putLimitTo.Value //limit to 10 max
                        | _ -> items
                printfn "Reading channel %s. Items: %i" name (items.Count())
                //execute in parallel workflow using a fork/join pattern
                Async.Parallel [for i in items -> createWordIndex (site, name) i] |> 
                Async.RunSynchronously |> Array.iter ( fun i -> 
                    //merge results into wordIndex
                    wordIndex := mergeToIndex !wordIndex i
                )

            return !wordIndex             
        }

    let private mergeIndexes (indexes:Map<string, List<WordMatches>>[]) = 
        let mutable finalIndex = Map.empty<string, List<WordMatches>>
        for i in indexes do            
            for kv in i do
                let matches = 
                    match finalIndex.ContainsKey(kv.Key) with 
                    | false -> kv.Value
                    | true -> 
                        let mutable mergedIndex = finalIndex.[kv.Key]                       
                        for m in kv.Value do 
                            mergedIndex <- addLink mergedIndex m.Link m.Positions 
                        mergedIndex                                  
                finalIndex <- finalIndex.Add(kv.Key, matches)
        finalIndex
        
    let readSites fileName = 
        Profiling.init()
        if Directory.Exists("data") then
            Directory.Delete("data", true)
        let doc = XDocument.Load(AppDomain.CurrentDomain.BaseDirectory + fileName)
        doc.Root.Elements(!!"site") |> Seq.map( fun e -> 
            let site = e.Attribute(!!"name").Value
            printfn "Reading site %s" site
            let channels = e.Descendants(!!"channel") 
            let channels = match channels.Count() with 
                            | count when putLimitTo.IsSome && count > putLimitTo.Value -> 
                                channels |> Seq.take putLimitTo.Value
                            | _ -> channels
            async {            
                return Async.Parallel [for c in channels -> readChannel site c] |> 
                Async.RunSynchronously |> 
                mergeIndexes
            }
        ) |> 
        Async.Parallel |> 
        Async.RunSynchronously |> 
        mergeIndexes    

    let saveIndex (index:Map<string, List<WordMatches>>) = 
        use db = dbSchema.GetDataContext()
        db.DataContext.ExecuteCommand("DELETE FROM WordsLinksPositions") |> ignore  
        db.DataContext.ExecuteCommand("DELETE FROM WordsLinks") |> ignore    
        db.DataContext.ExecuteCommand("DELETE FROM Words") |> ignore    
        db.DataContext.ExecuteCommand("DELETE FROM Links") |> ignore    
