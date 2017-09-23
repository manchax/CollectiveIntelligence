namespace Chapter3
module SiteIndexer =
    open System
    open System.Linq
    open System.Xml.Linq
    open System.Diagnostics
    open System.IO
    open System.Text
    open System.Text.RegularExpressions
    open System.Net
    open Microsoft.FSharp.Control.CommonExtensions
    open HtmlAgilityPack
    open FSharp.Data.TypeProviders
    open Microsoft.FSharp.Linq
    open Microsoft.FSharp.Control.WebExtensions
    open System.Collections.Concurrent
    open LemmaSharp
    open System.Data.Linq

    type WordMatches = { Link: string; Positions: List<int> }
    type dbSchema = SqlDataConnection<"Data Source=localhost; Initial Catalog=NewsIndex;Integrated Security=True">
    let private putLimitTo = None
    //let private db = dbSchema.GetDataContext()
    let private linkCache = new ConcurrentDictionary<string, dbSchema.ServiceTypes.Links>()
    let private wordCache = new ConcurrentDictionary<string, int>(StringComparer.OrdinalIgnoreCase)
    let private lemmatizer = new LemmatizerPrebuiltFull(LanguagePrebuilt.Spanish)

    let private updateReadOn linkID date =
        use db = dbSchema.GetDataContext()
        db.DataContext.ExecuteCommand("UPDATE Links SET ReadOn = {0} WHERE ID = {1}", date, linkID)

    let private findLinkID link newDate =        
        if linkCache.ContainsKey(link) then
            updateReadOn linkCache.[link].ID newDate |> ignore
            Some(linkCache.[link].ID)
        else
            sprintf "%s Looking into DB for %s" (DateTime.Now.ToShortTimeString()) link |> Debug.WriteLine
            use db = dbSchema.GetDataContext()
            let existing = db.Links.Where(fun l -> l.Link = link)
            //let existing = db.DataContext.ExecuteQuery<dbSchema.ServiceTypes.Links>("SELECT * FROM Links WHERE Link = {0}", link) |> Seq.toArray
            match existing.Any() with 
            | true ->
                let found = existing.First()
                if found.ReadOn <> newDate then
                    updateReadOn found.ID newDate |> ignore
                linkCache.AddOrUpdate(link, found, (fun _ oldValue -> oldValue) ) |> ignore
                Some(found.ID)
            | false -> None

    let private findWordID word = 
        use db = dbSchema.GetDataContext()
        if wordCache.ContainsKey(word) then
            Some(wordCache.[word])
        else
            let q = (query { for w in db.Words do
                                where ( w.Word.ToLower() = word.ToLower() )
                                select w.ID
                                take 1 }).FirstOrDefault()
            match q with 
            | id when id > 0 ->
                let id = wordCache.AddOrUpdate(word, id, (fun _ oldValue -> oldValue) ) 
                Some(id)
            | _ -> None
    
    let private ignoredWords = [""; "cómo"; "como"; "está"; "ante"; "bajo"; "sobre"; "entre"; "qué"; "sin"; 
    "ya"; "han"; "hemos"; "ha"; "tu"; "hace"; "tras"; "más"; "lo"; "y"; "si"; "no"; "de"; "del"; "para"; 
    "pero"; "como"; "por"; "que"; "al"; "es"; "son"; "fue"; "ha"; "quien"; "en"; "la"; "el"; "le"; "los"; "las"; 
    "un"; "uno"; "una"; "unos"; "su"; "sus"; "se"; "ser"; "si"; "éste"; "esta"; "eso"; "esa"; "con"; 
    "from"; "to"; "and"; "of"; "the"; "a"; "in"; "at"; "as"; "on"; "an"; "not"; "for"; "or"; "has"; 
    "have"; "had"; "i"; "you"; "he"; "she"; "it"; "we"; "they"; "with"; "by"; "will"; 
    "be"; "so"; "mine"; "his"; "her"; "your"; "our"; "ours"; "their"; "theirs"; "who"; 
    "which"; "whose"; "what"; "when"; "why"; "would"; "let"; "but"; "many"; "much"; "this"; "that"; "those"; "these"; 
    "how"; "do"; "does"; "did"; "was"; "were"; "is"; "are"; "com"; "css"; "days"; "hours"; "ago"; "http"; "through";
    "form"; "more"; "since"; "www"; "me"; "him"; "her"; "us"; "them"; 
    "horas"; "tener"; "ser"; "estar"; "haber"; "todo"; "hacer"; "así"; "para"; "hoy"; "aquí" ] |> List.sort |> Set.ofList

    // Shorthand - let the compiler do the work (used for XName cast to string with operator !!)
    let inline private implicit arg =
      ( ^a : (static member op_Implicit : ^b -> ^a) arg)

    let private (!!) : string -> XName = implicit

    let private clean (text:string) : string = 
        let mutable out = WebUtility.HtmlDecode(text)
        out <- Regex.Replace(out, @"\r\n", " | ")
        out <- Regex.Replace(out, @"\n", " | ")
        out <- Regex.Replace(out, @"\s{2,}", " ")
        out <- Regex.Replace(out, @"\t", " ")
        out <- Regex.Replace(out, @"^ +", "", RegexOptions.Multiline) //starting line spaces
        out <- Regex.Replace(out, @" +$", "", RegexOptions.Multiline) //ending line spaces
        out <- Regex.Replace(out, @"<!--\s*.*\s*-->", "")
        out <- Regex.Replace(out, @"([a-z])([A-Z])", "$1 $2") //contiguous words likeThis   
        out <- Regex.Replace(out, @"(-)(\w*)", "$2") //remove '-' from start of word
        out <- Regex.Replace(out, @"\b\d+\b", "")
        out

    let private addPosition (words:Map<string, List<int>>) word position =
        let positions = match words.ContainsKey(word) with
                | true -> position :: words.[word]
                | false -> [position]
        words.Add(word, positions)

    let private parseWords text : Map<string,List<int>> = 
        let mutable wordIndex = Map.empty    
        //parse capital words to keep personal names together
        for m in Regex.Matches(text, @"([A-Z][\wáéíóúüñ.]+\s?(del|de)?\s?)+") do
            wordIndex <- match m.Value.Trim().Split(' ').Length with
                | 1 when not(ignoredWords.Contains(m.Value.Trim().ToLower())) -> 
                    addPosition wordIndex (m.Value.Trim()) m.Index
                | len when len > 1 -> 
                    addPosition wordIndex (m.Value.Trim()) m.Index
                | _ -> wordIndex
        //continue indexing all other words
        for m in Regex.Matches(text, @"(?:\s)([a-záéíóúüñ][\wáéíóúüñ]+)") do
            let lemma = lemmatizer.Lemmatize(m.Value.Trim()).ToLower().Trim()
            //System.Diagnostics.Debug.WriteLine("Lemmatizing {0} to {1}", m.Value.Trim(), lemma)
            if not(ignoredWords.Contains(lemma)) then
                wordIndex <- addPosition wordIndex lemma m.Index
                wordIndex <- addPosition wordIndex (m.Value.Trim().ToLower()) m.Index
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
                | name when File.Exists(Path.Combine(dirName, name)) -> 
                    name + "_" + Guid.NewGuid().ToString() + ".txt"
                | name -> name
            let fileName = Path.Combine(dirName, fileName)
            use file = File.Create(fileName)
            let buffer = UTF8Encoding.UTF8.GetBytes(bodyText)
            do! file.AsyncWrite buffer
        } |> Async.Start
        bodyText |> parseWords

    let private saveLink url date = 
        let id = findLinkID url date
        match id with 
        | None -> 
            let link = new dbSchema.ServiceTypes.Links(Link = url, ReadOn = date)
            use db = dbSchema.GetDataContext()
            db.Links.InsertOnSubmit(link)
            try 
                db.DataContext.SubmitChanges()
                linkCache.AddOrUpdate(url, link, (fun _ oldValue -> oldValue) ).ID
            with ex -> 
                printfn "Error al guardar link. Message: %s" ex.Message; 0
        | Some(value) -> value

    let private parseHtml (site, channel) url date =
        let webClient = new WebClient() 
        webClient.Encoding <- Encoding.UTF8
        webClient.Headers.["user-agent"] <- "Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko"
        let uri = new Uri(url)    
        let linkID = saveLink url date
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

    let private createWordIndex (site, channel) (i:XElement) date =   
        let link = i.Element(!!"link").Value        
        async {            
            let! (pageWords, linkID) = parseHtml (site, channel) link date
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
                parent |> db.WordsLinks.InsertOnSubmit
                try 
                    db.DataContext.SubmitChanges()                    
                with ex -> 
                    printfn "Error al guardar alguna o varias Posiciones de Palabra %s en Sitio %s. Message: %s" kv.Key link ex.Message

                for p in kv.Value do
                    new dbSchema.ServiceTypes.WordsLinksPositions(
                        WordLinkID = parent.ID,
                        Position = p)
                    |> db.WordsLinksPositions.InsertOnSubmit

                try 
                    db.DataContext.SubmitChanges()                    
                with ex -> 
                    printfn "Error al guardar alguna o varias Posiciones de Palabra %s en Sitio %s. Message: %s" kv.Key link ex.Message

            return (pageWords, link)
        }

    let private readChannel site (e:XElement) date =   
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
                printfn "%s - Reading channel %s. Items: %i" site name (items.Count())
                //execute in parallel workflow using a fork/join pattern
                Async.Parallel [for i in items -> createWordIndex (site, name) i date] |> 
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
        
    let readSites fileName (date:DateTime) = 
        if Directory.Exists("data_"+ date.Ticks.ToString()) then
            Directory.Delete("data_"+ date.Ticks.ToString(), true)
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
                return Async.Parallel [for c in channels -> readChannel site c date] |> 
                Async.RunSynchronously |> 
                mergeIndexes
            }
        ) |> 
        Async.Parallel |> 
        Async.RunSynchronously |> 
        mergeIndexes

    let deleteData = 
        use db = dbSchema.GetDataContext()
        db.DataContext.ExecuteCommand("DELETE FROM WordsLinksPositions") |> ignore  
        db.DataContext.ExecuteCommand("DELETE FROM WordsLinks") |> ignore    
        db.DataContext.ExecuteCommand("DELETE FROM Words") |> ignore    
        db.DataContext.ExecuteCommand("DELETE FROM Links") |> ignore    
