namespace Chapter3

module GraphDB =
    open System
    open Microsoft.FSharp.Collections
    open System.Collections.Generic
    open Neo4jClient
    open Neo4jClient.Cypher
    open GraphModel

    let createClient () = 
        let client = new GraphClient(new Uri("http://mvillavi-3.desktop.amazon.com:7474"))
        client.Connect()
        client :> IGraphClient

    /// creates a new link in neo4j
    let saveLink (url:string, outLinks:seq<string>) =
        let c = createClient ()
        let node = new URLNode(url)
        c.Create(node) |> ignore
        for l in outLinks do
            c.CreateRelationship(node, new LinkTo(new URLNode(l))) |> ignore
        node

    let findOrCreateURL url =
        let c = createClient ()
        let results = 
            c.Cypher.Match("(w:URL)").
                Where(fun (w:URLNode) -> w.URL = url ).
                Return( fun (w:ICypherResultItem) -> w.As<URLNode>() ).
                Results |> Seq.toList
        
        if results.Length = 0 then
            c.Cypher.Create("(w:URL {newURL})").
                WithParam("newURL", new URLNode(url)).
                ExecuteWithoutResults()

        c.Cypher.Match("(w:URL)").
            Where(fun (w:URLNode) -> w.URL = url ).
            Return( fun (w:ICypherResultItem) -> w.As<URLNode>() ).
            Results |> Seq.head
    
    let saveWord word (positions:IEnumerable<WordPosition>) (link:URLNode) =
        let c = createClient()
        let w = new WordNode(word, positions)
        let wNode = c.Create(w)
        c.CreateRelationship(wNode, new WordInURL(link)) |> ignore
        w

