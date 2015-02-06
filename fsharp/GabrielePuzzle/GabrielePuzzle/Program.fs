open canopy
open runner
open System
open System.Linq
open Microsoft.FSharp.Collections
open OpenQA.Selenium



let (|Int|_|) str =
    match Int32.TryParse(str) with
    | (true,int) -> Some(int)
    | _ -> None

start firefox

let findTilePosition (tile:IWebElement) = 
    let classes = tile.GetAttribute("class").Split(' ')
    let mutable i = -1
    let mutable j = -1
    for c in classes do
        if c.StartsWith("tile-position") then
            let positions = c.Split('-').[2..]
            j <- Int32.Parse(positions.[0]) - 1
            i <- Int32.Parse(positions.[1]) - 1
    (i,j)

let readMatrix container = 
    let matrix = Array2D.zeroCreate<int> 4 4
    container |> elementsWithin ".tile-new" |> List.iter ( fun t ->         
        let i,j = findTilePosition t
        match (read t) with 
            | Int value -> matrix.[i,j] <- value            
            | _ -> ()
    )
    matrix

type Direction = 
    | Up
    | Down
    | Left 
    | Right

let rec findAdjacentPairs (matrix:int[,]) (start:int*int) = 
    let startI, _ = start
    let maxI = (Array2D.length1 matrix)-1
    let maxJ = (Array2D.length2 matrix)-1
    seq {
        
        for i in startI..maxI do
            let _, startJ = match i with 
                | startI -> start
                | _ -> (0,0)            
            for j in startJ..maxJ do
                if matrix.[i,j] > 0 then
                    let adjacency = ref(false)
                    //try right
                    if j < maxJ-1 then
                        for j2 in j+1..maxJ do
                            if matrix.[i, j2] = matrix.[i,j] then
                                adjacency := true
                                describe "Right Adjacency"
                                yield (Right, matrix.[i,j])                                
                    //try down
                    if not(!adjacency) && i < maxI-1 then
                        for i2 in i+1..maxI do
                            if matrix.[i2, j] = matrix.[i,j] then
                                adjacency := true
                                describe "Down Adjacency"
                                yield (Down, matrix.[i,j])                            
                    
//                    //when no adjacency found, direct towards more spaces available
//                    if !adjacency = false then
//                        let downSpaces = ref(0)
//                        let rightSpaces = ref(0)
//                        // count spaces down
//                        if i < maxI-1 then
//                            for i2 in i+1..maxI do
//                                if matrix.[i2, j] = 0 then
//                                    downSpaces := !downSpaces + 1
//                        //count spaces right
//                        if j < maxJ-1 then
//                            for j2 in j+1..maxJ do  
//                                if matrix.[i, j2] = 0 then
//                                    rightSpaces := !rightSpaces + 1
//
//                        if !rightSpaces > 0 then
//                            describe "Space Right"
//                            yield (Right, matrix.[i,j])
//                        else
//                            describe "Space Down"
//                            yield (Down, matrix.[i,j])
    }

let findDirection (pairs:seq<Direction * int>) = 
    (query { for p in pairs do                                
                groupBy (fst(p)) into g
                sortByDescending (g.Count())
                select (g.Key)
                take 1
    }).FirstOrDefault()

let getMaxValue matrix = 
    let mutable max = 0
    for i in 0..(Array2D.length1 matrix)-1 do
        for j in 0..(Array2D.length2 matrix)-1 do
            if matrix.[i,j] > max then
                max <- matrix.[i,j]
    max

let findNextTile matrix (start: int * int) = 
    let maxI = (Array2D.length1 matrix)-1
    let maxJ = (Array2D.length2 matrix)-1
    let startI, _ = start    
    let mutable next = start
    let mutable found = false
    for i in startI..maxI do
        let _, startJ = match i with 
            | startI -> start
            | _ -> (0,0)
        if startJ < maxJ-1 && not(found) then
            for j in startJ+1..maxJ do
                if matrix.[i,j] > 0 && not(found) then
                    next <- (i,j)
                    found <- true
    next
    

"solve it" &&& fun _ ->
    url "http://gabrielecirulli.github.io/2048/"
    sleep 2
    
    let mutable container = element ".tile-container"
    let mutable matrix = readMatrix container    
    let mutable maxTile = getMaxValue matrix
    let mutable firstTile = findNextTile matrix (0,0)
    while maxTile < 2048 do        
        let pairs = findAdjacentPairs matrix firstTile
        pairs |> Seq.iter ( fun p -> 
            System.Diagnostics.Debug.WriteLine(p) )        
        let direction = findDirection pairs
        
        match direction with 
        | Down -> press down; //describe "Moving down" 
        | Right -> press right; //describe "Moving right" 
        | Up -> press up; //describe "Moving up" 
        | Left -> press left; //describe "Moving left" 
        sleep 2
        container <- element ".tile-container"
        matrix <- readMatrix container
        maxTile <- getMaxValue matrix
        firstTile <- findNextTile matrix (0,0)
    describe "Puzzle loaded!!"

//runs all tests
run()

printfn "press [enter] to exit"
Console.ReadLine() |> ignore

quit()