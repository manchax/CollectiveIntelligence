open System.IO
open System.Text
open iTextSharp.text.pdf
open iTextSharp.text.pdf.parser

let readPdf (fileName:string) =    
    let text = new StringBuilder()
    use doc = new PdfReader(fileName)
    for p in 1..doc.NumberOfPages do
        PdfTextExtractor.GetTextFromPage (doc, p) |> text.Append |> ignore
    doc.Close()
    text.ToString()

[<EntryPoint>]
let main argv = 
    if argv.Length < 1 then failwith "Missing argument: [fileName]"    
    let text = readPdf argv.[0]
    printfn "%A" text
    0 // return an integer exit code
