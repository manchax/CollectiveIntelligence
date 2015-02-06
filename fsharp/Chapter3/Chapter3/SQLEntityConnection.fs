// The SqlEntityConnection (Entity Data Model) TypeProvider allows you to write code that uses 
// a live connection to a database that is represented by the Entity Data Model. For more information, 
// please go to 
//    http://go.microsoft.com/fwlink/?LinkId=229210

module SQLEntityConnection

#if INTERACTIVE
#r "System.Data"
#r "System.Data.Entity"
#r "FSharp.Data.TypeProviders"
#endif

open System.Data
open System.Data.Entity
open Microsoft.FSharp.Data.TypeProviders

// You can use Server Explorer to build your ConnectionString.
type internal SqlConnection = Microsoft.FSharp.Data.TypeProviders.SqlEntityConnection<ConnectionString = @"Data Source=MANCHAX-LAP;Initial Catalog=NewsIndex;Integrated Security=True">
let internal db = SqlConnection.GetDataContext()


//let internal table = query {
//    for r in db.SomeTable do
//    select r
//    }
//
//for p in table do
//    printfn "%s" p.SomeProperty

