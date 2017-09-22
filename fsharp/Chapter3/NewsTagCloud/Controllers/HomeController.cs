using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using NewsTagCloud.Models;
using Sparc.TagCloud;
using Microsoft.EntityFrameworkCore;

namespace NewsTagCloud.Controllers
{
    public class HomeController : Controller
    {
        public IActionResult Index()
        {
            using ( var db = new NewsIndexContext() )
            {
                //var words = from wl in db.WordsLinks
                //            join w in db.Words on wl.WordID equals w.ID
                //            group w by w.Content into g
                //            let count = g.Count()
                //            where count > 2
                //            orderby count descending
                //            select new KeyValuePair<string, int>(g.Key, count);
                var dict = new Dictionary<string, int>(100);
                using ( var cmd = db.Database.GetDbConnection().CreateCommand() )
                {
                    cmd.CommandText = @"select top 100 sum([Count]) as Count, Word
from WordsLinks wl
join Words w on w.ID = wl.WordID
group by Word
order by 1 desc";
                    cmd.Connection.Open();
                    using ( var reader = cmd.ExecuteReader() )
                    {
                        while ( reader.Read() )
                        {
                            dict.Add(reader.GetString(1), reader.GetInt32(0));
                        }
                        reader.Close();
                    }
                    cmd.Connection.Close();
                }
                db.Database.CloseConnection();
                var cloudAnalyzer = new TagCloudAnalyzer();
                return View(
                    cloudAnalyzer.ComputeTagCloud(dict).Shuffle()
                );
            }
        }

        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}
