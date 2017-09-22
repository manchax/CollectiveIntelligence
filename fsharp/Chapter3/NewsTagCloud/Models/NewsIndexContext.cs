using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations.Schema;

namespace NewsTagCloud.Models
{
    public class NewsIndexContext : DbContext
    {
        public DbSet<Word> Words { get; set; }
        public DbSet<Link> Links { get; set; }
        public DbSet<WordLink> WordsLinks { get; set; }
        public DbSet<WordLinkPositions> WordLinkPositions { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            base.OnConfiguring(optionsBuilder);
            optionsBuilder.UseSqlServer("Data Source=localhost;Initial Catalog=NewsIndex;Integrated Security=True;");

        }
    }

    public class Word
    {
        public int ID { get; set; }
        [Column("Word")]
        public string Content { get; set; }
    }

    public class Link
    {
        public int ID { get; set; }
        [Column("Link")]
        public string URL { get; set; }
    }

    public class WordLink
    {
        public int ID { get; set; }
        public int WordID { get; set; }
        public int LinkID { get; set; }
        public Word Word { get; set; }
        public Link Link { get; set; }
        public int Count { get; set; }
    }

    public class WordLinkPositions
    {
        public int ID { get; set; }
        public int WordLinkID { get; set; }
        public int Position { get; set; }
        public WordLink WordLink { get; set; }
    }
}
