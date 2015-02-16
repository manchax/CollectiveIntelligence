using System;
using System.Collections.Generic;
using Neo4jClient;

namespace GraphModel
{
	public class WordPosition
	{
		public int Position { get; set; }
		public WordPosition(int p) {
			this.Position = p; 
		}
	}

	public class WordNode
	{
		public WordNode (string s, IEnumerable<WordPosition> p)
		{
			this.Word = s;
			this.Positions = p;
		}

		public string Word { get; set; }
		public IEnumerable<WordPosition> Positions { get; set; }

		public override int GetHashCode ()
		{
			return Word.GetHashCode ();
		}
	}
}

