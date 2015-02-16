using System;
using Neo4jClient;

namespace GraphModel
{
	public class WordInURL: Relationship,
		IRelationshipAllowingSourceNode<WordNode>,
		IRelationshipAllowingTargetNode<URLNode>
	{
		public WordInURL (NodeReference targetNode): base(targetNode)
		{
		}

		public override string RelationshipTypeKey {
			get { return "FoundIn"; }
		}
	}
}

