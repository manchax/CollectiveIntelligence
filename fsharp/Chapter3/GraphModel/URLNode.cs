using System;
using System.Collections.Generic;
using Neo4jClient;

namespace GraphModel
{
	public class URLNode: NodeReference<URLNode>
	{
		public URLNode (string url): base(url.GetHashCode())
		{
			this.URL = url;
		}

		public string URL { get; set; }
		public List<URLNode> OutLinks { get; set; }
	}

	public class LinkTo: Relationship,
	IRelationshipAllowingSourceNode<URLNode>,
	IRelationshipAllowingTargetNode<URLNode>
	{
		public LinkTo(NodeReference target) : base(target) {
		}

		public override string RelationshipTypeKey {
			get { return "LinksTo"; }
		}
	}
}

