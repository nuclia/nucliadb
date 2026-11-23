1. Find nodes by properties

MATCH (p:Person {email: $email})
RETURN p

A basic lookup, often the entry point for other queries.

2. Get a node’s neighborhood

MATCH (p:Person {name: $name})-[r]-(neighbor)
RETURN p, r, neighbor
LIMIT 100

Explore directly connected nodes and relationships, either in one direction or both

3. Traverse a bounded number of hops

MATCH path = (p:Person {name: $name})-[:WORKS_WITH|REPORTS_TO*1..3]->(other)
RETURN path
LIMIT 100

Useful for org structures, related entities and “connected through” questions. A practical system should bound hop count and result size.

4. Match a multi-patterh

MATCH (p:Person)-[:WORKS_WITH]->(c:Company)<-[:PUBLISHED_BY]-(d:Document)
WHERE p.name = $name
RETURN p, c, d

Combines multiple relationship patterns to answer questions that cannot be expressed as single-edge lookup.

5. Filter relationships or their evidence

MATCH (a:Entity)-[r:RELATED_TO]->(b:Entity)
WHERE r.confidence >= $minimum
RETURN a, r, b

In a KG, filters may apply to edge properties, as well as node properties.

6. Aggregate and rank

MATCH (c:Company)<-[:WORKS_FOR]-(p:Person)
RETURN c.name, count(p) AS employees
ORDER BY employees DESC
LIMIT 10

Counts, grouping and ranking support summaries and analytics.

7. Find paths or connectivity

MATCH path = shortestPath(
  (a:Person {name: $start})-[:KNOWS*..6]-(b:Person {name: $end})
)
RETURN path

Path queries answer whether and how two nodes are connected. Path length and execution limits are important here.