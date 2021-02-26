package ops

// Author: Charles Randolph
// Function:
//   Ops (short for "operations") provides
//   1. Complex graphing operations

import (

	// Standard packages
	"fmt"
	"errors"

	// Custom packages
	"graph"
)


/*
 *******************************************************************************
 *                              Type Definitions                               *
 *******************************************************************************
*/

// Token: Found within the adjacency matrix, represents an edge for a chain
type Token struct {
	Tag       int          // Sequence identifier
	Num       int          // Sequence edge number
	Color     string       // Color to use with edge
}

// Edge: Represents an edge when use is needed outside of a matrix context
type Edge struct {
	Base      int          // ID of the node at which the edge begins
	Dest      int          // ID of the node at which the edge points
	Token     *Token       // Token identifying the edge owner
}

/*
 *******************************************************************************
 *                         Public Function Definitions                         *
 *******************************************************************************
*/


// Returns true if a given edge is equal to another
func (e *Token) Equals (other *Token) bool {
	return (e.Tag == other.Tag) && (e.Num == other.Num)
}

// Returns the larger of two integer numbers (silly but no default)
func Max (a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Returns the number of nodes in a slice of chains
func NodeCount (chains []int) int {
	sum := 0
	for _, c := range chains {
		sum += c
	}
	return sum
}

// Returns a string describing a path
func Path2String (path []int) string {
	s := "{"
	for i, n := range path {
		s += fmt.Sprintf("%d", n)
		if i < (len(path)-1) {
			s += ","
		}
	}
	return s + "}"
}

// Returns a string describing an edge. To be used with graph.Map
func Show (x interface{}) string {
	es := x.([]*Token)
	if len(es) == 0 {
		return " _ "
	}
	s := fmt.Sprintf("(%d", es[0].Tag)
	for i := 1; i < len(es); i++ {
		s += fmt.Sprintf(",%d", es[i].Tag)
	}
	return s + ")"
}

// Returns a slice of tokens from the given index in the graph. Panics on error
func EdgesAt (row, col int, g *graph.Graph) []*Token {
	val, err := g.Get(row, col)
	if nil != err {
		panic(err)
	}
	if nil == val {
		return []*Token{}
	}
	return val.([]*Token)
}

// Returns the slice of all edges in the given graph. Panics on error
func AllEdges (g *graph.Graph) []Edge {
	if nil == g {
		panic(errors.New("Cannot operate on empty graph"))	
	}
	edges := []Edge{}
	for row := 0; row < g.Len(); row++ {
		for col := 0; col < g.Len(); col++ {
			tokens := EdgesAt(row, col, g)
			for _, t := range tokens {
				edges = append(edges, Edge{Base: row, Dest: col, Token: t})
			}
		}
	}

	return edges
}

// Returns all rows on which chains start, given ordered chain lengths
func StartingRows (chains []int) []int {
	starting_rows := []int{}
	for i, sum := 0, 0; i < len(chains); i++ {
		starting_rows = append(starting_rows, sum)
		sum += chains[i]
	}
	return starting_rows	
}

// Returns the starting row for a given chain, and the column at which
// the outgoing edge was found. If the returned column is -1, then the row is the
// only component of the chain

// Returns the first node of the chain as the row. If the chain has length > 1,
// then the column at which the token is found is also returned. Otherwise -1
func StartCoordForChain (chain_id int, chains []int, g *graph.Graph) (int, int) {
	row, col := -1, -1

	// Chains must all start somewhere in the original start-rows
	start_rows := StartingRows(chains)

	// Edge case: If the chain has length 1, it must begin at its starting row
	if chains[chain_id] == 1 {
		return start_rows[chain_id], col
	}

	// Search other rows in case it is there
	for _, r := range start_rows {
		if c, _ := ColumnForToken(chain_id, 0, r, g); c != -1 {
			row = r
			col = c
			break
		}
	}

	return row, col
}

// Returns the chain to which the given row belongs
func ChainForRow (row int, chains []int) int {
	i, sum := 0, 0
	for i = 0; i < len(chains); i++ {
		if (row >= sum) && (row < (sum + chains[i])) {
			break
		} else {
			sum += chains[i]
		}
	}
	return i	
}

// Returns true if an edge exists between (a,b) in the graph (g)
func EdgeBetween (a, b int, g *graph.Graph) bool {

	// Closure: Returns true if x has a directed edge to y
	has_outgoing_edge := func (x, y int, g *graph.Graph) bool {
		edges := EdgesAt(x,y,g)
		return (len(edges) > 0)
	}

	return (has_outgoing_edge(a, b, g) || has_outgoing_edge(b, a, g))	
}

// Returns true if the given row has no incoming or outgoing edges
func Disconnected (row int, g *graph.Graph) bool {

	// Check row (outgoing)
	for i := 0; i < g.Len(); i++ {
		edges := EdgesAt(row, i, g)
		if (len(edges) > 0) {
			return false
		}
	}

	// Check column (incoming)
	for i := 0; i < g.Len(); i++ {
		edges := EdgesAt(i, row, g)
		if (len(edges) > 0) {
			return false
		}
	}

	return true	
}

// Returns (column, *Token) at which a given token is found along a row. Else (-1, nil)
// If the num parameter is given as -1, then the first matching token with the tag is returned
func ColumnForToken (tag, num, row int, g *graph.Graph) (int, *Token) {
	var edge *Token = nil
	var col int    = -1

	for i := 0; i < g.Len(); i++ {
		edges := EdgesAt(row, i, g)
		if len(edges) == 0 {
			continue
		}
		for _, e := range edges {
			if e.Tag == tag && (e.Num == num || num == -1) {
				edge = e
				col = i
				break
			}
		}
	}
	return col, edge	
}

// Repairs the tags of all paths in a graph (assuming no loops), and returns the path
func RepairPathTags (chains []int, g *graph.Graph) (error, [][]int) {
	paths := make([][]int, len(chains))

	// For each chain, construct the path (not expecting the numbers to be correct)
	for chain_id := 0; chain_id < len(chains); chain_id++ {
		// Locate the column at which the next token is found (only one)
		row, col := StartCoordForChain(chain_id, chains, g)
		for {
			paths[chain_id] = append(paths[chain_id], row)
			if col == -1 {
				break
			}
			row = col
			col, _ = ColumnForToken(chain_id, -1, row, g)
		}
	}

	// TODO: Technically tokens don't need a number. 
	// Can be derived from path, given no loops

	// For each path, update the sequence numbers
	for chain_id := 0; chain_id < len(chains); chain_id++ {
		for i := 0; i < len(paths[chain_id]) - 1; i++ {
			row, col := paths[chain_id][i], paths[chain_id][i+1]
			tokens := EdgesAt(row, col, g)
			for _, t := range tokens {
				if t.Tag == chain_id {
					t.Num = i
					break
				}
			}
			g.Set(row, col, tokens)
		}
	}
	return nil, paths
}

// Returns a sequence of visited nodes (rows) for a given chain
func PathForChain (chain_id int, chains []int, g *graph.Graph) []int {
	row, col, path := -1, -1, []int{}

	// Chains always start in start-rows. Obtain all starting rows
	row, col = StartCoordForChain(chain_id, chains, g)

	// Loop until end of chain is detected
	for {
		path = append(path, row)
		if col == -1 {
			break
		} else {
			row = col
			col, _ = ColumnForToken(chain_id, len(path), row, g)
		}
	}

	return path
}

// Returns true if the given path contains the given node
func PathContains (path []int, x int) bool {
	for _, y := range path {
		if y == x {
			return true
		}
	}
	return false
}

// Collects all tokens along a column
func TokensForColumn (col int, g *graph.Graph) []*Token {
	tokens := []*Token{}

	for row := 0; row < g.Len(); row++ {
		tokens = append(tokens, EdgesAt(row, col, g)...)
	}

	return tokens
}

// Returns true if a cycle exists in a dependency list
func IsCycle (offset, origin, visit_index int, visited []int, slices [][]int) bool {
	slice := slices[visit_index - offset]

	// Search cycle detection (don't visit twice)
	for _, v := range visited {
		if visit_index == v {
			return false
		}
	}

	// Search elements along the slice
	for _, item := range slice {

		// If the origin is discovered, then return true
		if item == origin {
			return true
		}

		// Otherwise search that visit index
		if IsCycle(offset, origin, item, append(visited, visit_index), slices) {
			return true
		}
	}

	return false
}

// Searches back through a chain until the given condition is satisified
// or no more options exist. 
func Backtrace (tag, col int, cond func(int)bool, g *graph.Graph) []int {

	// Visited list
	visited := []int{}

	// Search thw rows of the column for incoming edges
	for row := 0; row < g.Len(); row++ {
		tokens := EdgesAt(row, col, g)

		// For each incoming line, take action if it belongs to the same tag
		for _, t := range tokens {

			if t.Tag == tag {
				// If the condition is satisified, add the row. Otherwise, 
				// recursively search farther
				if cond(row) {
					visited = append(visited, row)
				} else {
					visited = append(visited, Backtrace(tag, row, cond, g)...)
				}
			}
		}
	}

	return visited
}

// Rewires an edge to the given destination (assumes exists)
func RewireTo (edge Edge, new_dest int, g *graph.Graph) error {
	var found bool = false

	// Remove existing edge at old position
	tokens := EdgesAt(edge.Base, edge.Dest, g)
	for i, t := range tokens {
		if t.Equals(edge.Token) {
			found = true
			g.Set(edge.Base, edge.Dest, append(tokens[:i], tokens[i+1:]...))
			break
		}
	}

	// Verify edge was found and removed
	if !found {
		reason := fmt.Sprintf("Couldn't find edge (%d,%d) at (%d,%d)!",
			edge.Token.Tag, edge.Token.Num, edge.Base, edge.Dest)
		return errors.New(reason)
	}

	// Insert edge at destination column
	tokens = EdgesAt(edge.Base, new_dest, g)
	tokens = append(tokens, edge.Token)
	return g.Set(edge.Base, new_dest, tokens)
}

// Extends given NxN graph to (N+1)x(N+1) graph, and returns new length
func ExtendGraphByOne (g *graph.Graph) int {
	n := g.Len()
	var expanded_graph graph.Graph = make([][]interface{}, n+1)
	for i := 0; i < (n+1); i++ {
		expanded_graph[i] = make([]interface{}, n+1)
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			expanded_graph[i][j] = (*g)[i][j]
		}
	}
	(*g) = expanded_graph

	return (n+1)	
}

// Adds an edge to the given graph
func Wire (from, to, tag, num int, color string, g *graph.Graph) error {
	edges := EdgesAt(from, to, g)
	edges = append(edges, &Token{Tag: tag, Num: num, Color: color})
	return g.Set(from, to, edges)
}

// Merges row 'from' into row 'to' in graph 'g'
func Merge (from, to int, g *graph.Graph) error {
	var err error = nil

	// All contents in row 'from' should be moved to row 'to'
	for i := 0; i < g.Len(); i++ {
		src_edges, dst_edges := EdgesAt(from, i, g), EdgesAt(to, i, g)
		if len(src_edges) == 0 {
			continue
		}
		err = g.Set(to, i, append(src_edges, dst_edges...))
		if nil != err {
			return err
		}
		err = g.Set(from, i, nil)
		if nil != err {
			return err
		}
	}

	// All contents in column 'from' should be moved to column 'to'
	for i := 0; i < g.Len(); i++ {
		src_edges, dst_edges := EdgesAt(i, from, g), EdgesAt(i, to, g)
		if len(src_edges) == 0 {
			continue
		}
		err = g.Set(i, to, append(src_edges, dst_edges...))
		if nil != err {
			return err
		}
		err = g.Set(i, from, nil)
		if nil != err {
			return err
		}
	}

	return nil
}

// Places sync node between two edges. Returns nil on success
func Sync (a, b Edge, g *graph.Graph) error {
	var err error = nil

	// Closure: Returns true if specific edge exists
	existsEdge := func (x Edge) bool {
		ys := EdgesAt(x.Base, x.Dest, g)
		if len(ys) == 0 {
			return false
		}
		for _, token := range ys {
			if token.Equals(x.Token) {
				return true
			}
		}
		return false
	}

	// Return if edges do not exist
	if !(existsEdge(a) && existsEdge(b)) {
		reason := fmt.Sprintf("Edges (%d--[%d]-->%d), or (%d--[%d]-->%d) doesn't exist\n",
			a.Base, a.Token.Tag, a.Dest, b.Base, b.Token.Tag, b.Dest)
		return errors.New("Cannot sync non-existant edges: " + reason)
	}

	// Extend the graph by adding the sync node
	sync_node_id := ExtendGraphByOne(g) - 1

	// Rewire edges to point to sync node
	if err = RewireTo(a, sync_node_id, g); nil != err {
		return err
	}
	if err = RewireTo(b, sync_node_id, g); nil != err {
		return errors.New("Unable to re-wire n")
	}

	// Add edges back from the sync nodes
	if err = Wire(sync_node_id, a.Dest, a.Token.Tag, a.Token.Num + 1,
		a.Token.Color, g); nil != err {
		return errors.New("Unable to wire new edge: " + err.Error())
	}
	if err = Wire(sync_node_id, b.Dest, b.Token.Tag, b.Token.Num + 1, 
		b.Token.Color, g); nil != err {
		return errors.New("Unable to wire new edge: " + err.Error())
	}

	return err
}

// Returns a new graph sized for given chains. With chain edge colors
func InitGraph (chains []int, chain_colors []string) *graph.Graph {

	// Compute number of rows required
	n := NodeCount(chains)

	// Initialize graph
	var g graph.Graph = make([][]interface{}, n)

	// Setup columns and rows
	for row := 0; row < n; row++ {
		g[row] = make([]interface{}, n)
		for col := 0; col < n; col++ {
			g[row][col] = []*Token{}
		}
	}

	// Setup all chains
	for i, offset := 0,0; i < len(chains); i++ {
		for j := 0; j < (chains[i] - 1); j++ {
			edge := Token{Tag: i, Num: j, Color: chain_colors[i]}
			g.Set(offset+j, offset+j+1, []*Token{&edge})
		}
		offset += chains[i]
	}

	return &g
}

// Returns a deep clone of a graph
func CloneGraph (g *graph.Graph) *graph.Graph {
	var clone graph.Graph = make([][]interface{}, g.Len())

	// Setup columns and rows
	for row := 0; row < g.Len(); row++ {
		clone[row] = make([]interface{}, g.Len())
		for col := 0; col < g.Len(); col++ {
			var copies []*Token = nil
			if nil != (*g)[row][col] {
				var items []*Token = ((*g)[row][col]).([]*Token)
				copies = []*Token{}
				for i := 0; i < len(items); i++ {
					copies = append(copies, 
						&Token{Tag: items[i].Tag, 
					           Num: items[i].Num,
					           Color: items[i].Color})
				}
			}
			clone[row][col] = copies
		}
	}

	return &clone
}
