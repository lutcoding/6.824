package main

import "fmt"

type gid struct {
	gids     []int
	shardNum map[int]int
}

func (g gid) Less(i, j int) bool {
	a, b := g.gids[i], g.gids[j]
	if g.shardNum[a] != g.shardNum[b] {
		return g.shardNum[a] > g.shardNum[b]
	} else {
		return a < b
	}
}
func (g gid) Len() int {
	return len(g.gids)
}
func (g gid) Swap(i, j int) {
	tmp := g.gids[i]
	g.gids[i] = g.gids[j]
	g.gids[j] = tmp
}

func main() {
	a := [2]bool{}
	fmt.Printf("%v", a[0])
}
