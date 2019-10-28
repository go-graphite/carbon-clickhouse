package uploader

/*
Blacklisted patterns are grouped by length of their paths.
Patterns in single group are represented by tree-like structure.
Each pattern is stored in both forward and backward forms.

Let's say we have following patterns' list here:
["a.b.c.d", "a.b.c.d.e", "a.b.d.c", "a.b.c.d.f", "k.b.*.f"]
So there are 2 groups: length = 4 and length = 5.

length-4-group:
(root):
	a:
		b:
			c:
				d
			d:
				c
	k:
		b:
			*:
				f
(and the same for these patterns' right-to-left forms)

length-5-group:
(root):
	a:
		b:
			c:
				d:
					e
					f
(and the same for these patterns' right-to-left forms)
*/

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

type Blacklist struct {
	patterns  []string
	searchL2R patternTreeGroup
	searchR2L patternTreeGroup
}

type patternTreeNode map[string]patternTreeNode
type patternTreeGroup map[int]patternTreeNode

type patternPath []string
type patternPathList []patternPath

func NewBlacklist(ignoredPatterns []string) *Blacklist {
	patternPathGrouped := make(map[int]patternPathList)
	result := &Blacklist{
		patterns: ignoredPatterns,
	}

	// grouping split patterns by their paths' length
	for _, pattern := range ignoredPatterns {
		path := strings.Split(pattern, ".")
		pathLength := len(path)

		patternPathGrouped[pathLength] = append(patternPathGrouped[pathLength], path)
	}

	result.searchL2R = make(patternTreeGroup, len(patternPathGrouped))
	result.searchR2L = make(patternTreeGroup, len(patternPathGrouped))

	// trees themselves
	for pathLength, pathList := range patternPathGrouped {
		treeL2R, treeR2L := buildTrees(pathList, pathLength)
		result.searchL2R[pathLength] = treeL2R
		result.searchR2L[pathLength] = treeR2L
	}

	return result
}

// Contains method determines whether or not this value is blacklisted
func (blacklist *Blacklist) Contains(value string, isReverse bool) bool {
	var (
		treeGroup patternTreeGroup
	)

	pattern := strings.Split(value, ".")
	if isReverse {
		treeGroup = blacklist.searchR2L
	} else {
		treeGroup = blacklist.searchL2R
	}

	if searchTree, exists := treeGroup[len(pattern)]; exists {
		return blacklist.containsInner(searchTree, pattern, 0)
	} else {
		return false
	}
}

// containsInner is recursive part of Contains method
func (blacklist *Blacklist) containsInner(searchTree patternTreeNode, pattern []string, start int) bool {
	if start == len(pattern) {
		return true
	}

	// searching for both exact and wildcard match
	for _, patternPart := range []string{pattern[start], "*"} {
		if treeNode, exists := searchTree[patternPart]; exists {
			if blacklist.containsInner(treeNode, pattern, start+1) {
				return true
			}
		}
	}

	return false
}

// buildTrees constructs 2 trees (for l2r and r2l search) for list of patterns of the same length
func buildTrees(pathList patternPathList, pathLength int) (patternTreeNode, patternTreeNode) {
	pathsQty := len(pathList)
	rootNodeL2R := make(patternTreeNode, pathsQty)
	rootNodeR2L := make(patternTreeNode, pathsQty)

	for _, path := range pathList {
		// tree node iterators
		currentTreeNodeL2R := rootNodeL2R
		currentTreeNodeR2L := rootNodeR2L

		for i := range path {
			partL2R := path[i]
			if currentTreeNodeL2R[partL2R] == nil {
				currentTreeNodeL2R[partL2R] = make(patternTreeNode, pathsQty)
			}
			currentTreeNodeL2R = currentTreeNodeL2R[partL2R]

			partR2L := path[pathLength-1-i]
			if currentTreeNodeR2L[partR2L] == nil {
				currentTreeNodeR2L[partR2L] = make(patternTreeNode, pathsQty)
			}
			currentTreeNodeR2L = currentTreeNodeR2L[partR2L]
		}

	}

	return rootNodeL2R, rootNodeR2L
}

// String is string representation of the tree (for debug purposes only)
func (root patternTreeNode) String() string {
	return root.stringInner(0)
}

// stringInner is recursive part of String method
func (root patternTreeNode) stringInner(depth int) string {
	patternList := make([]string, 0, len(root))
	result := bytes.Buffer{}

	for pattern := range root {
		patternList = append(patternList, pattern)
	}
	sort.Strings(patternList)

	for _, pattern := range patternList {
		result.WriteString(strings.Repeat("\t", depth))
		result.WriteString(fmt.Sprintf("%s\n", pattern))
		result.WriteString(root[pattern].stringInner(depth + 1))
	}

	return result.String()
}
