// This file is generated by vetgen.
// Do NOT modify this file.
//
// You can run this tool with go vet such as:
//
//	go vet -vettool=$(which myvet) pkgname
package main

import (
	"github.com/gostaticanalysis/vetgen/analyzers"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/unitchecker"

	"github.com/gostaticanalysis/wraperrfmt" // add by vetgen
)

var myAnayzers = []*analysis.Analyzer{
	wraperrfmt.Analyzer,
}

func main() {
	unitchecker.Main(append(
		analyzers.Recommend(),
		myAnayzers...,
	)...)
}
