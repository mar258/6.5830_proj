package planner

import (
	"fmt"
	"strings"
)

func (p *Plan) String() string {
	if p == nil {
		return "<nil>"
	}
	if p.LeftChild == nil {
		return fmt.Sprintf("T%d", p.RightTable)
	}
	return fmt.Sprintf("(%s %s T%d)", p.LeftChild.String(), p.PhysicalJoin, p.RightTable)
}

func (p *Plan) Explain() string {
	if p == nil {
		return "No valid join plan found.\n"
	}

	var b strings.Builder

	b.WriteString("========================================\n")
	b.WriteString(" Cost-Based Join Optimizer Explain\n")
	b.WriteString("========================================\n\n")

	b.WriteString("Best left-deep plan:\n")
	b.WriteString(fmt.Sprintf("  %s\n\n", p.String()))

	b.WriteString("Estimated total cost:\n")
	b.WriteString(fmt.Sprintf("  %.2f\n\n", p.Cost))

	b.WriteString("Estimated output rows:\n")
	b.WriteString(fmt.Sprintf("  %.2f\n\n", p.OutputRows))

	b.WriteString("Plan tree:\n")
	p.explainTree(&b, "", true)

	return b.String()
}

func (p *Plan) explainTree(b *strings.Builder, prefix string, isLast bool) {
	if p == nil {
		return
	}

	connector := "├── "
	nextPrefix := prefix + "│   "
	if isLast {
		connector = "└── "
		nextPrefix = prefix + "    "
	}

	if p.LeftChild == nil {
		b.WriteString(fmt.Sprintf(
			"%s%sSeqScan T%d  rows=%.2f  cost=%.2f\n",
			prefix,
			connector,
			p.RightTable,
			p.OutputRows,
			p.Cost,
		))
		return
	}

	b.WriteString(fmt.Sprintf(
		"%s%s%s  add=T%d  rows=%.2f  cost=%.2f\n",
		prefix,
		connector,
		p.PhysicalJoin,
		p.RightTable,
		p.OutputRows,
		p.Cost,
	))

	p.LeftChild.explainTree(b, nextPrefix, true)
}

func ExplainBestJoin(opt *JoinOptimizer) string {
	if opt == nil {
		return "No optimizer provided.\n"
	}
	best := opt.FindBestJoin()
	return best.Explain()
}
