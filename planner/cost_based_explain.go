package planner

import (
	"fmt"
	"math"
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

	b.WriteString("Summary:\n")
	b.WriteString(fmt.Sprintf("  total_cost=%.2f\n", p.Cost))
	b.WriteString(fmt.Sprintf("  output_rows=%.2f\n", p.OutputRows))
	b.WriteString(fmt.Sprintf("  joins=%d\n\n", p.JoinCount))

	b.WriteString("Plan tree:\n")
	p.explainTree(&b, "", true)

	b.WriteString("\nJoin decisions:\n")
	p.explainJoinDecisions(&b, 1)

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
		"%s%s%s  add=T%d  rows=%.2f  cumulative_cost=%.2f\n",
		prefix,
		connector,
		p.PhysicalJoin,
		p.RightTable,
		p.OutputRows,
		p.Cost,
	))

	p.LeftChild.explainTree(b, nextPrefix, true)
}

func (p *Plan) explainJoinDecisions(b *strings.Builder, step int) int {
	if p == nil || p.LeftChild == nil {
		return step
	}

	step = p.LeftChild.explainJoinDecisions(b, step)

	b.WriteString(fmt.Sprintf(
		"  Step %d: join %s with T%d\n",
		step,
		p.LeftChild.String(),
		p.RightTable,
	))
	b.WriteString(fmt.Sprintf("    chosen: %s\n", p.PhysicalJoin))
	b.WriteString("    candidates:\n")

	for _, c := range p.Candidates {
		if !c.Applicable || math.IsInf(c.Cost, 1) {
			b.WriteString(fmt.Sprintf("      - %-22s not applicable\n", c.PhysicalJoin))
			continue
		}

		marker := " "
		if c.PhysicalJoin == p.PhysicalJoin {
			marker = "*"
		}

		b.WriteString(fmt.Sprintf(
			"      %s %-22s cost=%10.2f output_rows=%10.2f\n",
			marker,
			c.PhysicalJoin,
			c.Cost,
			c.OutputRows,
		))
	}

	b.WriteString("\n")
	return step + 1
}

func ExplainBestJoin(opt *JoinOptimizer) string {
	if opt == nil {
		return "No optimizer provided.\n"
	}
	best := opt.FindBestJoin()
	return best.Explain()
}
