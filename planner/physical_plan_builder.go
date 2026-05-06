package planner

import (
	"fmt"

	"mit.edu/dsg/godb/catalog"
	"mit.edu/dsg/godb/common"
)

type ExpressionBinder struct{}

func NewExpressionBinder() *ExpressionBinder {
	return &ExpressionBinder{}
}

// BindExpr converts LogicalColumn pointers into BoundValueExpr (calculating integer offsets)
// The bindExpr function in logical plan builder only builds LOGICAL expression trees.
func (b *ExpressionBinder) BindExpr(e Expr, logicalSchema LogicalSchema, physicalSchema []common.Type) Expr {
	switch v := e.(type) {
	case *LogicalColumn:
		for i, col := range logicalSchema {
			if col.Equals(v) {
				// We use the index 'i' to find the type in the physical schema
				return NewColumnValueExpression(i, physicalSchema, v.cname)
			}
		}
		panic(fmt.Sprintf("ExpressionBinder Failed: Column %s not found in input logicalSchema, %v", v.cname, logicalSchema))

	case *ComparisonExpression:
		return NewComparisonExpression(
			b.BindExpr(v.left, logicalSchema, physicalSchema),
			b.BindExpr(v.right, logicalSchema, physicalSchema),
			v.compType,
		)
	case *BinaryLogicExpression:
		return NewBinaryLogicExpression(
			b.BindExpr(v.left, logicalSchema, physicalSchema),
			b.BindExpr(v.right, logicalSchema, physicalSchema),
			v.logicType,
		)
	case *ArithmeticExpression:
		return NewArithmeticExpression(
			b.BindExpr(v.left, logicalSchema, physicalSchema),
			b.BindExpr(v.right, logicalSchema, physicalSchema),
			v.op,
		)
	case *NegationExpression:
		return NewNegationExpression(
			b.BindExpr(v.child, logicalSchema, physicalSchema),
		)
	case *NullCheckExpression:
		return NewNullCheckExpression(
			b.BindExpr(v.child, logicalSchema, physicalSchema),
			v.checkType,
		)
	case *StringConcatExpression:
		return NewStringConcatenation(
			b.BindExpr(v.left, logicalSchema, physicalSchema),
			b.BindExpr(v.right, logicalSchema, physicalSchema),
		)
	case *LikeExpression:
		return NewLikeExpression(
			b.BindExpr(v.left, logicalSchema, physicalSchema),
			b.BindExpr(v.right, logicalSchema, physicalSchema),
		)
	case *ConstantValueExpr:
		return v
	}
	return e
}

// BindExpr shifts all BoundValueExprs' integer offsets by a fixed constant
// additional offset.
func (b *ExpressionBinder) ShiftExpr(e Expr, shiftOffset int) Expr {
	switch v := e.(type) {
	case *BoundValueExpr:
		return &BoundValueExpr{
			fieldOffset: v.fieldOffset + shiftOffset,
			outputType:  v.outputType,
			name:        v.name,
		}

	case *ComparisonExpression:
		return NewComparisonExpression(
			b.ShiftExpr(v.left, shiftOffset),
			b.ShiftExpr(v.right, shiftOffset),
			v.compType,
		)
	case *BinaryLogicExpression:
		return NewBinaryLogicExpression(
			b.ShiftExpr(v.left, shiftOffset),
			b.ShiftExpr(v.right, shiftOffset),
			v.logicType,
		)
	case *ArithmeticExpression:
		return NewArithmeticExpression(
			b.ShiftExpr(v.left, shiftOffset),
			b.ShiftExpr(v.right, shiftOffset),
			v.op,
		)
	case *NegationExpression:
		return NewNegationExpression(
			b.ShiftExpr(v.child, shiftOffset),
		)
	case *NullCheckExpression:
		return NewNullCheckExpression(
			b.ShiftExpr(v.child, shiftOffset),
			v.checkType,
		)
	case *StringConcatExpression:
		return NewStringConcatenation(
			b.ShiftExpr(v.left, shiftOffset),
			b.ShiftExpr(v.right, shiftOffset),
		)
	case *LikeExpression:
		return NewLikeExpression(
			b.ShiftExpr(v.left, shiftOffset),
			b.ShiftExpr(v.right, shiftOffset),
		)
	case *ConstantValueExpr:
		return v
	}
	return e
}

/*
For now, FuseProjectionIntoExpr only handles the case where the child projection does not involve any expression
evaluation (i.e. it's just a projection of columns from its child). In this case, we can directly replace
references to the LogicalColumns in the parent projection with the corresponding expressions from the child
projection.

TODO: Supports more complex cases where the child projection involves expression evaluation (e.g. projecting a + b).
*/
func (b *ExpressionBinder) FuseProjectionIntoExpr(e Expr, childLogicalSchema LogicalSchema, childExpressions []Expr) (Expr, error) {
	switch v := e.(type) {
	case *LogicalColumn:
		for i, logicalCol := range childLogicalSchema {
			if v.Equals(logicalCol) {
				return childExpressions[i], nil
			}
		}
		return nil, fmt.Errorf("ExpressionBinder.FuseProjectionIntoExpr: Column %s not found in child expressions, cannot fuse projection", v.cname)

	case *ComparisonExpression:
		leftFused, err := b.FuseProjectionIntoExpr(v.left, childLogicalSchema, childExpressions)
		if err != nil {
			return nil, err
		}
		rightFused, err := b.FuseProjectionIntoExpr(v.right, childLogicalSchema, childExpressions)
		if err != nil {
			return nil, err
		}
		return NewComparisonExpression(
			leftFused,
			rightFused,
			v.compType,
		), nil
	case *BinaryLogicExpression:
		leftFused, err := b.FuseProjectionIntoExpr(v.left, childLogicalSchema, childExpressions)
		if err != nil {
			return nil, err
		}
		rightFused, err := b.FuseProjectionIntoExpr(v.right, childLogicalSchema, childExpressions)
		if err != nil {
			return nil, err
		}
		return NewBinaryLogicExpression(
			leftFused,
			rightFused,
			v.logicType,
		), nil
	case *ArithmeticExpression:
		leftFused, err := b.FuseProjectionIntoExpr(v.left, childLogicalSchema, childExpressions)
		if err != nil {
			return nil, err
		}
		rightFused, err := b.FuseProjectionIntoExpr(v.right, childLogicalSchema, childExpressions)
		if err != nil {
			return nil, err
		}
		return NewArithmeticExpression(
			leftFused,
			rightFused,
			v.op,
		), nil
	case *NegationExpression:
		childFused, err := b.FuseProjectionIntoExpr(v.child, childLogicalSchema, childExpressions)
		if err != nil {
			return nil, err
		}
		return NewNegationExpression(
			childFused,
		), nil
	case *NullCheckExpression:
		childFused, err := b.FuseProjectionIntoExpr(v.child, childLogicalSchema, childExpressions)
		if err != nil {
			return nil, err
		}
		return NewNullCheckExpression(
			childFused,
			v.checkType,
		), nil
	case *StringConcatExpression:
		leftFused, err := b.FuseProjectionIntoExpr(v.left, childLogicalSchema, childExpressions)
		if err != nil {
			return nil, err
		}
		rightFused, err := b.FuseProjectionIntoExpr(v.right, childLogicalSchema, childExpressions)
		if err != nil {
			return nil, err
		}
		return NewStringConcatenation(
			leftFused,
			rightFused,
		), nil
	case *LikeExpression:
		leftFused, err := b.FuseProjectionIntoExpr(v.left, childLogicalSchema, childExpressions)
		if err != nil {
			return nil, err
		}
		rightFused, err := b.FuseProjectionIntoExpr(v.right, childLogicalSchema, childExpressions)
		if err != nil {
			return nil, err
		}
		return NewLikeExpression(
			leftFused,
			rightFused,
		), nil
	case *ConstantValueExpr:
		return v, nil
	}
	return nil, fmt.Errorf("ExpressionBinder.FuseProjectionIntoExpr: Unsupported expression type %T in projection fusion", e)
}

type PhysicalPlanBuilder struct {
	catalog    *catalog.Catalog
	rules      []PhysicalConversionRule
	exprBinder *ExpressionBinder

	// Optional cost-based join path (join cluster built from FindBestJoin instead of rule priority).
	cboOpt       *JoinOptimizer
	cboBest      *Plan
	cboScans     []*LogicalScanNode
	cboOperands  []LogicalPlanNode // per table index: subtree including filters (lazily filled)
	operandsInit bool
}

func NewPhysicalPlanBuilder(catalog *catalog.Catalog, customRules []PhysicalConversionRule) *PhysicalPlanBuilder {
	return &PhysicalPlanBuilder{
		catalog:    catalog,
		rules:      customRules,
		exprBinder: NewExpressionBinder(),
	}
}

// WithCostBasedJoin attaches CBO output so Build uses FindBestJoin's join order and operators
// for the multi-table join cluster. Falls back to rules if the cluster cannot be matched.
func (b *PhysicalPlanBuilder) WithCostBasedJoin(opt *JoinOptimizer, best *Plan, scans []*LogicalScanNode) *PhysicalPlanBuilder {
	nb := *b
	nb.cboOpt = opt
	nb.cboBest = best
	nb.cboScans = scans
	nb.cboOperands = nil
	nb.operandsInit = false
	return &nb
}

func (b *PhysicalPlanBuilder) Build(logicalPlan LogicalPlanNode) (PlanNode, error) {
	if j, ok := logicalPlan.(*LogicalJoinNode); ok && b.isCBOJoinRoot(j) {
		b.ensureCBOOperands(j)
		return b.materializeCBOPlan()
	}

	logicalChildren := logicalPlan.Children()
	physicalChildren := make([]PlanNode, len(logicalChildren))

	for i, child := range logicalChildren {
		physChild, err := b.Build(child)
		if err != nil {
			return nil, err
		}
		physicalChildren[i] = physChild
	}

	var bestRule PhysicalConversionRule
	maxPriority := -1
	for _, rule := range b.rules {
		if rule.Match(logicalPlan, physicalChildren, b.catalog) && rule.Priority() > maxPriority {
			bestRule = rule
			maxPriority = rule.Priority()
		}
	}

	if bestRule == nil {
		return nil, fmt.Errorf("No physical conversion rule matched for node: %s", logicalPlan.String())
	}

	return bestRule.Apply(logicalPlan, physicalChildren, b.catalog, b.exprBinder)
}

func countJoinScans(n LogicalPlanNode) int {
	if n == nil {
		return 0
	}
	switch x := n.(type) {
	case *LogicalScanNode:
		return 1
	case *LogicalJoinNode:
		return countJoinScans(x.Left) + countJoinScans(x.Right)
	case *LogicalFilterNode:
		return countJoinScans(x.Child)
	case *LogicalProjectionNode:
		return countJoinScans(x.Child)
	case *LogicalAggregationNode:
		return countJoinScans(x.Child)
	case *LogicalSortNode:
		return countJoinScans(x.Child)
	case *LogicalLimitNode:
		return countJoinScans(x.Child)
	case *LogicalSubqueryNode:
		return countJoinScans(x.Child)
	default:
		sum := 0
		for _, c := range n.Children() {
			sum += countJoinScans(c)
		}
		return sum
	}
}

func containsScanPointer(n LogicalPlanNode, scan *LogicalScanNode) bool {
	if n == nil || scan == nil {
		return false
	}
	if ls, ok := n.(*LogicalScanNode); ok && ls == scan {
		return true
	}
	for _, c := range n.Children() {
		if containsScanPointer(c, scan) {
			return true
		}
	}
	return false
}

// singleTableOperandRoot returns the smallest subtree rooted under root that contains exactly
// one base-table scan (e.g. Filter(Scan)) so physical planning preserves pushed predicates.
func singleTableOperandRoot(root LogicalPlanNode, scan *LogicalScanNode) LogicalPlanNode {
	if root == nil {
		return nil
	}
	if countJoinScans(root) == 1 && containsScanPointer(root, scan) {
		return root
	}
	for _, c := range root.Children() {
		if r := singleTableOperandRoot(c, scan); r != nil {
			return r
		}
	}
	return nil
}

func (b *PhysicalPlanBuilder) ensureCBOOperands(joinRoot *LogicalJoinNode) {
	if b.operandsInit || len(b.cboScans) == 0 {
		return
	}
	b.cboOperands = make([]LogicalPlanNode, len(b.cboScans))
	for i, s := range b.cboScans {
		op := singleTableOperandRoot(joinRoot, s)
		if op == nil {
			op = s
		}
		b.cboOperands[i] = op
	}
	b.operandsInit = true
}

func (b *PhysicalPlanBuilder) isCBOJoinRoot(j *LogicalJoinNode) bool {
	if b.cboBest == nil || b.cboOpt == nil || len(b.cboScans) < 2 {
		return false
	}
	switch j.joinType {
	case Inner, Cross:
	default:
		return false
	}
	if countJoinScans(j) != len(b.cboScans) {
		return false
	}
	if countJoinScans(j.Left) != len(b.cboScans)-1 || countJoinScans(j.Right) != 1 {
		return false
	}
	return true
}

func (b *PhysicalPlanBuilder) logicalSubplanForPlan(p *Plan) LogicalPlanNode {
	if p.LeftChild == nil {
		return b.cboScans[p.RightTable]
	}
	left := b.logicalSubplanForPlan(p.LeftChild)
	right := b.cboScans[p.RightTable]
	preds := b.cboOpt.predicatesForJoin(p.LeftChild.Tables, p.RightTable)
	return NewLogicalJoinNode(left, right, preds, Inner)
}

func (b *PhysicalPlanBuilder) materializeCBOPlan() (PlanNode, error) {
	return b.physFromCBOPlan(b.cboBest)
}

func (b *PhysicalPlanBuilder) physFromCBOPlan(p *Plan) (PlanNode, error) {
	if p.LeftChild == nil {
		idx := p.RightTable
		if idx < 0 || idx >= len(b.cboOperands) {
			return nil, fmt.Errorf("CBO plan references unknown table index %d", idx)
		}
		return b.Build(b.cboOperands[idx])
	}

	leftPhys, err := b.physFromCBOPlan(p.LeftChild)
	if err != nil {
		return nil, err
	}
	rt := p.RightTable
	if rt < 0 || rt >= len(b.cboOperands) {
		return nil, fmt.Errorf("CBO plan references unknown table index %d", rt)
	}
	rightPhys, err := b.Build(b.cboOperands[rt])
	if err != nil {
		return nil, err
	}

	joinLog := NewLogicalJoinNode(
		b.logicalSubplanForPlan(p.LeftChild),
		b.cboScans[rt],
		b.cboOpt.predicatesForJoin(p.LeftChild.Tables, rt),
		Inner,
	)
	return b.applyPhysicalJoinByName(joinLog, []PlanNode{leftPhys, rightPhys}, p.PhysicalJoin)
}

func (b *PhysicalPlanBuilder) applyPhysicalJoinByName(join *LogicalJoinNode, children []PlanNode, physicalJoin string) (PlanNode, error) {
	for _, rule := range b.rules {
		if rule.Name() == physicalJoin {
			return rule.Apply(join, children, b.catalog, b.exprBinder)
		}
	}
	return nil, fmt.Errorf("no physical rule named %q for cost-based join choice", physicalJoin)
}
