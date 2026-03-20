package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/transaction"
	"mit.edu/dsg/godb/storage"
)

// UpdateExecutor implements the execution logic for updating tuples in a table.
// It iterates over the tuples provided by its child executor, which represent the full value of the current row
// and its RID. It uses the expressions defined in the plan to calculate the new values for every column in the new row.
// The executor updates the table heap in-place and ensures that all relevant indexes are updated
// if the key columns have changed. It produces a single tuple containing the count of updated rows.
type UpdateExecutor struct {
	plan *planner.UpdateNode
	child Executor
	tableHeap *TableHeap
	indexes []indexing.Index
	err error
	updatedRows int64
	done bool
	txn *transaction.TransactionContext
}

func NewUpdateExecutor(plan *planner.UpdateNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *UpdateExecutor {
	return &UpdateExecutor{plan: plan, child: child, tableHeap: tableHeap, indexes: indexes}
}

func (e *UpdateExecutor) PlanNode() planner.PlanNode {
	return e.plan

}

func (e *UpdateExecutor) Init(ctx *ExecutorContext) error {
	e.err = nil
	e.updatedRows = 0
	e.done = false
	if ctx != nil{
		e.txn = ctx.GetTransaction()
	}else{
		e.txn = nil
	}

	if err := e.child.Init(ctx); err != nil {
		e.err = err
		return err
	}
	return nil
}

func (e *UpdateExecutor) Next() bool {
	if e.done || e.err != nil {
		return false
	}

	desc := e.tableHeap.StorageSchema()

	for e.child.Next() {
		oldTup := e.child.Current()
		rid := oldTup.RID()

		// Build updated row in a physical buffer.
		row := make([]byte, desc.BytesPerTuple())
		for col := 0; col < len(e.plan.Expressions); col++ {
			val := e.plan.Expressions[col].Eval(oldTup)
			desc.SetValue(row, col, val)
		}
		newTup := storage.FromRawTuple(storage.RawTuple(row), desc, rid)

		// Update indexes if any key column changed.
		for _, idx := range e.indexes {
			md := idx.Metadata()

			// Collect old and new key values.
			changed := false
			oldVals := make([]common.Value, len(md.ProjectionList))
			newVals := make([]common.Value, len(md.ProjectionList))
			for i, colIdx := range md.ProjectionList {
				oldVals[i] = oldTup.GetValue(colIdx)
				newVals[i] = newTup.GetValue(colIdx)
				if oldVals[i].Compare(newVals[i]) != 0 {
					changed = true
				}
			}
			if !changed {
				continue
			}

			// Build old key.
			oldKeyTup := storage.FromValues(oldVals...)
			oldBuf := make([]byte, md.KeySchema.BytesPerTuple())
			oldKeyTup.WriteToBuffer(oldBuf, md.KeySchema)
			oldKey := md.AsKey(storage.RawTuple(oldBuf))

			// Build new key.
			newKeyTup := storage.FromValues(newVals...)
			newBuf := make([]byte, md.KeySchema.BytesPerTuple())
			newKeyTup.WriteToBuffer(newBuf, md.KeySchema)
			newKey := md.AsKey(storage.RawTuple(newBuf))

			if err := idx.DeleteEntry(oldKey, rid, e.txn); err != nil {
				e.err = err
				return false
			}
			if err := idx.InsertEntry(newKey, rid, e.txn); err != nil {
				e.err = err
				return false
			}
		}

		// Update the heap tuple in-place.
		if err := e.tableHeap.UpdateTuple(e.txn, rid, storage.RawTuple(row)); err != nil {
			e.err = err
			return false
		}

		e.updatedRows++
	}

	if childErr := e.child.Error(); childErr != nil {
		e.err = childErr
		return false
	}

	e.done = true
	return true
}
func (e *UpdateExecutor) OutputSchema() []common.Type {
	return e.plan.OutputSchema()
}

func (e *UpdateExecutor) Current() storage.Tuple {
	return storage.FromValues(common.NewIntValue((e.updatedRows)))
}

func (e *UpdateExecutor) Close() error {
	return e.child.Close()
}

func (e *UpdateExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	if err := e.child.Error(); err != nil {
		return err
	}
	return nil
}