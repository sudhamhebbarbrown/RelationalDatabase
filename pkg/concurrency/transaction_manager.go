package concurrency

import (
	"errors"
	"sync"

	"dinodb/pkg/database"

	"github.com/google/uuid"
)

// Transaction Manager manages all of the transactions on a server.
// Every client runs 1 transaction at a time, so uuid (clientID) can be used to uniquely identify a Transaction.
// Resources are like Entries that can be uniquely identified across tables
type TransactionManager struct {
	resourceLockManager *ResourceLockManager       // Maps every resource to it's corresponding mutex
	waitsForGraph       *WaitsForGraph             // Identifies deadlocks through cycle detection
	transactions        map[uuid.UUID]*Transaction // Identifies the Transaction for a particular client
	mtx                 sync.RWMutex
}

func NewTransactionManager(lm *ResourceLockManager) *TransactionManager {
	return &TransactionManager{
		resourceLockManager: lm,
		waitsForGraph:       NewGraph(),
		transactions:        make(map[uuid.UUID]*Transaction),
	}
}

func (tm *TransactionManager) GetResourceLockManager() (lm *ResourceLockManager) {
	return tm.resourceLockManager
}

func (tm *TransactionManager) GetTransactions() (txs map[uuid.UUID]*Transaction) {
	return tm.transactions
}

// Get a particular transaction of a client.
func (tm *TransactionManager) GetTransaction(clientId uuid.UUID) (tx *Transaction, found bool) {
	tm.mtx.RLock()
	defer tm.mtx.RUnlock()
	tx, found = tm.transactions[clientId]
	return tx, found
}

// Begin a transaction for the given client; error if already began.
func (tm *TransactionManager) Begin(clientId uuid.UUID) error {
	tm.mtx.Lock()
	defer tm.mtx.Unlock()
	_, found := tm.transactions[clientId]
	if found {
		return errors.New("transaction already began")
	}
	tm.transactions[clientId] = &Transaction{clientId: clientId, lockedResources: make(map[Resource]LockType)}
	return nil
}

// Locks the requested resource. Will return an error if deadlock is created by locking.
// 1) Get the transaction we want, and construct the resource.
// 2) Check if we already have rights to the resource
//   - Error if upgrading from read to write locks within this transaction.
//   - Ignore requests for a duplicate lock
//
// 4) Check for deadlocks using waitsForGraph
// 5) Lock resource's mutex
// 6) Add resource to the transaction's resources
// Hint: conflictingTransactions(), GetTransaction()
	
func (tm *TransactionManager) Lock(clientId uuid.UUID, table database.Index, resourceKey int64, lType LockType) error {

	transaction, status := tm.GetTransaction(clientId)
	if !status {
		return errors.New("no such transaction")
	}
	newResource := Resource{tableName: table.GetName(), key: resourceKey}
	possibleConflicts := tm.conflictingTransactions(newResource, lType)
	transaction.RLock()

	curr, locked := transaction.GetResources()[newResource]
	if locked {
		transaction.RUnlock()
		if curr == R_LOCK && lType == W_LOCK {
			return errors.New("tm.lock: cannot upgrade lock")
		}
		return nil
	}

	for _, t := range possibleConflicts {
		tm.waitsForGraph.AddEdge(transaction, t)
		defer tm.waitsForGraph.RemoveEdge(transaction, t)
	}
	if tm.waitsForGraph.DetectCycle() {
		return errors.New("tm.lock: deadlock detected")
	}
	transaction.RUnlock()
	if err := tm.resourceLockManager.Lock(newResource, lType); err != nil {
		return err
	}
	transaction.WLock()
	defer transaction.WUnlock()
	// Set the lock in transaction.lockedResources
	transaction.GetResources()[newResource] = lType

	return nil
}

// Unlocks the requested resource.
// 1) Get the transaction we want, and construct the resource.
// 2) Remove resource from the transaction's currently locked resources if it is valid.
// 3) Unlock resource's mutex
func (tm *TransactionManager) Unlock(clientId uuid.UUID, table database.Index, resourceKey int64, lType LockType) error {
	transaction, status := tm.GetTransaction(clientId)
	if(!status) {
		return errors.New("no such transaction")
	}
	transaction.WLock()
	defer transaction.WUnlock()
	resouceToDel := Resource{tableName: table.GetName(), key: resourceKey}
	lock, status := transaction.lockedResources[resouceToDel]
	if(!status || lock != lType) {
		return errors.New("tm.unlock: invalid unlock request")
	}
	delete(transaction.lockedResources, resouceToDel)
	err := tm.resourceLockManager.Unlock(resouceToDel, lType)
	if err != nil {
		return err
	}
	return nil
}

// Commits the given transaction and removes it from the running transactions list.
func (tm *TransactionManager) Commit(clientId uuid.UUID) error {
	tm.mtx.Lock()
	defer tm.mtx.Unlock()
	// Get the transaction we want.
	t, found := tm.transactions[clientId]
	if !found {
		return errors.New("no transactions running")
	}
	// Unlock all resources.
	t.RLock()
	defer t.RUnlock()
	for r, lType := range t.lockedResources {
		err := tm.resourceLockManager.Unlock(r, lType)
		if err != nil {
			return err
		}
	}
	// Remove the transaction from our transactions list.
	delete(tm.transactions, clientId)
	return nil
}

// Returns a slice of all transactions that conflict w/ the given resource and locktype.
func (tm *TransactionManager) conflictingTransactions(r Resource, lType LockType) []*Transaction {
	txs := make([]*Transaction, 0)
	for _, t := range tm.transactions {
		t.RLock()
		for storedResource, storedType := range t.lockedResources {
			if storedResource == r && (storedType == W_LOCK || lType == W_LOCK) {
				txs = append(txs, t)
				break
			}
		}
		t.RUnlock()
	}
	return txs
}
