package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        if (readonly && (lockType.equals(LockType.X) || lockType.equals(LockType.IX))) {
            throw new UnsupportedOperationException("The context is read only!");
        }
        if (hasSIXAncestor(transaction) && (lockType.equals(LockType.S) || lockType.equals(LockType.IS))) {
            throw new InvalidLockException("The request is invalid!");
        }
        LockType lkType= lockman.getLockType(transaction, name);
        if (!lkType.equals(LockType.NL)) {
            throw new DuplicateLockRequestException("There is already a lock on transaction!");
        } else {
            LockType parentLockType = getParentLockType(transaction);
            if (!parentLockType.equals(LockType.NL) && !LockType.canBeParentLock(parentLockType, lockType)) {
                throw new InvalidLockException("The request is invalid!");
            }
        }
        lockman.acquire(transaction, this.name, lockType);
        addNumChildLocks(transaction, parent);
    }

    public LockType getParentLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        LockContext travel = parent;
        while (travel != null) {
            if (!travel.getExplicitLockType(transaction).equals(LockType.NL)) {
                return travel.getExplicitLockType(transaction);
            } else {
                travel = parent.parent;
            }
        }
        return LockType.NL;
    }

    public void addNumChildLocks(TransactionContext transaction, LockContext parent) {
        if (parent == null) return;
        int lockNum = parent.numChildLocks.getOrDefault(transaction.getTransNum(), 0);
        parent.numChildLocks.put(transaction.getTransNum(), lockNum + 1);
        addNumChildLocks(transaction, parent.parent);
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly) throw new UnsupportedOperationException("The context is read only!");
        if (lockman.getLockType(transaction, name).equals(LockType.NL)) {
            throw new NoLockHeldException("There is no lock held on the transaction!");
        }
        if (numChildLocks.getOrDefault(transaction.getTransNum(), 0) != 0) {
            List<Lock> locks = lockman.getLocks(transaction);
            throw new InvalidLockException("Releasing the lock violates multigranularity locking constraints!");
        }
        lockman.release(transaction, name);
        deleteNumChildLocks(transaction, parent);
    }

    public void deleteNumChildLocks(TransactionContext transaction, LockContext parent) {
        if (parent == null) return;
        int lockNum = parent.numChildLocks.getOrDefault(transaction.getTransNum(), 0);
        if (lockNum == 0) return;
        parent.numChildLocks.put(transaction.getTransNum(), lockNum - 1);
        deleteNumChildLocks(transaction, parent.parent);
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly) throw new UnsupportedOperationException("The context is read only!");
        LockType oldLockType = lockman.getLockType(transaction, name);
        if (oldLockType.equals(newLockType)) {
            throw new DuplicateLockRequestException("Transaction already has a `newLockType` Lock!");
        } else if (oldLockType.equals(LockType.NL)) {
            throw new NoLockHeldException("There is no lock held on the transaction!");
        }
        if (!newLockType.equals(LockType.SIX)) {
            if (LockType.substitutable(newLockType, oldLockType)) {
                lockman.promote(transaction, name, newLockType);
            } else {
                throw new InvalidLockException("The requested lock type is not valid!");
            }
        } else {
            if (!LockType.substitutable(newLockType, oldLockType) || hasSIXAncestor(transaction)) {
                throw new InvalidLockException("The requested lock type is not valid!");
            } else {
                List<ResourceName> descendants = sisDescendants(transaction);
                List<LockContext> contexts = new ArrayList<>();
                descendants.add(getResourceName());
                for (ResourceName descendant: descendants) {
                    contexts.add(fromResourceName(lockman, descendant));
                }
                lockman.acquireAndRelease(transaction, name, newLockType, descendants);
                for (LockContext context: contexts) {
                    context.deleteNumChildLocks(transaction, context.parent);
                }
            }
        }
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        if (readonly) throw new UnsupportedOperationException("The context is read only!");
        LockType lockType = lockman.getLockType(transaction, name);
        if (lockType.equals(LockType.NL )) {
            throw new NoLockHeldException("There is no lock held on the transaction!");
        } else if (lockType.equals(LockType.X) || lockType.equals(LockType.S)) {
            return;
        }
        List<LockContext> children = getAllChildren(transaction);
        List<ResourceName> childrenNames = getChildrenNames(children);
        if (lockType.equals(LockType.IS)) {
            lockman.acquireAndRelease(transaction, name, LockType.S, childrenNames);
            for(LockContext context: children) {
                context.deleteNumChildLocks(transaction, context.parent);
            }
        } else if (lockType.equals(LockType.IX) || lockType.equals(LockType.SIX)) {
            lockman.acquireAndRelease(transaction, name, LockType.X, childrenNames);
            for (LockContext context: children) {
                context.deleteNumChildLocks(transaction, context.parent);
            }
        }

    }

    public List<LockContext> getAllChildren(TransactionContext transaction) {
        Set<LockContext> set = new HashSet<>();
        set.add(this);

        List<Lock> locks = lockman.getLocks(transaction);
        int beforeSize = 0;
        int size = 1;
        while (beforeSize != size) {
            beforeSize = size;
            for (Lock lock: locks) {
                LockContext child = fromResourceName(lockman, lock.name);
                if (child.parent != null && set.contains(child.parent) &&
                    !set.contains(child)) {
                    set.add(child);
                    size++;
                }
            }
        }
        return new ArrayList<>(set);
    }

    public List<ResourceName> getChildrenNames(List<LockContext> children) {
        List<ResourceName> list = new ArrayList<>();
        for (LockContext child: children) {
            list.add(child.getResourceName());
        }
        return list;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction, name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        LockType explicitLockType = getExplicitLockType(transaction);
        if (!explicitLockType.equals(LockType.NL)) return explicitLockType;
        LockContext travel = parent;
        while (travel != null) {
            if (travel.getExplicitLockType(transaction).equals(LockType.NL)) {
                travel = travel.parent;
            } else {
                break;
            }
        }
        if (travel == null) return LockType.NL;
        LockType lockType = travel.getExplicitLockType(transaction);
        if (lockType.equals(LockType.SIX)) {
            return LockType.S;
        } else if (lockType.equals(LockType.IS) || lockType.equals(LockType.IX)) {
            return LockType.NL;
        }
        return lockType;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        LockContext cur = parent;
        while (cur != null) {
            LockType lockType = lockman.getLockType(transaction, cur.getResourceName());
            if (lockType.equals(LockType.SIX)) return true;
            cur = cur.parent;
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<ResourceName> res = new ArrayList<>();
        List<Lock> locks = lockman.getLocks(transaction);
        for (Lock lock: locks) {
            if (lock.lockType.equals(LockType.S) || lock.lockType.equals(LockType.IS)) {
                LockContext level = fromResourceName(lockman, lock.name);
                if (isDescendant(level, this) && !level.equals(this)) {
                    res.add(lock.name);
                }
            }
        }

        return res;
    }

    private boolean isDescendant(LockContext level, LockContext parent) {
        if (level == null) return false;
        if (parent == null) return true;
        LockContext cur = level;
        while (cur != null) {
            if (cur == parent) return true;
            cur = cur.parent;
        }
        return false;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

