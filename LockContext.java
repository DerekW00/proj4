package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

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

        // error checking
        if (readonly) throw new UnsupportedOperationException("context is readonly");

        if (!getExplicitLockType(transaction).equals(LockType.NL)) throw new DuplicateLockRequestException("lock alr held by the transaction");

        if (parentContext() != null && !LockType.canBeParentLock(parentContext().getExplicitLockType(transaction), lockType)) {
            System.out.println(parentContext() + ", " + this + ", " + parentContext().getExplicitLockType(transaction) + ", " + lockType + ", " + !LockType.canBeParentLock(parentContext().getExplicitLockType(transaction), lockType));
            throw new InvalidLockException("cannot be parent");
        }

        if (!sisDescendants(transaction).isEmpty() && lockType == LockType.SIX) {
            throw new InvalidLockException("getting SIX while having S/IS descendants");
        }

        if (hasSIXAncestor(transaction) && (lockType == LockType.IS || lockType == LockType.S)) {
            throw new InvalidLockException("getting IS/S while having SIX ancestor");
        }

        // compatibility checked by lock manager
        // no substitutability because it does not have a lock in the first place

        // acquire
        lockman.acquire(transaction, name, lockType);

        // update numChildLocks
        if (parentContext() != null) {
            int numChildNew = parentContext().getNumChildren(transaction) + 1;
            parentContext().numChildLocks.put(transaction.getTransNum(), numChildNew);
        }

        return;
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
        if (readonly) throw new UnsupportedOperationException("context is readonly");
        if (getExplicitLockType(transaction) == LockType.NL) throw new NoLockHeldException("no lock on 'name' is held");

        if (getNumChildren(transaction) > 0) throw new InvalidLockException("cannot release before children");

        // releases
        lockman.release(transaction, name);

        // update numChildLocks
        if (parentContext() != null) {
            int numChildNew = parentContext().getNumChildren(transaction) - 1;
            parentContext().numChildLocks.put(transaction.getTransNum(), numChildNew);
        }

        return;
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
        if (readonly) throw new UnsupportedOperationException("context is readonly");
        if (getExplicitLockType(transaction).equals(newLockType)) throw new DuplicateLockRequestException("already has newLockType");
        if (getExplicitLockType(transaction).equals(LockType.NL)) throw new NoLockHeldException("transaction has no lock");


        LockType currLockType = getExplicitLockType(transaction);

        // promote from A to B
        // if B is substitutable for A and B is not equal to A
        if (newLockType != currLockType && LockType.substitutable(newLockType, currLockType)) {
            lockman.promote(transaction, name, newLockType);

            // if B is SIX and A is IS/IX/S
        } else if (newLockType == LockType.SIX && (currLockType == LockType.IS || currLockType == LockType.IX || currLockType == LockType.S)) {
            // disallow if an ancestor has SIX
            if (hasSIXAncestor(transaction)) {
                throw new InvalidLockException("promote to SIX but ancestor has SIX");
            }

            List<ResourceName> descReleased = sisDescendants(transaction);
            descReleased.add(name);
            // release all descendant locks IS/S
            lockman.acquireAndRelease(transaction, name, newLockType, descReleased);

            // update numChildLocks
            for (ResourceName rn : descReleased) {
                LockContext parentContext = fromResourceName(lockman, rn).parentContext();
                if (parentContext != null) {
                    int numChildNew = parentContext.getNumChildren(transaction) - 1;
                    parentContext.numChildLocks.put(transaction.getTransNum(), numChildNew);
                }
            }

        } else {
            throw new InvalidLockException("invalid somehow");
        }


        return;
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

        if (readonly) throw new UnsupportedOperationException("context is readonly");
        if (getExplicitLockType(transaction).equals(LockType.NL)) throw new NoLockHeldException("transaction has no lock");
        if (getExplicitLockType(transaction).equals(LockType.S) || getExplicitLockType(transaction).equals(LockType.X)) return;

        List<ResourceName> releaseNames = new ArrayList<>();
        LockType lock = lockman.getLockType(transaction, name);


        // If the descendants don't have a lock, return
        for (ResourceName rn : getAllDescendants(transaction)) {
            if (lockman.getLockType(transaction, rn) != LockType.NL) {
                releaseNames.add(rn);
            }
        }

        releaseNames.add(name);

        if (lock == LockType.IS) {
            lockman.acquireAndRelease(transaction, name, LockType.S, releaseNames);
        } else if (lock == LockType.IX || lock == LockType.SIX) {
            lockman.acquireAndRelease(transaction, name, LockType.X, releaseNames);
        }


        // update numChildLocks
        for (ResourceName rn : releaseNames) {
            LockContext parentContext = fromResourceName(lockman, rn).parentContext();
            if (parentContext != null) {
                int numChildNew = parentContext.getNumChildren(transaction) - 1;
                parentContext.numChildLocks.put(transaction.getTransNum(), numChildNew);
            }
        }

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
        LockType ret = getExplicitLockType(transaction);
        LockContext curr = this;

        while (curr.parentContext() != null) {
            LockType parentLockType = curr.parentContext().getExplicitLockType(transaction);

            if (parentLockType == LockType.S || parentLockType == LockType.X) ret = parentLockType;
            if (parentLockType == LockType.SIX) ret = LockType.S;

            curr = curr.parentContext();
        }
        return ret;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        for (Lock lk : lockman.getLocks(transaction)) {
            if (name.isDescendantOf(lk.name) && lk.lockType == LockType.SIX) return true;
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
        List<ResourceName> descList = new ArrayList<>();
        for (Lock lk : lockman.getLocks(transaction)) {
            if (lk.name.isDescendantOf(name) && (lk.lockType == LockType.S || lk.lockType == LockType.IS)) descList.add(lk.name);
        }
        return descList;
    }

    private List<ResourceName> getAllDescendants(TransactionContext transaction) {
        List<ResourceName> descList = new ArrayList<>();
        for (Lock lk : lockman.getLocks(transaction)) {
            if (lk.name.isDescendantOf(name)) descList.add(lk.name);
        }
        return descList;
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

