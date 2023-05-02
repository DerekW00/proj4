package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;
/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement
            for (Lock lock : locks) {
                if (!LockType.compatible(lockType, lock.lockType)) {
                    // allows conflicts for locks already held by the transaction
                    if (!(lock.transactionNum == except)) {
                        return false;
                    }
                }
            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement

            // if the transaction does not hold a lock on this resource, grant lock
            if (getTransactionLockType(lock.transactionNum) == LockType.NL) {
                locks.add(lock);

                List<Lock> existingLocks = transactionLocks.getOrDefault(lock.transactionNum, new ArrayList<>());
                existingLocks.add(lock);
                transactionLocks.put(lock.transactionNum, existingLocks);

                // otherwise, update lock
            } else {
                // remove old lock in transactionLocks HashMap, update with new lock

                List<Lock> existingLocks = transactionLocks.get(lock.transactionNum);
                Lock lockToRmHM = null;
                for (Lock oldLock : existingLocks) {
                    // locks with the same resource name, pointing to the same resource
                    if (oldLock.name == lock.name) {
                        lockToRmHM = oldLock;
                        break;
                    }
                }
                existingLocks.remove(lockToRmHM);
                existingLocks.add(lock);
                transactionLocks.put(lock.transactionNum, existingLocks);

                // remove old lock in the resource's locks list, update with new lock
                Lock lockToRm = null;
                for (Lock oldLock : locks) {
                    if (oldLock.transactionNum.equals(lock.transactionNum)) {
                        lockToRm = oldLock;
                        break;
                    }
                }
                locks.remove(lockToRm);
                locks.add(lock);
            }
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement
            if (!locks.contains(lock) || !transactionLocks.containsKey(lock.transactionNum)) {
                return;
            }

            // remove lock from transactionLocks HashMap for the corresponding transaction
            List<Lock> existingLocks = transactionLocks.get(lock.transactionNum);
            existingLocks.remove(lock);
            if (existingLocks.isEmpty()) {
                transactionLocks.remove(lock.transactionNum);
            } else {
                transactionLocks.put(lock.transactionNum, existingLocks);
            }

            // remove lock from the resource's locks list
            locks.remove(lock);

            processQueue();

        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement
            if (addFront) {
                waitingQueue.addFirst(request);
            } else {
                waitingQueue.addLast(request);
            }
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
//            Iterator<LockRequest> requests = waitingQueue.iterator();

            // TODO(proj4_part1): implement

            while (true) {
                LockRequest req = waitingQueue.peek();
//                Lock reqlock = req.lock;

                // For each request, if compatible,
                // release the releasedLocks/grant lock/unblock transaction, and remove the request.
                if (req != null && checkCompatible(req.lock.lockType, req.lock.transactionNum)) {
                    waitingQueue.removeFirst();
                    Lock reqlock = req.lock;
                    grantOrUpdateLock(reqlock);
                    int size = req.releasedLocks.size();
                    for (int i = 0; i < size; i++) {
                        Lock rl = req.releasedLocks.get(i);
                        releaseLock(rl);
                        req.releasedLocks.remove(rl);
                    }
                    req.transaction.unblock();

                // Put request back to the back of the queue and block
                } else {
//                    addToQueue(waitingQueue.removeFirst(), false);
//                    req.transaction.prepareBlock();
//                    req.transaction.block();
                    break;
                }
            }
            return;
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            for (Lock lock : locks) {
                if (lock.transactionNum == transaction) {
                    return lock.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {

            List<Lock> tLocks = getLocks(transaction); // Locks on transaction
            List<Lock> rLocks = getLocks(name); // Locks on resource
//            List<Lock> listOfLocks = getLocks(name);
//            ResourceEntry rEntry = resourceEntries.get(name);
              ResourceEntry rEntry = getResourceEntry(name);
//            System.out.println(rEntry);

//             If a lock on `name` is held by `transaction`, throw err
//            for (Lock rlock: rLocks) {
//                for (Lock tlock: tLocks) {
//                    if (tlock.equals(rlock) && !releaseNames.contains(name)) {
//                        throw new DuplicateLockRequestException("lock on `name` is already held by `transaction` and isn't being released");
//                    }
//                }
//            }

            if (getLockType(transaction, name) != LockType.NL && !releaseNames.contains(name))
                throw new DuplicateLockRequestException("lock on `name` is already held by `transaction` and isn't being released");

            // if `transaction` doesn't hold a lock on one
            // or more of the names in `releaseNames`
            for (ResourceName rname : releaseNames) {
                if (getLockType(transaction, rname).equals(LockType.NL)) {
                    throw new NoLockHeldException("`transaction` doesn't hold a lock on one or more of the names in `releaseNames`");
                }
            }

//            if (true) throw new NoLockHeldException("`transaction` doesn't hold a lock on one or more of the names in `releaseNames`");

            // Create a new lock request
            Lock newLock = new Lock(name, lockType, transaction.getTransNum());
            LockRequest newRequest = new LockRequest(transaction, newLock);

            // If the new lock is not compatible with another transaction's lock on the
            // resource, the transaction is blocked and the request is placed at the
            // FRONT of the resource's queue.
            if (!rEntry.checkCompatible(lockType, transaction.getTransNum())) {
                shouldBlock = true;
                rEntry.addToQueue(newRequest,true);
            } else {
                rEntry.grantOrUpdateLock(newLock);
                for (ResourceName n : releaseNames) {
                    if (n.equals(name)) continue;
                    release(transaction, n);
//                    for (Lock lock: getLocks(n)) {
//                        rEntry.releaseLock(lock);
//                    }
                }
            }

//            // Acquire Lock
//            acquire(transaction, name, lockType);

//            // Locks on `releaseNames` should be released only after the requested lock
//            // has been acquired. The corresponding queues should be processed.
//            for (ResourceName n : releaseNames) {
//                for (Lock lock: getLocks(n)) {
//                    rEntry.releaseLock(lock);
//                }
////            }
//            rEntry.processQueue();
//
//            // Release Lock
//            release(transaction, name);

        }
        if (shouldBlock) {
            transaction.prepareBlock();
            transaction.block();
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {

            // Get new/existing entry on resource
            ResourceEntry rEntry = getResourceEntry(name);
            if (rEntry == null) {
                rEntry = getResourceEntry(name);
            }

            // Make new lock and request
            Lock newLock = new Lock(name, lockType, transaction.getTransNum());
            LockRequest newRequest = new LockRequest(transaction, newLock);

            List<Lock> tLocks = getLocks(transaction); // Locks on transaction
            List<Lock> rLocks = getLocks(name); // Locks on resource
//             If a lock on `name` is held by `transaction`, throw err
            for (Lock rlock: rLocks) {
                for (Lock tlock: tLocks) {
                    if (tlock.equals(rlock)) {
                        throw new DuplicateLockRequestException("lock on `name` is already held by `transaction` and isn't being released");
                    }
                }
            }


            // If the new lock is not compatible with another transaction's lock on the resource, or if there are
            // other transaction in the queue for the resource, the transaction is
            // blocked and the request is placed at the BACK of NAME's queue.


            // Zoe: do we need to check if waitingQueue is empty before granting?
            // might cause problem since acquire is called by acquireAndRelease
            if (rEntry.checkCompatible(lockType, transaction.getTransNum()) && rEntry.waitingQueue.isEmpty()) {

                rEntry.grantOrUpdateLock(newLock);
            } else {
                shouldBlock = true;
                rEntry.addToQueue(newRequest,false);

            }

            // Zoe: what is this part doing?
//            Iterator<LockRequest> iter = rEntry.waitingQueue.iterator();
//            while (iter.hasNext()) {
//                if (transaction.equals(iter.next().transaction)) {
//                    shouldBlock = true;
//                    rEntry.addToQueue(newRequest,false);
//                    break;
//                }
//            }

            if (shouldBlock) transaction.prepareBlock();

            //System.out.println(resourceEntries);
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {

            ResourceEntry rEntry = getResourceEntry(name);
            if (rEntry == null) throw new NoLockHeldException("no lock on `name` is held by `transaction`");
            if (getLockType(transaction, name).equals(LockType.NL)) throw new NoLockHeldException("no lock on `name` is held by `transaction`");
            Lock lock = null;

            for (Lock l : rEntry.locks) {
                if (l.transactionNum.equals(transaction.getTransNum())) {
                    lock = l;
                }
            }

            if (lock == null) throw new NoLockHeldException("no lock on `name` is held by `transaction`");

            rEntry.releaseLock(lock);

            // The resource name's queue should be processed after this call
//            rEntry.processQueue();

            // If any requests in the queue have locks to be released, those should be
            // released, and the corresponding queues also processed
            // x_x"

        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        boolean shouldBlock = false;
        synchronized (this) {
            if (getLockType(transaction, name).equals(newLockType)) throw new DuplicateLockRequestException("LockType exists");
            if (getLockType(transaction, name).equals(LockType.NL)) throw new NoLockHeldException("No Lock Held on name");
            if (!LockType.substitutable(newLockType, getLockType(transaction, name))) throw new InvalidLockException("cannot substitute");

            ResourceEntry rEntry = getResourceEntry(name);
            Lock newLock = new Lock(name, newLockType, transaction.getTransNum());
            LockRequest newRequest = new LockRequest(transaction, newLock);

            // if compatible, give lock
            if (rEntry.checkCompatible(newLockType, transaction.getTransNum())) {
                rEntry.grantOrUpdateLock(newLock);
                // if not compatible, blocked and added to front of queue
            } else {
                shouldBlock = true;
                rEntry.addToQueue(newRequest, true);
                transaction.prepareBlock();
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        ResourceEntry resourceEntry = getResourceEntry(name);
        if (resourceEntry != null) {
            return resourceEntry.getTransactionLockType(transaction.getTransNum());
        }
        return LockType.NL;
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}
