package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor locks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null || requestType == LockType.NL) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement

//        // Release lock on leaf
//        if (requestType == LockType.NL && explicitLockType != LockType.NL && !explicitLockType.isIntent()) {
//            lockContext.release(transaction);
//            return;
//        }

//        if (requestType == LockType.NL) return;

        // Ensure that we have the appropriate locks on ancestors, except SIX case
//        if (!(explicitLockType == LockType.IX && requestType == LockType.S) && !LockUtil.ensureAppropriateLocksOnAncestors(lockContext, requestType, transaction, false)) {
//            return;
//        }
        // explicit = X, request = S
        if (LockType.substitutable(effectiveLockType, requestType)) return;

        // Do nothing if no lock going to be held by transaction on the LockContext is the same
        if (explicitLockType == requestType) return;

        setParentLocks(lockContext, requestType, transaction);

        // The current lock type can effectively substitute the requested type
        if (LockType.substitutable(requestType, explicitLockType)) {
//            if (!LockUtil.ensureAppropriateLocksOnAncestors(parentContext, LockType.parentLock(requestType), transaction, false)) return;
//            if (LockType.substitutable(LockType.parentLock(requestType), parentContext.getExplicitLockType(transaction))) LockUtil.setParentLocks(parentContext, LockType.parentLock(requestType), transaction);
            // explicit = NL, request = S
            // explicit = NL, request = X
            if (explicitLockType == LockType.NL) {
                lockContext.acquire(transaction, requestType);
            } else {
                // explicit = S, request = X
                lockContext.promote(transaction, requestType);
            }
        }
        // explicit = IX, request = S
        else if (explicitLockType == LockType.IX && requestType == LockType.S) {
            lockContext.promote(transaction, LockType.SIX);
        }
        // The current lock type is an intent lock (IS, IX, SIX)
        else if (explicitLockType.isIntent()) {
            // explicit = SIX, request = S
            // do nothing
            if (explicitLockType == LockType.SIX && requestType == LockType.S) return;
            // explicit = IX, request = X
            // explicit = SIX, request = X
            // explicit = IS, request = S
            // escalate
            lockContext.escalate(transaction);
            // explicit = IS, request = X
            // escalate then promote
            if (explicitLockType == LockType.IS && requestType == LockType.X) {
                lockContext.promote(transaction, requestType);
            }

        }
        // None of the above: In this case, consider what values the explicit lock type can be,
        // and think about how ancestor locks will need to be acquired or changed
        // explicit = X, request = S
        else {

        }

    }

    // Ensure that we have the appropriate locks on ancestors
    // Acquiring the lock on the resource.
    // You will need to promote in some cases, and escalate in some cases (these cases are not mutually exclusive)

    // TODO(proj4_part2) add any helper methods you want

    /**
     * Set all ancestors as requestType. Assumes ensureAppropriateLocksOnAncestors was pre-called and evaluated to true.
     */
    public static void setParentLocks(LockContext lockContext, LockType requestType, TransactionContext transaction) {

//        if (lockContext != null) {
//
//            LockType explicitLockType = lockContext.getExplicitLockType(transaction);
//
//            // No need to change if locks are the same
//            if (explicitLockType == requestType) return;
//
//            // Recurse
//            setParentLocks(lockContext.parentContext(), requestType, transaction);
//
//            if (explicitLockType == LockType.NL) {
//                lockContext.acquire(transaction, requestType);
//            } else {
//                lockContext.promote(transaction, requestType);
//            }
//        }

        LockContext parentContext = lockContext.parentContext();
        List<LockContext> ancestors = new ArrayList<>();
        while (parentContext != null) {
            ancestors.add(parentContext);
            parentContext = parentContext.parentContext();
        }

        LockType neededLockType = LockType.parentLock(requestType);

        // from topmost to current
        for (int i = ancestors.size() - 1; i >= 0; i--) {
            LockContext ancestorContext = ancestors.get(i);
            LockType currLockType = ancestorContext.getExplicitLockType(transaction);
            if (!LockType.substitutable(currLockType, neededLockType)) {
                if (currLockType == LockType.SIX && neededLockType == LockType.IX) {
                    continue;
                }
                if (currLockType == LockType.S && neededLockType == LockType.IX) {
                    try {
                        ancestorContext.promote(transaction, LockType.SIX);
                    } catch (InvalidLockException e) {
                        continue;
                    }
                    continue;
                }
                if (currLockType == LockType.NL) {
//                    System.out.println("currLockType:");
//                    System.out.println(currLockType);
//                    System.out.println("ancestor:");
//                    System.out.println(ancestorContext.getExplicitLockType(transaction));
//                    System.out.println("needed:");
//                    System.out.println(neededLockType);
                    ancestorContext.acquire(transaction, neededLockType);

                } else {
//                    System.out.println(currLockType);
//                    System.out.println(ancestorContext.getExplicitLockType(transaction));
//                    System.out.println(neededLockType);
                    ancestorContext.promote(transaction, neededLockType);
                }
            }
        }
    }

    /**
     * Ensure you have the appropriate locks on all ancestors. Start from lockContext to topmost LockContext.
     */
    public static boolean ensureAppropriateLocksOnAncestors(LockContext lockContext, LockType requestType, TransactionContext transaction, boolean ok) {

        // Base Case
        if (lockContext == null) {
            return ok;
        }

        // Recurse
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        if (LockType.compatible(requestType, explicitLockType)) {
            return ensureAppropriateLocksOnAncestors(lockContext.parentContext(), requestType, transaction, true);
        } else {
            return false;
        }

    }
}
