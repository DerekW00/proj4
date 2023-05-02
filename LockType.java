package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement

        if (a == NL || b == NL) {
            return true;
        }

        if (a == IS) {
            if (b == IS) {
                return true;
            } else if (b == IX) {
                return true;
            } else if (b == S) {
                return true;
            } else if (b == SIX) {
                return true;
            } else if (b == X) {
                return false;
            }

        } else if (a == IX) {
            if (b == IS) {
                return true;
            } else if (b == IX) {
                return true;
            } else if (b == S) {
                return false;
            } else if (b == SIX) {
                return false;
            } else if (b == X) {
                return false;
            }

        } else if (a == S) {
            if (b == IS) {
                return true;
            } else if (b == IX) {
                return false;
            } else if (b == S) {
                return true;
            } else if (b == SIX) {
                return false;
            } else if (b == X) {
                return false;
            }

        } else if (a == SIX) {
            if (b == IS) {
                return true;
            } else if (b == IX) {
                return false;
            } else if (b == S) {
                return false;
            } else if (b == SIX) {
                return false;
            } else if (b == X) {
                return false;
            }

        } else if (a == X) {
            if (b == IS) {
                return false;
            } else if (b == IX) {
                return false;
            } else if (b == S) {
                return false;
            } else if (b == SIX) {
                return false;
            } else if (b == X) {
                return false;
            }
        }
        return a == NL || b == NL;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }

        // TODO(proj4_part1): implement
        // since holding an SIX lock already allows reading all descendants,
        // requesting an S or IS or SIX lock would be redundant.

        if (parentLockType == NL) {
            return childLockType == NL;
        }

        if (childLockType == IS || childLockType == S) {
            return (parentLockType == IS || parentLockType == IX);

        } else if (childLockType == X || childLockType == IX) {
            return (parentLockType == IX || parentLockType == SIX);

        } else if (childLockType == SIX) {
            return parentLockType == IX;

        }

        return childLockType == NL;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        // substitute(S, X) = false;
        // true if substitute can do everything required can do
        // see GradeScope Optional Assignment for better understanding
        if (substitute == required) {
            return true;
        }
        if (required == NL) {
            return true;
        }

        if (required == IS) {
            return substitute == IX;

            // only allowed when substitute = required = IX
        } else if (required == IX) {
            return false;

        } else if (required == S) {
            return substitute == X || substitute == SIX;

            // only allowed when substitute = required = SIX
        } else if (required == SIX) {
            return false;

            // only allowed when substitute = required = X
        } else if (required == X) {
            return false;

        }
        return false;
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

