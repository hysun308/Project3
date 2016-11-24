package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Database;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Each table will have a lock object associated with it in order
 * to implement table-level locking. The lock will keep track of its
 * transaction owners, type, and the waiting queue.
 */
public class Lock {


  private Set<Long> transactionOwners;
  private ConcurrentLinkedQueue<LockRequest> transactionQueue;
  private LockManager.LockType type;

  public Lock(LockManager.LockType type) {
    this.transactionOwners = new HashSet<Long>();
    this.transactionQueue = new ConcurrentLinkedQueue<LockRequest>();
    this.type = type;
  }

  protected Set<Long> getOwners() {
    return this.transactionOwners;
  }

  public LockManager.LockType getType() {
    return this.type;
  }

  private void setType(LockManager.LockType newType) {
    this.type = newType;
  }

  public int getSize() {
    return this.transactionOwners.size();
  }

  public boolean isEmpty() {
    return this.transactionOwners.isEmpty();
  }

  private boolean containsTransaction(long transNum) {
    return this.transactionOwners.contains(transNum);
  }

  private void addToQueue(long transNum, LockManager.LockType lockType) {
    LockRequest lockRequest = new LockRequest(transNum, lockType);
    this.transactionQueue.add(lockRequest);
  }

  private void removeFromQueue(long transNum, LockManager.LockType lockType) {
    LockRequest lockRequest = new LockRequest(transNum, lockType);
    this.transactionQueue.remove(lockRequest);
  }

  private void addOwner(long transNum) {
    this.transactionOwners.add(transNum);
  }

  private void removeOwner(long transNum) {
    this.transactionOwners.remove(transNum);
  }

  /**
   * Attempts to resolve the specified lockRequest. Adds the request to the queue
   * and calls wait() until the request can be promoted and removed from the queue.
   * It then modifies this lock's owners/type as necessary.
   * @param transNum transNum of the lock request
   * @param lockType lockType of the lock request
   */
  protected synchronized void acquire(long transNum, LockManager.LockType lockType) {
    //TODO: Implement Me!!
      if(this.containsTransaction(transNum)&&this.type.equals(lockType)){

      }else {
          this.addToQueue(transNum, lockType);
          while (!this.checkCompatible(transNum, lockType)) {
              try {
                  this.wait();
              } catch (InterruptedException ie) {

              }
          }
          this.removeFromQueue(transNum, lockType);
          this.addOwner(transNum);
          this.setType(lockType);
      }
      this.notifyAll();

    return;
  }

  /**
   * transNum releases ownership of this lock
   * @param transNum transNum of transaction that is releasing ownership of this lock
   */
  protected synchronized void release(long transNum) {
    //TODO: Implement Me!!
      this.removeOwner(transNum);
      this.notifyAll();

    return;
  }

  /**
   * Checks if the specified transNum holds a lock of lockType on this lock object
   * @param transNum transNum of lock request
   * @param lockType lock type of lock request
   * @return true if transNum holds the lock of type lockType
   */
  protected synchronized boolean holds(long transNum, LockManager.LockType lockType) {
    //TODO: Implement Me!!

      if(this.containsTransaction(transNum)&&this.type.equals(lockType)){
          return true;
      }
    return false;
  }

  private boolean checkCompatible (long transNum,LockManager.LockType lockType){
      LockRequest lockRequest = new LockRequest(transNum,lockType);
      if(lockType.equals(LockManager.LockType.EXCLUSIVE)){
          if(this.containsTransaction(transNum)&&this.type.equals(LockManager.LockType.SHARED)&&this.getOwners().size()==1){
              return true;
          }
      }
      if(this.transactionQueue.peek().equals(lockRequest)&&this.transactionOwners.isEmpty()){
          return true;
      }
      if(lockType.equals(LockManager.LockType.SHARED)&&this.type.equals(LockManager.LockType.SHARED)){
          Iterator<LockRequest> lockRequestIterator = this.transactionQueue.iterator();
          while (lockRequestIterator.hasNext()){
              LockRequest currLockRequest = lockRequestIterator.next();
              if(currLockRequest.lockType.equals(LockManager.LockType.EXCLUSIVE)){
                  break;
              }
              if(currLockRequest.equals(lockRequest)){
                  return true;
              }
          }
      }

      return false;
  }

  /**
   * LockRequest objects keeps track of the transNum and lockType.
   * Two LockRequests are equal if they have the same transNum and lockType.
   */
  private class LockRequest {
      private long transNum;
      private LockManager.LockType lockType;
      private LockRequest(long transNum, LockManager.LockType lockType) {
        this.transNum = transNum;
        this.lockType = lockType;
      }

      @Override
      public int hashCode() {
        return (int) transNum;
      }

      @Override
      public boolean equals(Object obj) {
        if (!(obj instanceof LockRequest))
          return false;
        if (obj == this)
          return true;

        LockRequest rhs = (LockRequest) obj;
        return (this.transNum == rhs.transNum) && (this.lockType == rhs.lockType);
      }

  }

}
