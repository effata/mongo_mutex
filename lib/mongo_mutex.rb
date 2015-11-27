# encoding: utf-8

require 'mongo'

class MongoMutex
  def initialize(collection, lock_id, locker_id, options = {})
    raise ArgumentError, "String lock id required" unless lock_id.is_a?(String)
    raise ArgumentError, "String locker id required" unless locker_id.is_a?(String)
    @collection = collection
    @lock_id = lock_id
    @locker_id = locker_id
    options = options.dup
    @lock_check_period = options.delete(:lock_check_period) || 5
    @lock_retention_timeout = options.delete(:lock_retention_timeout) || 600
    @clock = options.delete(:clock) || Time
    @logger = options.delete(:logger)
    @lock_operations = options.delete(:lock_operations) || MongoOperations.new
    raise ArgumentError, "Unsupported options #{options.keys}" unless options.empty?
  end

  def try_lock
    previous = @lock_operations.try_lock(@collection, @lock_id, @locker_id, @clock.now, @lock_retention_timeout)
    if previous && (locked_by = previous[LOCKED_BY])
      if expired?(previous[LOCKED_AT])
        @logger.warn "Ignoring old #{@lock_id} lock by #{locked_by} since #{previous[LOCKED_AT]} was too long ago" if @logger
      else
        raise ThreadError, "mutex already locked by #{locked_by} at #{previous[LOCKED_AT]}"
      end
    end
    true
  rescue Mongo::Error::OperationFailure => error
    if error.message.include?(DUPLICATE_KEY_ERROR)
      return false
    else
      raise
    end
  end

  def locked?
    lock_info = @lock_operations.lock_info(@collection, @lock_id)
    lock_info && lock_info[LOCKED_BY] && !expired?(lock_info[LOCKED_AT])
  end

  def lock
    until try_lock
      sleep @lock_check_period
    end
    self
  end

  def unlock
    unless @lock_operations.unlock(@collection, @lock_id, @locker_id, @clock.now, @lock_retention_timeout)
      raise ThreadError, 'lock is either not locked or locked by someone else'
    end
    self
  end

  def synchronize(&block)
    lock
    begin
      yield
    ensure
      unlock
    end
  end

  private

  LOCKED_BY = 'locked_by'.freeze
  LOCKED_AT = 'locked_at'.freeze

  def expired?(time)
    !time || time < @clock.now - @lock_retention_timeout
  end

  DUPLICATE_KEY_ERROR = 'E11000'

  class MongoOperations
    def lock_info(collection, lock_id)
      collection.find_one({_id: lock_id})
    end

    def try_lock(collection, lock_id, locker_id, now, lock_retention_timeout)
      collection.find_and_modify(
        query: {:_id => lock_id, :$or => [
          {locked_at: {:$lt => now - lock_retention_timeout}},
          {locked_by: locker_id},
          {locked_by: {:$exists => 0}},
        ]},
        update: {_id: lock_id, locked_by: locker_id, locked_at: now},
        upsert: true,
      )
    end

    def unlock(collection, lock_id, locker_id, now, lock_retention_support)
      collection.find_and_modify(
        query: {_id: lock_id, locked_by: locker_id, locked_at: {:$gte => now - lock_retention_support}},
        update: {:$unset => {locked_by: 1, locked_at: 1}},
      )
    end
  end
end
