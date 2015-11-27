# encoding: utf-8

require 'mongo'
require 'mongo_mutex'

describe MongoMutex do
  let :mutex do
    described_class.new(collection, lock_id, locker_id, options)
  end

  let :collection do
    double(:collection)
  end

  let :lock_id do
    'lock-id'
  end

  let :locker_id do
    'locking-host'
  end

  let :options do
    { lock_operations: lock_operations, clock: clock, logger: logger }
  end

  let :lock_operations do
    double(:lock_operations, unlock: nil)
  end

  let :clock do
    double(:clock, now: Time.at(1_000_000))
  end

  let :logger do
    double(:logger, warn: nil)
  end

  shared_examples 'MongoMutex#try_lock' do
    it 'grabs the lock' do
      expect(mutex.try_lock).to be_truthy
    end
  end

  shared_examples 'MongoMutex#lock' do
    it 'returns self' do
      expect(mutex.lock).to be_equal(mutex)
    end
  end

  shared_examples 'MongoMutex#synchronize' do
    before do
      allow(lock_operations).to receive(:unlock).and_return({'_id' => lock_id})
    end

    it 'returns with the value of the block' do
      expect(mutex.synchronize { :sync }).to be == :sync
    end

    it 'yields to the block once' do
      yields = 0
      mutex.synchronize { yields += 1 }
      expect(yields).to be == 1
    end

    it 'releases the mutex' do
      mutex.synchronize do
        expect(lock_operations).to receive(:unlock)
      end
    end
  end

  context 'when arriving the first time' do
    before do
      allow(lock_operations).to receive(:try_lock).and_return(nil)
    end

    describe '#try_lock' do
      include_examples 'MongoMutex#try_lock'
    end

    describe '#lock' do
      include_examples 'MongoMutex#lock'
    end

    describe '#synchronize' do
      include_examples 'MongoMutex#synchronize'
    end
  end

  context 'when arriving first on subsequent runs' do
    before do
      expect(lock_operations).to receive(:try_lock).and_return({'_id' => lock_id})
    end

    describe '#try_lock' do
      include_examples 'MongoMutex#try_lock'
    end

    describe '#lock' do
      include_examples 'MongoMutex#lock'
    end

    describe '#synchronize' do
      include_examples 'MongoMutex#synchronize'
    end
  end

  context 'when arriving after another host' do
    before do
      @sleeps = 0
      allow(mutex).to receive(:sleep) { @sleeps += 1 }
      allow(lock_operations).to receive(:try_lock) do
        if @sleeps >= 3
          {'_id' => lock_id}
        else
          raise Mongo::Error::OperationFailure.new('insertDocument :: caused by :: 11000 E11000 duplicate key error index: mutex.test.$_id_  dup key: { : \"test\" } (11000)')
        end
      end
    end

    describe '#try_lock' do
      it 'does not grab the lock' do
        expect(mutex.try_lock).to_not be_truthy
      end
    end

    describe '#lock' do
      include_examples 'MongoMutex#lock'

      it 'sleeps until the lock can be acquired' do
        mutex.lock
        expect(@sleeps).to be == 3
      end
    end

    describe '#synchronize' do
      include_examples 'MongoMutex#synchronize'

      it 'sleeps until the lock can be acquired' do
        mutex.synchronize do
          expect(@sleeps).to be == 3
        end
      end
    end
  end

  context 'when arriving after another host that timed out' do
    before do
      allow(lock_operations).to receive(:try_lock).and_return({'_id' => lock_id, 'locked_by' => 'another-locker', 'locked_at' => Time.at(0)})
    end

    describe '#try_lock' do
      include_examples 'MongoMutex#try_lock'

      it 'warns about overriding the timed out lock' do
        expect(logger).to receive(:warn).with(/another-locker/)
        mutex.try_lock
      end
    end

    describe '#lock' do
      include_examples 'MongoMutex#lock'

      it 'warns about overriding the timed out lock' do
        expect(logger).to receive(:warn).with(/another-locker/)
        mutex.try_lock
      end
    end

    describe '#synchronize' do
      include_examples 'MongoMutex#synchronize'

      it 'warns about overriding the timed out lock' do
        expect(logger).to receive(:warn).with(/another-locker/)
        mutex.synchronize { }
      end
    end
  end

  context 'on other Mongo errors on lock' do
    before do
      allow(lock_operations).to receive(:try_lock).and_raise(Mongo::Error::OperationFailure.new('other Mongo error'))
    end

    describe '#try_lock' do
      it 'propagates the error' do
        expect { mutex.try_lock }.to raise_error(Mongo::Error::OperationFailure, 'other Mongo error')
      end
    end

    describe '#lock' do
      it 'propagates the error' do
        expect { mutex.lock }.to raise_error(Mongo::Error::OperationFailure, 'other Mongo error')
      end
    end

    describe '#synchronize' do
      it 'propagates the error' do
        expect { mutex.try_lock }.to raise_error(Mongo::Error::OperationFailure, 'other Mongo error')
      end

      it 'does not try to release the lock' do
        expect(lock_operations).to_not receive(:unlock)
        expect { mutex.try_lock }.to raise_error(Mongo::Error::OperationFailure)
      end
    end
  end

  context 'without lock id' do
    let :lock_id do
      nil
    end

    it 'raises an error on construction' do
      expect { mutex }.to raise_error(ArgumentError)
    end
  end

  context 'without locker id' do
    let :locker_id do
      nil
    end

    it 'raises an error on construction' do
      expect { mutex }.to raise_error(ArgumentError)
    end
  end

  describe '#locked?' do
    it 'returns false when never locked' do
      allow(lock_operations).to receive(:lock_info).and_return(nil)
      expect(mutex).to_not be_locked
    end

    it 'returns false when unlocked' do
      allow(lock_operations).to receive(:lock_info).and_return({'_id' => lock_id})
      expect(mutex).to_not be_locked
    end

    it 'returns true when locked by us' do
      allow(lock_operations).to receive(:lock_info).and_return({'_id' => lock_id, 'locked_by' => locker_id, 'locked_at' => Time.now})
      expect(mutex).to be_locked
    end

    it 'returns true when locked by someone else' do
      allow(lock_operations).to receive(:lock_info).and_return({'_id' => lock_id, 'locked_by' => 'another-locker', 'locked_at' => Time.now})
      expect(mutex).to be_locked
    end

    it 'returns false when lock expired' do
      allow(lock_operations).to receive(:lock_info).and_return({'_id' => lock_id, 'locked_by' => locker_id, 'locked_at' => Time.at(0)})
      expect(mutex).to_not be_locked
    end
  end

  describe '#unlock' do
    context 'when locked by us' do
      before do
        allow(lock_operations).to receive(:unlock).and_return({'_id' => lock_id, 'locked_by' => locker_id, 'locked_at' => Time.now})
      end

      it 'does not raise an error' do
        expect do
          mutex.unlock
        end.not_to raise_error
      end
    end

    context 'when not locked, timed out lock or locked by another party' do
      before do
        allow(lock_operations).to receive(:unlock).and_return(nil)
      end

      it 'raises an error' do
        expect do
          mutex.unlock
        end.to raise_error(ThreadError)
      end
    end

    context 'when raising Mongo errors' do
      before do
        allow(lock_operations).to receive(:unlock).and_raise(Mongo::Error::OperationFailure.new('insertDocument :: caused by :: 11000 E11000 duplicate key error index: mutex.test.$_id_  dup key: { : \"test\" } (11000)'))
      end

      it 'propagates the error' do
        expect do
          mutex.unlock
        end.to raise_error(Mongo::Error::OperationFailure)
      end
    end
  end
end
