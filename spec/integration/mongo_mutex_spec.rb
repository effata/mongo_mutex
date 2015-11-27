# encoding: utf-8

require 'mongo'
require 'mongo_mutex'
Mongo::Logger.logger.level = ::Logger::FATAL

describe MongoMutex do
  let :mutex do
    described_class.new(collection, lock_id, locker_id, options)
  end

  let :second_mutex do
    described_class.new(collection, lock_id, second_locker_id, options)
  end

  let :collection do
    client = Mongo::Client.new(['localhost'], :database => 'mongo_mutex_test')
    client[:mutex]
  end

  let :lock_id do
    'lock-id'
  end

  let :locker_id do
    'locking-host'
  end

  let :second_locker_id do
    'another-locker-host'
  end

  let :options do
    { clock: clock, logger: logger }
  end

  let :clock do
    double(:clock, now: Time.at(1_000_000))
  end

  let :logger do
    double(:logger, warn: nil)
  end

  before do
    collection.drop
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
      end
      expect(mutex).to_not be_locked
    end
  end

  context 'when arriving the first time' do
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
      second_mutex.lock
      @sleeps = 0
      allow(mutex).to receive(:sleep) do
        @sleeps += 1
        if @sleeps >= 3
          second_mutex.unlock
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
      allow(clock).to receive(:now).and_return(Time.at(0))
      second_mutex.lock
      allow(clock).to receive(:now).and_return(Time.at(1_000_000))
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
      expect(mutex).to_not be_locked
    end

    it 'returns false when unlocked' do
      mutex.lock
      mutex.unlock
      expect(mutex).to_not be_locked
    end

    it 'returns true when locked by us' do
      mutex.lock
      expect(mutex).to be_locked
    end

    it 'returns true when locked by someone else' do
      second_mutex.lock
      expect(mutex).to be_locked
    end

    it 'returns false when lock expired' do
      allow(clock).to receive(:now).and_return(Time.at(0))
      second_mutex.lock
      allow(clock).to receive(:now).and_return(Time.at(1_000_000))
      expect(mutex).to_not be_locked
    end
  end

  describe '#unlock' do
    context 'when locked by us' do
      it 'does not raise an error' do
        mutex.lock
        expect do
          mutex.unlock
        end.not_to raise_error
      end
    end

    context 'when not locked, timed out lock or locked by another party' do
      before do
        second_mutex.lock
      end

      it 'raises an error' do
        expect do
          mutex.unlock
        end.to raise_error(ThreadError)
      end
    end
  end
end
