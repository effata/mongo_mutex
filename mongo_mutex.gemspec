$: << File.expand_path('../lib', __FILE__)

require 'mongo_mutex/version'

Gem::Specification.new do |s|
  s.name = 'mongo_mutex'
  s.version = MongoMutex::VERSION
  s.licenses = ['BSD-3-Clause']
  s.files = ['lib/mongo_mutex.rb', 'lib/mongo_mutex/mutex.rb']
  s.require_paths = ['lib']
  s.bindir = 'bin'
  s.executables = %w[mongo_mutex]
  s.summary = "Mongo Mutex"
  s.description = "A distributed lock using MongoDB as backend"
  s.authors = ['David Dahl', 'Gustav Munkby']
  s.email = 'david@burtcorp.com'
  s.homepage = 'https://github.com/effata/mongo_mutex'
  s.add_dependency 'mongo', '~> 2.1'
end
