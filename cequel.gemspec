require File.expand_path('../lib/cequel/version', __FILE__)

Gem::Specification.new do |s|
  s.name = 'cequel'
  s.version = Cequel::VERSION
  s.authors = ['Mat Brown', 'Aubrey Holland', 'Keenan Brock']
  s.email = 'mat.a.brown@gmail.com'
  s.license = 'MIT'
  s.summary = 'Query abstraction layer and object-row mapper for Cassandra and CQL'
  s.description = <<DESC
Cequel is a lightweight query abstraction layer for Cassandra's CQL language. It
also provides Cequel::Model, which is an ActiveModel-compliant object-row mapper
for Cassandra. Cequel is heavily inspired by the Sequel library.
DESC

  s.files = Dir['lib/**/*.rb', 'spec/**/*.rb']
  s.test_files = Dir['spec/examples/**/*.rb']
  s.has_rdoc = false
  #s.extra_rdoc_files = 'README.md'
  s.required_ruby_version = '>= 1.9'
  s.add_runtime_dependency 'activesupport', '~> 3.1'
  s.add_runtime_dependency 'activemodel', '~> 3.1'
  s.add_runtime_dependency 'cassandra-cql', '~> 1.2'
  s.add_runtime_dependency 'connection_pool', '~> 0.9.2'
  s.add_runtime_dependency 'i18n'
  s.add_development_dependency 'rspec', '~> 2.0'
  s.add_development_dependency 'yard', '~> 0.6'
  s.requirements << 'Cassandra 1.0+'
end
