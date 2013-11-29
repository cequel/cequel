require File.expand_path('../lib/cequel/version', __FILE__)

Gem::Specification.new do |s|
  s.name = 'cequel'
  s.version = Cequel::VERSION
  s.authors = ['Mat Brown', 'Aubrey Holland', 'Keenan Brock', 'Insoo Buzz Jung', 'Louis Simoneau']
  s.homepage = "https://github.com/cequel/cequel"
  s.email = 'mat.a.brown@gmail.com'
  s.license = 'MIT'
  s.summary = 'Full-featured, ActiveModel-compliant ORM for Cassandra using CQL3'
  s.description = <<DESC
Cequel is an ActiveRecord-like domain model layer for Cassandra that exposes
the robust data modeling capabilities of CQL3, including parent-child
relationships via compound primary keys and in-memory atomic manipulation of
collection columns.
DESC

  s.files = Dir['lib/**/*.rb', 'templates/**/*.rb', 'spec/**/*.rb']
  s.test_files = Dir['spec/examples/**/*.rb']
  s.has_rdoc = false
  #s.extra_rdoc_files = 'README.md'
  s.required_ruby_version = '>= 1.9'
  s.add_runtime_dependency 'activesupport', '>= 3.1'
  s.add_runtime_dependency 'activemodel'
  s.add_runtime_dependency 'cassandra-cql', '~> 1.2'
  s.add_runtime_dependency 'connection_pool', '~> 1.1'
  s.add_runtime_dependency 'i18n'
  s.add_development_dependency 'appraisal'
  s.add_development_dependency 'rspec', '~> 2.0'
  s.add_development_dependency 'yard', '~> 0.6'
  s.requirements << 'Cassandra 1.0+'
end
