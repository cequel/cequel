require File.expand_path('../../environment', __FILE__)
require 'cequel'

Dir.glob(File.expand_path('../../support/**/*.rb', __FILE__)).each do |file|
  require file
end
Dir.glob(File.expand_path('../../shared/**/*.rb', __FILE__)).each do |file|
  require file
end

RSpec.configure do |config|
  config.include(Cequel::SpecSupport::Helpers)
  config.extend(Cequel::SpecSupport::Macros)

  config.filter_run_excluding rails: ->(requirement) {
    !Gem::Requirement.new(requirement).
      satisfied_by?(Gem::Version.new(ActiveSupport::VERSION::STRING))
  }

  config.before(:all) do
    connection = CassandraCQL::Database.new(
      Cequel::SpecSupport::Helpers.host,
      :cql_version => '3.0.0'
    )
    keyspace = Cequel::SpecSupport::Helpers.keyspace_name
    connection.execute <<-CQL
      CREATE KEYSPACE #{keyspace}
      WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
    CQL
    Cequel::Record.connection = cequel
  end

  config.after(:all) do
    keyspace = Cequel::SpecSupport::Helpers.keyspace_name
    cequel.execute("DROP KEYSPACE #{keyspace}")
  end
end

if defined? byebug
  Kernel.module_eval { alias_method :debugger, :byebug }
end
