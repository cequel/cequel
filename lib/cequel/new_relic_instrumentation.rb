begin
  require 'new_relic/agent/method_tracer'
rescue LoadError => e
  raise LoadError, "Can't use NewRelic instrumentation without NewRelic gem"
end

module Cequel

  module NewRelicInstrumentation

    extend ActiveSupport::Concern

    included do
      include NewRelic::Agent::MethodTracer
      add_method_tracer :execute, 'Database/Cassandra/#{args[0][/^[A-Z ]*[A-Z]/].sub(/ FROM$/, \'\')}'
    end

  end

end

Cequel::Keyspace.module_eval { include Cequel::NewRelicInstrumentation }
