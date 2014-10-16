# -*- encoding : utf-8 -*-
begin
  require 'new_relic/agent/method_tracer'
rescue LoadError => e
  fail LoadError, "Can't use NewRelic instrumentation without NewRelic gem"
end

module Cequel
  module Metal
    #
    # Provides NewRelic instrumentation for CQL queries.
    #
    module NewRelicInstrumentation
      extend ActiveSupport::Concern

      included do
        include NewRelic::Agent::MethodTracer

        add_method_tracer :execute_with_consistency,
                          'Database/Cassandra/#{args[0][/^[A-Z ]*[A-Z]/]' \
                          '.sub(/ FROM$/, \'\')}'
      end
    end
  end
end

Cequel::Metal::Keyspace.module_eval do
  include Cequel::Metal::NewRelicInstrumentation
end
