# -*- encoding : utf-8 -*-
begin
  require 'new_relic/agent/datastores'
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

      define_method :execute_with_consistency_with_newrelic do |statement, bind_vars, consistency|
        callback = Proc.new do |result, scoped_metric, elapsed|
          NewRelic::Agent::Datastores.notice_statement(statement, elapsed)
        end

        statement_words = statement.split
        operation = statement_words.first.downcase
        table = nil
        case operation
        when "begin"
          operation = "batch"
        when "select"
          table = statement_words.at(statement_words.index("FROM") + 1)
        when "insert"
          table = statement_words[2]
        when "update"
          table = statement_words[1]
        end

        NewRelic::Agent::Datastores.wrap("Cassandra", operation, table, callback) do
          execute_with_consistency_without_newrelic(statement, bind_vars, consistency)
        end
      end

      included do
        alias_method_chain :execute_with_consistency, :newrelic
      end
    end
  end
end

Cequel::Metal::Keyspace.module_eval do
  include Cequel::Metal::NewRelicInstrumentation
end
