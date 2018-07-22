# -*- encoding : utf-8 -*-
begin
  require 'new_relic/agent/datastores'
rescue LoadError
  fail LoadError, "Can't use NewRelic instrumentation without NewRelic gem"
end

module Cequel
  module Metal
    #
    # Provides NewRelic instrumentation for CQL queries.
    #
    module NewRelicInstrumentation
      extend ActiveSupport::Concern

      define_method :execute_with_options_with_newrelic do |statement, options|

        operation = nil
        statement_txt = nil
        statement_words = nil

        if statement.is_a?(::Cequel::Metal::Statement)
          statement_txt = statement.cql
          statement_words = statement_txt.split
          operation = statement_words.first.downcase
        elsif statement.is_a?(::Cassandra::Statements::Batch)
          operation = "batch"
          statement_txt = 'BEGIN BATCH'
        end

        callback = Proc.new do |result, scoped_metric, elapsed|
          NewRelic::Agent::Datastores.notice_statement(statement_txt, elapsed)
        end

        table = nil
        case operation
        when "batch"
          # Nothing to do
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
          execute_with_options_without_newrelic(statement, options)
        end
      end


      included do
        alias :execute_with_options_without_newrelic :execute_with_options
        alias :execute_with_options :execute_with_options_with_newrelic
      end
    end
  end
end

Cequel::Metal::Keyspace.module_eval do
  include Cequel::Metal::NewRelicInstrumentation
end
