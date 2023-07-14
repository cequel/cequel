module Cequel
  module Metal
    #
    # Provides Datadog instrumentation for CQL queries.
    #
    module DatadogInstrumentation
      extend ActiveSupport::Concern

      define_method :execute_with_options_with_datadog do |statement, options|

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

        Datadog::Tracing.trace("cassandra.query", service: "cassandra") do |span, trace|
          span.resource = statement_txt
          span.span_type = "cequel"
          span.set_tag("component", "cassandra")
          span.set_tag("operation", "query")
          span.set_tag("span.kind", "client")
          span.set_tag("cassandra.table", table)
          span.set_tag("cassandra.operation", operation)
          execute_with_options_without_datadog(statement, options)
        end
      end

      def self.instrument!
        Cequel::Metal::Keyspace.module_eval do
          include Cequel::Metal::DatadogInstrumentation
        end
      end


      included do
        alias :execute_with_options_without_datadog :execute_with_options
        alias :execute_with_options :execute_with_options_with_datadog
      end
    end
  end
end
