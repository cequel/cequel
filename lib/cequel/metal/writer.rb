# -*- encoding : utf-8 -*-
module Cequel
  module Metal
    #
    # Internal representation of a data manipulation statement
    #
    # @abstract Subclasses must implement #write_to_statement, which writes
    #   internal state to a Statement instance
    #
    # @since 1.0.0
    # @api private
    #
    class Writer
      extend Util::Forwardable

      #
      # @param data_set [DataSet] data set to write to
      #
      def initialize(data_set, &block)
        @data_set, @options, @block = data_set, options, block
        @statements, @bind_vars = [], []
        SimpleDelegator.new(self).instance_eval(&block) if block
      end

      #
      # Execute the statement as a write operation
      #
      # @param options [Options] options
      # @option options [Symbol] :consistency what consistency level to use for
      #   the operation
      # @option options [Integer] :ttl time-to-live in seconds for the written
      #   data
      # @option options [Time,Integer] :timestamp the timestamp associated with
      #   the column values
      # @return [void]
      #
      def execute(options = {})
        options.assert_valid_keys(:timestamp, :ttl, :consistency)
        return if empty?
        statement = Statement.new
        consistency = options.fetch(:consistency, data_set.query_consistency)
        write_to_statement(statement, options)
        statement.append(*data_set.row_specifications_cql)
        data_set.write_with_consistency(
          statement.cql, statement.bind_vars, consistency)
      end

      private

      attr_reader :data_set, :options, :statements, :bind_vars
      def_delegator :data_set, :table_name
      def_delegator :statements, :empty?

      def prepare_upsert_value(value)
        case value
        when ::Array
          yield '[?]', value
        when ::Set then
          yield '{?}', value.to_a
        when ::Hash then
          binding_pairs = ::Array.new(value.length) { '?:?' }.join(',')
          yield "{#{binding_pairs}}", *value.flatten
        else
          yield '?', value
        end
      end

      #
      # Generate CQL option statement for inserts and updates
      #
      def generate_upsert_options(options)
        upsert_options = options.slice(:timestamp, :ttl)
        if upsert_options.empty?
          ''
        else
          ' USING ' <<
          upsert_options.map do |key, value|
            serialized_value =
              case key
              when :timestamp then (value.to_f * 1_000_000).to_i
              else value
              end
            "#{key.to_s.upcase} #{serialized_value}"
          end.join(' AND ')
        end
      end
    end
  end
end
