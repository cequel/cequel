require 'delegate'

module Cequel

  module Metal

    class Writer

      extend Forwardable

      def initialize(data_set, options = {}, &block)
        @data_set, @options, @block = data_set, options, block
        @statements, @bind_vars = [], []
        SimpleDelegator.new(self).instance_eval(&block) if block
      end

      def execute
        return if empty?
        statement = Statement.new
        write_to_statement(statement)
        statement.append(*data_set.row_specifications_cql)
        data_set.write(*statement.args)
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
      # @param [Hash] options options for insert
      # @option options [Symbol,String] :consistency required consistency for the write
      # @option options [Integer] :ttl time-to-live in seconds for the written data
      # @option options [Time,Integer] :timestamp the timestamp associated with the column values
      #
      def generate_upsert_options
        if options.empty?
          ''
        else
          ' USING ' <<
          options.map do |key, value|
            serialized_value =
              case key
              when :consistency then value.to_s.upcase
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
