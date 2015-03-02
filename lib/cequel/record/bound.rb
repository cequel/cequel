# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # An upper or lower bound for a range query.
    #
    # @abstract Subclasses must implement the `to_cql` method, and may override
    #   the `operator` and `bind_value` methods.
    #
    # @api private
    # @since 1.0.0
    #
    class Bound
      # @return [Schema::Column] column bound applies to
      attr_reader :column
      # @return value for bound
      attr_reader :value

      #
      # Create a bound object for the given column. This method returns an
      # instance of the appropriate `Bound` subclass given the type of the
      # column and the class of the value.
      #
      # @param (see #initialize)
      # @return [Bound] instance of appropriate bound implementation
      #
      def self.create(column, gt, inclusive, value)
        implementation =
          if column.partition_key?
            PartitionKeyBound
          elsif column.type?(:timeuuid) && !Cequel.uuid?(value)
            TimeuuidBound
          else
            ClusteringColumnBound
          end

        implementation.new(column, gt, inclusive, value)
      end

      #
      # @param column [Schema::Column] column bound applies to
      # @param gt [Boolean] `true` if this is a lower bound
      # @param inclusive [Boolean] `true` if this is an inclusive bound
      # @param value value for bound
      #
      def initialize(column, gt, inclusive, value)
        @column, @gt, @inclusive, @value = column, gt, inclusive, value
      end

      #
      # @return [Array] pair containing CQL string and bind value
      #
      def to_cql_with_bind_variables
        [to_cql, bind_value]
      end

      #
      # @return [Boolean] `true` if this is a lower bound
      #
      def gt?
        !!@gt
      end

      #
      # @return [Boolean] `true` if this is an upper bound
      #
      def lt?
        !gt?
      end

      #
      # @return [Boolean] `true` if this is an inclusive bound
      #
      def inclusive?
        !!@inclusive
      end

      #
      # @return [Boolean] `true` if this is an exclusive bound
      #
      def exclusive?
        !inclusive?
      end

      protected

      def bind_value
        column.cast(value)
      end

      def operator
        exclusive? ? base_operator : "#{base_operator}="
      end

      def base_operator
        lt? ? '<' : '>'
      end
    end

    #
    # A bound on a partition key.
    #
    # @api private
    # @since 1.0.0
    #
    class PartitionKeyBound < Bound
      protected

      def to_cql
        %(TOKEN("#{column.name}") #{operator} TOKEN(?))
      end
    end

    #
    # A bound on a clustering column.
    #
    # @api private
    # @since 1.0.0
    #
    class ClusteringColumnBound < Bound
      protected

      def to_cql
        %("#{column.name}" #{operator} ?)
      end
    end

    #
    # A bound on a column of type `timeuuid` whose bound value is a `timestamp`
    #
    # @api private
    # @since 1.0.0
    #
    class TimeuuidBound < ClusteringColumnBound
      protected

      def to_cql
        %("#{column.name}" #{operator} #{function}(?))
      end

      def operator
        base_operator
      end

      def bind_value
        cast_value = Type::Timestamp.instance.cast(value)
        if inclusive?
          lt? ? cast_value + 0.001 : cast_value - 0.001
        else
          cast_value
        end
      end

      def function
        lt? ^ exclusive? ? 'maxTimeuuid' : 'minTimeuuid'
      end
    end
  end
end
