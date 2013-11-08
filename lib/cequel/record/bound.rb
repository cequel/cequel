module Cequel

  module Record

    class Bound
      attr_reader :column, :value

      def self.create(column, gt, inclusive, value)
        implementation =
          if column.partition_key?
            PartitionKeyBound
          elsif column.type?(Type::Timeuuid) && !value.is_a?(CassandraCQL::UUID)
            TimeuuidBound
          else
            ClusteringColumnBound
          end

        implementation.new(column, gt, inclusive, value)
      end

      def initialize(column, gt, inclusive, value)
        @column, @gt, @inclusive, @value = column, gt, inclusive, value
      end

      def to_cql_with_bind_variables
        [to_cql, bind_value]
      end


      def gt?
        !!@gt
      end

      def lt?
        !gt?
      end

      def inclusive?
        !!@inclusive
      end

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

    class PartitionKeyBound < Bound
      def to_cql
        "TOKEN(#{column.name}) #{operator} TOKEN(?)"
      end
    end

    class ClusteringColumnBound < Bound
      def to_cql
        "#{column.name} #{operator} ?"
      end
    end

    class TimeuuidBound < Bound
      def to_cql
        "#{column.name} #{operator} #{function}(?)"
      end

      protected

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
