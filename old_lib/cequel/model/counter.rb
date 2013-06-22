require 'cequel/model/readable_dictionary'

module Cequel

  module Model

    class Counter < ReadableDictionary

      def increment(columns_or_deltas, delta = 1)
        scope.increment(construct_deltas(columns_or_deltas, delta))
      end

      def decrement(columns_or_deltas, delta = 1)
        scope.decrement(construct_deltas(columns_or_deltas, delta))
      end

      private

      def construct_deltas(columns_or_deltas, delta)
        if Hash === columns_or_deltas
          columns_or_deltas
        else
          {}.tap do |deltas|
            Array.wrap(columns_or_deltas).each do |column|
              deltas[column] = delta
            end
          end
        end
      end

    end

  end

end
