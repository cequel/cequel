module Cequel

  module Model

    class Scope < BasicObject

      include ::Enumerable

      def initialize(clazz, data_set)
        @clazz, @data_set = clazz, data_set
      end

      def each(&block)
        if block
          @data_set.each do |row|
            result = @clazz._hydrate(row)
            yield result if result
          end
        else
          ::Enumerator.new(self, :each)
        end
      end

      def first
        row = @data_set.first
        @clazz._hydrate(row) if row
      end

      def count
        @data_set.count
      end

      def update_all(changes)
        key_alias = @clazz.key_alias
        if @data_set.row_specifications.length == 0
          return @data_set.update(changes)
        end
        if @data_set.row_specifications.length == 1
          specification = @data_set.row_specifications.first
          if specification.respond_to?(:column)
            if specification.column == key_alias
              return @data_set.update(changes)
            end
          end
        end
        @clazz.where(key_alias => @data_set.select(key_alias)).update_all(changes)
      end

      def find(*keys, &block)
        if block then super
        else with_scope(self) { @clazz.find(*keys) }
        end
      end

      def any?(&block)
        if block then super
        else count > 0
        end
      end

      def none?(&block)
        if block then super
        else count == 0
        end
      end

      def one?(&block)
        if block then super
        else count == 1
        end
      end

      def ==(other)
        to_a == other.to_a
      end

      def select(*rows, &block)
        if block then super
        else scoped(@data_set.select(*rows))
        end
      end

      def consistency(consistency)
        scoped(@data_set.consistency(consistency))
      end

      def where(*row_specification)
        scoped(@data_set.where(*row_specification))
      end

      def limit(*row_specification)
        scoped(@data_set.limit(*row_specification))
      end

      def scoped(data_set)
        Scope.new(@clazz, data_set)
      end

      def nil?
        false # for ActiveSupport delegation
      end

      def method_missing(method, *args, &block)
        if @clazz.respond_to?(method)
          @clazz.with_scope(self) do
            @clazz.__send__(method, *args, &block)
          end
        else
          super
        end
      end

    end
    
  end

end
