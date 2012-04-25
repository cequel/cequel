module Cequel

  module Model

    class Scope < BasicObject

      include ::Enumerable

      def initialize(clazz, data_sets)
        @clazz, @data_sets = clazz, data_sets
      end

      def each(&block)
        if block
          each_row { |row| yield @clazz._hydrate(row) }
        else
          ::Enumerator.new(self, :each)
        end
      end

      def each_row(&block)
        if block
          @data_sets.each do |data_set|
            data_set.each(&block)
          end
        else
          ::Enumerator.new(self, :each_row)
        end
      end

      def first
        @data_sets.each do |data_set|
          row = data_set.first
          return @clazz._hydrate(row) if row
        end
        nil
      end

      def count
        @data_sets.inject(0) { |count, data_set| count + data_set.count }
      end

      def update_all(changes)
        if @data_sets.length == 1
          if @data_sets.first.row_specifications.length == 0
            return @data_sets.first.update(changes)
          end
        end
        key_alias = @clazz.key_alias
        keys = []
        @data_sets.each do |data_set|
          if data_set.row_specifications.length == 1
            specification = data_set.row_specifications.first
            if specification.respond_to?(:column)
              if specification.column == key_alias
                keys.concat(::Kernel.Array(specification.value))
                next
              end
            end
          end
          data_set.select!(key_alias).each { |row| keys << row[key_alias] }
        end
        unless keys.empty?
          @clazz.column_family.where(key_alias => keys).update(changes)
        end
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
        else scoped { |data_set| data_set.select(*rows) }
        end
      end

      def select!(*rows)
        scoped { |data_set| data_set.select!(*rows) }
      end

      def consistency(consistency)
        scoped { |data_set| data_set.consistency(consistency) }
      end

      def where(*row_specification)
        if row_specification.length == 1 && ::Hash === row_specification.first
          row_specification.first.each_pair.inject(self) do |scope, (column, value)|
            scope.where_column_equals(column, value)
          end
        else
          scoped { |data_set| data_set.where(*row_specification) }
        end
      end

      def where!(*row_specification)
        scoped { |data_set| data_set.where!(*row_specification) }
      end

      def limit(*row_specification)
        scoped { |data_set| data_set.limit(*row_specification) }
      end

      def scoped(&block)
        new_data_sets = @data_sets.map(&block)
        Scope.new(@clazz, new_data_sets)
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

      protected

      def validate!
        key_column = false
        non_key_column = false
        @data_sets.each do |data_set|
          data_set.row_specifications.each do |specification|
            if specification.respond_to?(:column)
              if specification.column == @clazz.key_alias
                key_column = true
              else
                non_key_column = true
              end
            end
          end
        end
        if key_column && non_key_column
          ::Kernel.raise InvalidQuery,
            "Can't select by key and non-key columns in the same query"
        end
        self
      end

      protected

      def where_column_equals(column, value)
        if column.to_sym != @clazz.key_alias && ::Array === value
          new_data_sets = []
          @data_sets.each do |data_set|
            value.each do |element|
              new_data_sets << data_set.where(column => element)
            end
          end
          Scope.new(@clazz, new_data_sets)
        else
          scoped { |data_set| data_set.where(column => value) }
        end.validate!
      end

    end
    
  end

end
