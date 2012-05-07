module Cequel

  module Model

    class Scope < BasicObject

      include ::Enumerable

      def initialize(clazz, data_sets)
        @clazz, @data_sets = clazz, data_sets
      end

      def each(&block)
        if block
          each_row do |row|
            result = hydrate(row)
            yield result if result
          end
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
          row = hydrate(data_set.first)
          return row if row
        end
        nil
      end

      def count
        if restriction_columns == [@clazz.key_alias]
          ::Kernel.raise ::Cequel::Model::InvalidQuery,
            "Meaningless to perform count with key row restrictions"
        end
        @data_sets.inject(0) { |count, data_set| count + data_set.count }
      end

      def size
        count
      end

      def length
        to_a.length
      end

      def update_all(changes)
        if @data_sets.length == 1
          if @data_sets.first.row_specifications.length == 0
            return @data_sets.first.update(changes)
          end
        end
        keys = keys()
        unless keys.empty?
          @clazz.column_family.where(key_alias => keys).update(changes)
        end
      end

      def destroy_all
        each { |instance| instance.destroy }
      end

      def delete_all
        if @data_sets.length == 1
          if @data_sets.first.row_specifications.length == 0
            return @data_sets.first.truncate
          end
        end
        keys = keys()
        if keys.empty?
          @data_sets.each { |data_set| data_set.delete }
        else
          @clazz.column_family.where(key_alias => keys).delete
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
        else empty?
        end
      end

      def empty?
        count == 0
      end

      def one?(&block)
        if block then super
        else count == 1
        end
      end

      def keys
        key_alias = @clazz.key_alias
        [].tap do |keys|
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
        end
      end

      def inspect
        to_a.inspect
      end

      def ==(other)
        to_a == other.to_a
      end

      def select(*rows, &block)
        if block then super
        else scoped { |data_set| data_set.select(*rows) }.validate!
        end
      end

      def select!(*rows)
        scoped { |data_set| data_set.select!(*rows) }.validate!
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
        columns = restriction_columns
        key_column = restriction_columns.include?(@clazz.key_alias)
        non_key_column = restriction_columns.any? do |column|
          column != @clazz.key_alias
        end
        if key_column
          if non_key_column
            ::Kernel.raise InvalidQuery,
              "Can't select by key and non-key columns in the same query"
          elsif key_only_select?
            ::Kernel.raise InvalidQuery,
              "Meaningless to select only key column with key row specification"
          end
        end
        self
      end

      def where_column_equals(column, value)
        if [] == value
          Scope.new(@clazz, [])
        elsif column.to_sym != @clazz.key_alias && ::Array === value
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

      private

      def key_only_select?
        key_only_select = @data_sets.all? do |data_set|
          data_set.select_columns == [@clazz.key_alias]
        end
      end

      def restriction_columns
        [].tap do |columns|
          @data_sets.each do |data_set|
            data_set.row_specifications.each do |specification|
              if specification.respond_to?(:column)
                columns << specification.column
              end
            end
          end
        end
      end

      def hydrate(row)
        return if row.nil?
        key_alias = @clazz.key_alias.to_s
        key_alias = key_alias.upcase if key_alias =~ /^key$/i
        if row.keys.any? && (key_only_select? || row.keys != [key_alias])
          @clazz._hydrate(row)
        end
      end

    end
    
  end

end
