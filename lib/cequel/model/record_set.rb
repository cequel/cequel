module Cequel

  module Model

    class RecordSet

      extend Forwardable
      include Enumerable

      Bound = Struct.new(:value, :inclusive)

      def initialize(clazz)
        @clazz = clazz
        @select_columns = []
        @scoped_key_values = []
      end

      def all
        self
      end

      def select(*columns)
        return super if block_given?
        scoped { |record_set| record_set.select_columns.concat(columns) }
      end

      def limit(count)
        scoped { |record_set| record_set.row_limit = count }
      end

      def at(*scoped_key_values)
        record_set_class = next_key_column.partition_key? ?
          RecordSet : SortableRecordSet
        scoped(record_set_class) do |record_set|
          record_set.scoped_key_values.concat(scoped_key_values)
        end
      end

      def [](scoped_key_value)
        if next_key_column
          at(scoped_key_value)
        else
          attributes = {}
          key_values = [*scoped_key_values, scoped_key_value]
          clazz.key_column_names.zip(key_values) do |key_name, key_value|
            attributes[key_name] = key_value
          end
          clazz.new_empty { @attributes = attributes }
        end
      end

      def find(*scoped_key_values)
        attributes = {}
        key_values = [*self.scoped_key_values, *scoped_key_values]
        clazz.key_column_names.zip(key_values) do |key_name, key_value|
          raise MissingAttributeError, "#{key_name} is empty" if key_value.nil?
          attributes[key_name] = key_value
        end
        (clazz.new_empty { @attributes = attributes }).load!
      end

      def /(scoped_key_value)
        at(scoped_key_value)
      end

      def after(start_key)
        scoped do |record_set|
          record_set.lower_bound = Bound.new(start_key, false)
        end
      end

      def before(end_key)
        scoped do |record_set|
          record_set.upper_bound = Bound.new(end_key, false)
        end
      end

      def in(range)
        scoped do |record_set|
          record_set.lower_bound = Bound.new(range.first, true)
          record_set.upper_bound = Bound.new(range.last, !range.exclude_end?)
        end
      end

      def first(count = nil)
        count ? limit(count).entries : limit(1).each.first
      end

      def count
        data_set.count
      end

      def each(&block)
        find_each(&block)
      end

      def find_each(options = {})
        return enum_for(:find_each, options) unless block_given?
        find_each_row(options) { |row| yield clazz.hydrate(row) }
      end

      def find_each_row(options = {}, &block)
        return enum_for(:find_each_row, options) unless block
        find_rows_in_batches(options) { |row| row.each(&block) }
      end

      def find_rows_in_batches(options = {}, &block)
        return find_rows_in_single_batch(options, &block) if row_limit
        batch_size = options.fetch(:batch_size, 1000)
        batch_record_set = base_record_set = limit(batch_size)
        more_results = true

        while more_results
          rows = batch_record_set.find_rows_in_single_batch
          yield rows if rows.any?
          more_results = rows.length == batch_size
          last_row = rows.last
          if more_results
            find_nested_batches_from(last_row, options, &block)
            batch_record_set = base_record_set.next_batch_from(last_row)
          end
        end
      end

      protected
      attr_accessor :row_limit
      attr_reader :select_columns, :scoped_key_values,
        :lower_bound, :upper_bound

      def reversed?
        false
      end

      def lower_bound=(bound)
        @lower_bound = bound
      end

      def upper_bound=(bound)
        @upper_bound = bound
      end

      def data_set
        @data_set ||= construct_data_set
      end

      def next_batch_from(row)
        reversed? ? before(row[range_key_name]) : after(row[range_key_name])
      end

      def find_nested_batches_from(row, options, &block)
        if next_key_column
          at(row[range_key_name]).
            next_batch_from(row).
            find_rows_in_batches(options, &block)
        end
      end

      def find_rows_in_single_batch(options = {})
        if options.key?(:batch_size)
          raise ArgumentError,
            "Can't pass :batch_size argument with a limit in the scope"
        else
          data_set.entries.tap do |batch|
            yield batch if batch.any? && block_given?
          end
        end
      end

      def range_key
        clazz.key_columns[scoped_key_values.length]
      end

      def range_key_name
        range_key.name
      end

      def scoped_key_columns
        clazz.key_columns.first(scoped_key_values.length)
      end

      def scoped_key_names
        scoped_key_columns.map { |column| column.name }
      end

      def chain_from(collection)
        @select_columns = collection.select_columns.dup
        @scoped_key_values = collection.scoped_key_values.dup
        @lower_bound = collection.lower_bound
        @upper_bound = collection.upper_bound
        @row_limit = collection.row_limit
        self
      end

      private
      attr_reader :clazz
      def_delegators :clazz, :connection

      def scoped(record_set_class = self.class, &block)
        record_set_class.new(clazz).chain_from(self).tap(&block)
      end

      def next_key_column
        clazz.key_columns[scoped_key_values.length + 1]
      end

      def next_key_name
        next_key_column.name if next_key_column
      end

      def construct_data_set
        data_set = connection[clazz.table_name]
        data_set = data_set.limit(row_limit) if row_limit
        data_set = data_set.select(*select_columns) if select_columns
        if scoped_key_values
          key_conditions = Hash[scoped_key_names.zip(scoped_key_values)]
          data_set = data_set.where(key_conditions)
        end
        if lower_bound
          fragment = construct_bound_fragment(lower_bound, '>')
          data_set = data_set.where(fragment, lower_bound.value)
        end
        if upper_bound
          fragment = construct_bound_fragment(upper_bound, '<')
          data_set = data_set.where(fragment, upper_bound.value)
        end
        data_set
      end

      def construct_bound_fragment(bound, base_operator)
        operator = bound.inclusive ? "#{base_operator}=" : base_operator
        "TOKEN(#{range_key_name}) #{operator} TOKEN(?)"
      end

    end

    class SortableRecordSet < RecordSet

      def initialize(clazz)
        super
        @reversed = false
      end

      def from(start_key)
        scoped do |record_set|
          record_set.lower_bound = Bound.new(start_key, true)
        end
      end

      def upto(end_key)
        scoped do |record_set|
          record_set.upper_bound = Bound.new(end_key, true)
        end
      end

      def reverse
        scoped { |scope| scope.reversed = !reversed? }
      end

      def last
        reverse.first
      end

      def chain_from(collection)
        super
        @reversed = collection.reversed?
        self
      end

      protected
      attr_writer :reversed

      def construct_data_set
        data_set = super
        data_set = data_set.order(range_key_name => :desc) if reversed?
        data_set
      end

      def reversed?
        @reversed
      end

      def construct_bound_fragment(bound, base_operator)
        operator = bound.inclusive ? "#{base_operator}=" : base_operator
        "#{range_key_name} #{operator} ?"
      end

    end

  end

end
