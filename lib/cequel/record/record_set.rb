module Cequel

  module Record

    class RecordSet < SimpleDelegator

      extend Forwardable
      extend Cequel::Util::HashAccessors
      include Enumerable

      Bound = Struct.new(:value, :inclusive)

      def self.default_attributes
        {:scoped_key_values => [], :select_columns => []}
      end

      def initialize(clazz, attributes = {})
        attributes = self.class.default_attributes.merge!(attributes)
        @clazz, @attributes = clazz, attributes
        super(clazz)
      end

      def all
        self
      end

      def select(*columns)
        return super if block_given?
        scoped { |attributes| attributes[:select_columns].concat(columns) }
      end

      def limit(count)
        scoped(:row_limit => count)
      end

      def where(column_name, value)
        column = clazz.reflect_on_column(column_name)
        raise IllegalQuery,
          "Can't scope by more than one indexed column in the same query" if scoped_indexed_column
        raise ArgumentError,
          "No column #{column_name} configured for #{clazz.name}" unless column
        raise ArgumentError,
          "Use the `at` method to restrict scope by primary key" unless column.data_column?
        raise ArgumentError,
          "Can't scope by non-indexed column #{column_name}" unless column.indexed?
        scoped(scoped_indexed_column: {column_name => column.cast(value)})
      end

      def at(*scoped_key_values)
        scoped do |attributes|
          type_cast_key_values = scoped_key_values.zip(unscoped_key_columns).
            map { |value, column| column.cast(value) }
          attributes[:scoped_key_values].concat(type_cast_key_values)
        end
      end

      def [](scoped_key_value)
        scoped_key_value = cast_range_key(scoped_key_value)

        if next_range_key_column
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
        self[*scoped_key_values].load!
      end

      def /(scoped_key_value)
        at(scoped_key_value)
      end

      def after(start_key)
        scoped(lower_bound: bound(start_key, false))
      end

      def before(end_key)
        scoped(upper_bound: bound(end_key, false))
      end

      def in(range)
        scoped(
          lower_bound: bound(range.first, true),
          upper_bound: bound(range.last, !range.exclude_end?)
        )
      end

      def from(start_key)
        unless single_partition?
          raise IllegalQuery,
            "Can't construct exclusive range on partition key #{range_key_name}"
        end
        scoped(lower_bound: bound(start_key, true))
      end

      def upto(end_key)
        unless single_partition?
          raise IllegalQuery,
            "Can't construct exclusive range on partition key #{range_key_name}"
        end
        scoped(upper_bound: bound(end_key, true))
      end

      def reverse
        unless single_partition?
          raise IllegalQuery,
            "Can't reverse without scoping to partition key #{range_key_name}"
        end
        scoped(reversed: !reversed?)
      end

      def first(count = nil)
        count ? limit(count).entries : limit(1).each.first
      end

      def last(count = nil)
        reverse.first(count).tap do |results|
          results.reverse! if count
        end
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

      def data_set
        @data_set ||= construct_data_set
      end

      def scoped_key_attributes
        Hash[scoped_key_columns.map { |col| col.name }.zip(scoped_key_values)]
      end

      def_delegators :entries, :inspect

      def ==(other)
        entries == other.to_a
      end

      protected
      attr_reader :attributes
      hattr_reader :attributes, :select_columns, :scoped_key_values, :row_limit,
        :lower_bound, :upper_bound, :scoped_indexed_column
      protected :select_columns, :scoped_key_values, :row_limit, :lower_bound,
        :upper_bound, :scoped_indexed_column
      hattr_inquirer :attributes, :reversed
      protected :reversed?

      def next_batch_from(row)
        reversed? ? before(row[range_key_name]) : after(row[range_key_name])
      end

      def find_nested_batches_from(row, options, &block)
        if next_range_key_column
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

      def scoped_key_names
        scoped_key_columns.map { |column| column.name }
      end

      def scoped_key_columns
        clazz.key_columns.first(scoped_key_values.length)
      end

      def unscoped_key_columns
        clazz.key_columns.drop(scoped_key_values.length)
      end

      def unscoped_key_names
        unscoped_key_columns.map { |column| column.name }
      end

      def range_key_column
        unscoped_key_columns.first
      end

      def range_key_name
        range_key_column.name
      end

      def next_range_key_column
        unscoped_key_columns.second
      end

      def next_range_key_name
        next_range_key_column.try(:name)
      end

      def single_partition?
        scoped_key_values.length >= clazz.partition_key_columns.length
      end

      # Try to order results by the first clustering column. Fall back to partition key if none exist.
      def order_by_column
        clazz.clustering_columns.first.name if clazz.clustering_columns.any?
      end

      private
      attr_reader :clazz
      def_delegators :clazz, :connection
      def_delegator :range_key_column, :cast, :cast_range_key
      private :connection, :cast_range_key

      def method_missing(method, *args, &block)
        clazz.with_scope(self) { super }
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
        data_set = data_set.order(order_by_column => :desc) if reversed?
        data_set = data_set.where(scoped_indexed_column) if scoped_indexed_column
        data_set
      end

      def construct_bound_fragment(bound, base_operator)
        operator = bound.inclusive ? "#{base_operator}=" : base_operator
        single_partition? ?
          "#{range_key_name} #{operator} ?" :
          "TOKEN(#{range_key_name}) #{operator} TOKEN(?)"
      end

      def bound(value, inclusive)
        Bound.new(cast_range_key(value), inclusive)
      end

      def scoped(new_attributes = {}, &block)
        attributes_copy = Marshal.load(Marshal.dump(attributes))
        attributes_copy.merge!(new_attributes)
        attributes_copy.tap(&block) if block
        RecordSet.new(clazz, attributes_copy)
      end

    end

  end

end
