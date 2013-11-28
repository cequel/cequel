module Cequel

  module Record

    class RecordSet < SimpleDelegator

      extend Forwardable
      extend Cequel::Util::HashAccessors
      include Enumerable
      include BulkWrites

      def self.default_attributes
        {:scoped_key_values => [], :select_columns => []}
      end

      attr_reader :target_class
      attr_writer :unloaded_records

      def initialize(target_class, attributes = {})
        attributes = self.class.default_attributes.merge!(attributes)
        @target_class, @attributes = target_class, attributes
        super(target_class)
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
        column = target_class.reflect_on_column(column_name)
        raise IllegalQuery,
          "Can't scope by more than one indexed column in the same query" if scoped_indexed_column
        raise ArgumentError,
          "No column #{column_name} configured for #{target_class.name}" unless column
        raise ArgumentError,
          "Use the `at` method to restrict scope by primary key" unless column.data_column?
        raise ArgumentError,
          "Can't scope by non-indexed column #{column_name}" unless column.indexed?
        scoped(scoped_indexed_column: {column_name => column.cast(value)})
      end

      def at(*scoped_key_values)
        warn "`at` is deprecated. Use `[]` instead"
        scoped_key_values.
          inject(self) { |record_set, key_value| record_set[key_value] }
      end

      def [](*new_scoped_key_values)
        new_scoped_key_values =
          new_scoped_key_values.map(&method(:cast_range_key))

        new_scoped_key_values =
          new_scoped_key_values.first if new_scoped_key_values.one?

        scoped { |attributes| attributes[:scoped_key_values] <<
          new_scoped_key_values }.resolve_if_fully_specified
      end

      def find(*scoped_key_values)
        self[*scoped_key_values].load!
      end

      def /(scoped_key_value)
        self[scoped_key_value]
      end

      def after(start_key)
        scoped(lower_bound: bound(true, false, start_key))
      end

      def before(end_key)
        scoped(upper_bound: bound(false, false, end_key))
      end

      def in(range)
        scoped(
          lower_bound: bound(true, true, range.first),
          upper_bound: bound(false, !range.exclude_end?, range.last)
        )
      end

      def from(start_key)
        unless partition_specified?
          raise IllegalQuery,
            "Can't construct exclusive range on partition key #{range_key_name}"
        end
        scoped(lower_bound: bound(true, true, start_key))
      end

      def upto(end_key)
        unless partition_specified?
          raise IllegalQuery,
            "Can't construct exclusive range on partition key #{range_key_name}"
        end
        scoped(upper_bound: bound(false, true, end_key))
      end

      def reverse
        unless partition_specified?
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
        find_each_row(options) { |row| yield target_class.hydrate(row) }
      end

      def find_in_batches(options = {})
        return enum_for(:find_in_batches, options) unless block_given?
        find_rows_in_batches(options) do |rows|
          yield rows.map { |row| target_class.hydrate(row) }
        end
      end

      def find_each_row(options = {}, &block)
        return enum_for(:find_each_row, options) unless block
        find_rows_in_batches(options) { |rows| rows.each(&block) }
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
        target_class.key_columns.first(scoped_key_values.length)
      end

      def unscoped_key_columns
        target_class.key_columns.drop(scoped_key_values.length)
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

      def fully_specified?
        scoped_key_values.length == target_class.key_columns.length
      end

      def partition_specified?
        scoped_key_values.length >= target_class.partition_key_columns.length
      end

      def multiple_records_specified?
        scoped_key_values.any? { |values| values.is_a?(Array) }
      end

      def resolve_if_fully_specified
        if fully_specified?
          if multiple_records_specified?
            LazyRecordCollection.new(select_non_collection_columns!)
          else
            LazyRecordCollection.new(self).first
          end
        else
          self
        end
      end

      # Try to order results by the first clustering column. Fall back to partition key if none exist.
      def order_by_column
        target_class.clustering_columns.first.name if target_class.clustering_columns.any?
      end

      def selects_collection_columns?
        select_columns.any? do |column_name|
          target_class.reflect_on_column(column_name).
            is_a?(Cequel::Schema::CollectionColumn)
        end
      end

      def select_non_collection_columns!
        if selects_collection_columns?
          raise ArgumentError,
            "Can't scope by multiple keys when selecting a collection column."
        end
        if select_columns.empty?
          non_collection_columns = target_class.columns.
            reject { |column| column.is_a?(Cequel::Schema::CollectionColumn) }.
            map { |column| column.name }
          select(*non_collection_columns)
        else
          self
        end
      end

      private
      def_delegators :target_class, :connection
      def_delegator :range_key_column, :cast, :cast_range_key
      private :connection, :cast_range_key

      def method_missing(method, *args, &block)
        target_class.with_scope(self) { super }
      end

      def construct_data_set
        data_set = connection[target_class.table_name]
        data_set = data_set.limit(row_limit) if row_limit
        data_set = data_set.select(*select_columns) if select_columns
        if scoped_key_values
          key_conditions = Hash[scoped_key_names.zip(scoped_key_values)]
          data_set = data_set.where(key_conditions)
        end
        if lower_bound
          data_set = data_set.where(*lower_bound.to_cql_with_bind_variables)
        end
        if upper_bound
          data_set = data_set.where(*upper_bound.to_cql_with_bind_variables)
        end
        data_set = data_set.order(order_by_column => :desc) if reversed?
        data_set = data_set.where(scoped_indexed_column) if scoped_indexed_column
        data_set
      end

      def bound(gt, inclusive, value)
        Bound.create(range_key_column, gt, inclusive, value)
      end

      def cast_range_key_for_bound(value)
        if range_key_column.type?(Type::Timeuuid) && !value.is_a?(CassandraCQL::UUID)
          Type::Timestamp.instance.cast(value)
        else
          cast_range_key(value)
        end
      end

      def scoped(new_attributes = {}, &block)
        attributes_copy = Marshal.load(Marshal.dump(attributes))
        attributes_copy.merge!(new_attributes)
        attributes_copy.tap(&block) if block
        RecordSet.new(target_class, attributes_copy)
      end

      def key_attributes_for_each_row
        return enum_for(:key_attributes_for_each_row) unless block_given?
        select(*key_column_names).find_each do |record|
          yield record.key_attributes
        end
      end

    end

  end

end
