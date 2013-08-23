module Cequel

  module Metal

    #
    # Encapsulates a data set, specified as a column family and optionally
    # various query elements.
    #
    # @todo Support ALTER, CREATE, CREATE INDEX, DROP
    #
    class DataSet

      include Enumerable
      extend Forwardable

      attr_reader :keyspace, :table_name, :select_columns, :ttl_columns,
        :writetime_columns, :row_specifications, :sort_order, :row_limit

      def_delegator :keyspace, :execute, :execute_cql
      def_delegator :keyspace, :write

      #
      # @param table_name [Symbol] column family for this data set
      # @param keyspace [Keyspace] keyspace this data set's column family lives in
      #
      # @see Keyspace#[]
      #
      def initialize(table_name, keyspace)
        @table_name, @keyspace = table_name, keyspace
        @select_columns, @ttl_columns, @writetime_columns, @row_specifications,
          @sort_order = [], [], [], [], {}
      end

      #
      # Insert a row into the column family.
      #
      # @param [Hash] data column-value pairs. The first entry *must* be the key column.
      # @param [Options] options options for persisting the row
      # @option (see #generate_upsert_options)
      # @note if a enclosed in a Keyspace#batch block, this method will be executed as part of the batch.
      #
      def insert(data, options = {})
        inserter(options) { insert(data) }.execute
      end

      #
      # Update rows
      #
      # @param [Hash] data column-value pairs
      # @param [Options] options options for persisting the column data
      # @option (see #generate_upsert_options)
      # @note if a enclosed in a Keyspace#batch block, this method will be executed as part of the batch.
      #
      def update(*args, &block)
        if block
          updater(args.extract_options!, &block).execute
        else
          data = args.shift
          updater(args.extract_options!) { set(data) }.execute
        end
      end

      def increment(data, options = {})
        incrementer(options) { increment(data) }.execute
      end

      def decrement(data, options = {})
        incrementer(options) { decrement(data) }.execute
      end
      alias_method :decr, :decrement

      #
      # Prepend element(s) to a list in the row(s) matched by this data set.
      #
      # @param [Symbol] column name of list column to prepend to
      # @param [Object,Array] elements one element or an array of elements to prepend
      # @param [Options] options options for persisting the column data
      # @option (see #generate_upsert_options)
      # @note If multiple elements are passed, they will appear in the list in reverse order.
      # @note if a enclosed in a Keyspace#batch block, this method will be executed as part of the batch.
      #
      def list_prepend(column, elements, options = {})
        updater(options) { list_prepend(column, elements) }.execute
      end

      #
      # Append element(s) to a list in the row(s) matched by this data set.
      #
      # @param [Symbol] column name of list column to append to
      # @param [Object,Array] elements one element or an array of elements to append
      # @param [Options] options options for persisting the column data
      # @option (see #generate_upsert_options)
      # @note if a enclosed in a Keyspace#batch block, this method will be executed as part of the batch.
      #
      def list_append(column, elements, options = {})
        updater(options) { list_append(column, elements) }.execute
      end

      #
      # Replace a list element at a specified index with a new value
      #
      # @param [Symbol] column name of list column
      # @param [Integer] index which element to replace
      # @param [Object] value new value at this index
      # @param [Options] options options for persisting the data
      # @option (see #generate_upsert_options)
      # @note if a enclosed in a Keyspace#batch block, this method will be executed as part of the batch.
      #
      def list_replace(column, index, value, options = {})
        updater(options) { list_replace(column, index, value) }.execute
      end

      #
      # Remove all occurrences of a given value from a list column
      #
      # @param [Symbol] column name of list column
      # @param [Object] value value to remove
      # @param [Options] options for persisting the data
      # @option (see #generate_upsert_options)
      # @note if a enclosed in a Keyspace#batch block, this method will be executed as part of the batch.
      #
      def list_remove(column, value, options = {})
        updater(options) { list_remove(column, value) }.execute
      end

      #
      # Remove all occurrences of a given value from a list column
      #
      # @param [Symbol] column name of list column
      # @param [Object] position position in list to remove value from
      # @param [Options] options for persisting the data
      # @option (see #generate_upsert_options)
      # @note if a enclosed in a Keyspace#batch block, this method will be executed as part of the batch.
      #
      def list_remove_at(column, *positions)
        options = positions.extract_options!
        deleter(options) { list_remove_at(column, *positions) }.execute
      end

      #
      # Remove a given key from a map column
      #
      # @param [Symbol] column name of map column
      # @param [Object] key map key to remove
      # @param [Options] options for persisting the data
      # @option (see #generate_upsert_options)
      # @note if a enclosed in a Keyspace#batch block, this method will be executed as part of the batch.
      #
      def map_remove(column, *keys)
        options = keys.extract_options!
        deleter(options) { map_remove(column, *keys) }.execute
      end

      #
      # Add one or more elements to a set
      #
      # @param [Symbol] column name of set column
      # @param [Object,Set] value value or values to add
      # @param [Options] options for persisting the data
      # @option (see #generate_upsert_options)
      # @note if a enclosed in a Keyspace#batch block, this method will be executed as part of the batch.
      #
      def set_add(column, values, options = {})
        updater(options) { set_add(column, values) }.execute
      end

      #
      # Remove one or more elements from a set
      #
      # @param [Symbol] column name of set column
      # @param [Object,Set] value value or values to add
      # @param [Options] options for persisting the data
      # @option (see #generate_upsert_options)
      # @note if a enclosed in a Keyspace#batch block, this method will be executed as part of the batch.
      #
      def set_remove(column, value, options = {})
        updater(options) { set_remove(column, value) }.execute
      end

      #
      # Update one or more map elements
      #
      # @param [Symbol] column name of set column
      # @param [Hash] map updates
      # @param [Options] options for persisting the data
      # @option (see #generate_upsert_options)
      # @note if a enclosed in a Keyspace#batch block, this method will be executed as part of the batch.
      #
      def map_update(column, updates, options = {})
        updater(options) { map_update(column, updates) }.execute
      end

      #
      # Delete data from the column family
      #
      # @param columns zero or more columns to delete. Deletes the entire row if none specified.
      # @param options persistence options
      # @note if a enclosed in a Keyspace#batch block, this method will be executed as part of the batch.
      #
      def delete(*columns, &block)
        options = columns.extract_options!
        if block
          deleter(options, &block).execute
        elsif columns.empty?
          deleter(options) { delete_row }.execute
        else
          deleter(options) { delete_columns(*columns) }.execute
        end
      end

      #
      # Select specified columns from this data set.
      #
      # @param *columns [Symbol,Array] columns to select
      # @return [DataSet] new data set scoped to specified columns
      #
      def select(*columns)
        clone.tap do |data_set|
          data_set.select_columns.concat(columns.flatten)
        end
      end

      #
      # Return the remaining TTL for the specified columns from this data set.
      #
      # @param *columns [Symbol,Array] columns to select
      # @return [DataSet] new data set scoped to specified columns
      #
      def select_ttl(*columns)
        clone.tap do |data_set|
          data_set.ttl_columns.concat(columns.flatten)
        end
      end

      #
      # Return the write time for the specified columns in the data set
      #
      # @param *columns [Symbol,Array] columns to select
      # @return [DataSet] new data set scoped to specified columns
      #
      def select_writetime(*columns)
        clone.tap do |data_set|
          data_set.writetime_columns.concat(columns.flatten)
        end
      end

      #
      # Select specified columns from this data set, overriding chained scope.
      #
      # @param *columns [Symbol,Array] columns to select
      # @return [DataSet] new data set scoped to specified columns
      #
      def select!(*columns)
        clone.tap do |data_set|
          data_set.select_columns.replace(columns.flatten)
        end
      end

      #
      # Add a row_specification to this data set
      #
      # @param row_specification [Hash, String] row_specification statement
      # @param *bind_vars bind variables, only if using a CQL string row_specification
      # @return [DataSet] new data set scoped to this row_specification
      # @example Using a simple hash
      #   DB[:posts].where(:title => 'Hey')
      # @example Using a CQL string
      #   DB[:posts].where("title = 'Hey'")
      # @example Using a CQL string with bind variables
      #   DB[:posts].where('title = ?', 'Hey')
      # @example Use another data set as an input -- inner data set must return a single column per row!
      #   DB[:blogs].where(:id => DB[:posts].select(:blog_id).where(:title => 'Hey'))
      #
      def where(row_specification, *bind_vars)
        clone.tap do |data_set|
          data_set.row_specifications.
            concat(build_row_specifications(row_specification, bind_vars))
        end
      end

      def where!(row_specification, *bind_vars)
        clone.tap do |data_set|
          data_set.row_specifications.
            replace(build_row_specifications(row_specification, bind_vars))
        end
      end

      #
      # Limit the number of rows returned by this data set
      #
      # @param limit [Integer] maximum number of rows to return
      # @return [DataSet] new data set scoped with given limit
      #
      def limit(limit)
        clone.tap { |data_set| data_set.row_limit = limit }
      end

      #
      # Control how the result rows are sorted. Note that you can only sort by
      # clustering keys, and in the case of multiple clustering keys you can only
      # sort by the schema's clustering order or the reverse of the clustering
      # order for all keys.
      #
      def order(pairs)
        clone.tap do |data_set|
          data_set.sort_order.merge!(pairs.symbolize_keys)
        end
      end

      #
      # Enumerate over rows in this data set. Along with #each, all other
      # Enumerable methods are implemented.
      #
      # @yield [Hash] result rows
      # @return [Enumerator] enumerator for rows, if no block given
      #
      def each
        if block_given?
          begin
            keyspace.execute(*cql).fetch do |row|
              yield Row.from_result_row(row)
            end
          rescue EmptySubquery
            # Noop -- yield no results
          end
        else
          enum_for(:each)
        end
      end

      #
      # @return [Hash] the first row in this data set
      #
      def first
        row = keyspace.execute(*limit(1).cql).fetch_row
        Row.from_result_row(row)
      rescue EmptySubquery
        nil
      end

      #
      # @return [Fixnum] the number of rows in this data set
      #
      def count
        keyspace.execute(*count_cql).fetch_row['count']
      rescue EmptySubquery
        0
      end

      #
      # @return [String] CQL select statement encoding this data set's scope.
      #
      def cql
        statement = Statement.new.
          append(select_cql).
          append(" FROM #{table_name}").
          append(*row_specifications_cql).
          append(sort_order_cql).
          append(limit_cql).
          args
      end

      #
      # @return [String] CQL statement to get count of rows in this data set
      #
      def count_cql
        Statement.new.
          append("SELECT COUNT(*) FROM #{table_name}").
          append(*row_specifications_cql).
          append(limit_cql).args
      end

      def inspect
        "#<#{self.class.name}: #{CassandraCQL::Statement.sanitize(cql.first, cql[1..-1])}>"
      end

      def ==(other)
        cql == other.cql
      end

      def inserter(options = {}, &block)
        Inserter.new(self, options, &block)
      end

      def updater(options = {}, &block)
        Updater.new(self, options, &block)
      end

      def incrementer(options = {}, &block)
        Incrementer.new(self, options, &block)
      end

      def deleter(options = {}, &block)
        Deleter.new(self, options, &block)
      end

      def row_specifications_cql
        if row_specifications.any?
          cql_fragments, bind_vars = [], []
          row_specifications.each do |spec|
            cql_with_vars = spec.cql
            cql_fragments << cql_with_vars.shift
            bind_vars.concat(cql_with_vars)
          end
          [" WHERE #{cql_fragments.join(' AND ')}", *bind_vars]
        else ['']
        end
      end

      protected
      attr_writer :row_limit

      private

      def initialize_copy(source)
        super
        @select_columns = source.select_columns.clone
        @ttl_columns = source.ttl_columns.clone
        @writetime_columns = source.writetime_columns.clone
        @row_specifications = source.row_specifications.clone
        @sort_order = source.sort_order.clone
      end

      def select_cql
        all_columns = select_columns +
          ttl_columns.map { |column| "TTL(#{column})" } +
          writetime_columns.map { |column| "WRITETIME(#{column})" }

          if all_columns.any?
            "SELECT #{all_columns.join(',')}"
          else
            'SELECT *'
          end
      end

      def limit_cql
        row_limit ? " LIMIT #{row_limit}" : ''
      end

      def sort_order_cql
        if sort_order.any?
          order = sort_order.
            map { |column, direction| "#{column} #{direction.to_s.upcase}" }.
            join(', ')
          " ORDER BY #{order}"
        end
      end

      def build_row_specifications(row_specification, bind_vars)
        case row_specification
        when Hash then RowSpecification.build(row_specification)
        when String then CqlRowSpecification.build(row_specification, bind_vars)
        else raise ArgumentError, "Invalid argument #{row_specification.inspect}; expected Hash or String"
        end
      end

    end

  end

end
