require 'forwardable'

module Cequel
  module Metal
    #
    # Encapsulates a data set, specified as a table and optionally
    # various query elements.
    #
    # @example Data set representing entire contents of a table
    #   data_set = database[:posts]
    #
    # @example Data set limiting rows returned
    #   data_set = database[:posts].limit(10)
    #
    # @example Data set targeting only one partition
    #   data_set = database[:posts].where(blog_subdomain: 'cassandra')
    #
    # @see http://www.datastax.com/documentation/cql/3.0/webhelp/index.html#cql/cql_reference/select_r.html CQL documentation for SELECT
    #
    class DataSet
      include Enumerable
      extend Forwardable

      # @return [Keyspace] keyspace that this data set's table resides in
      attr_reader :keyspace
      # @return [Symbol] name of the table that this data set retrieves data
      #   from
      attr_reader :table_name
      # @return [Array<Symbol>] columns that this data set restricts result rows
      #   to; empty if none
      attr_reader :select_columns
      # @return [Array<Symbol>] columns that this data set will select the TTLs
      #   of
      attr_reader :ttl_columns
      # @return [Array<Symbol>] columns that this data set will select the
      #   writetimes of
      attr_reader :writetime_columns
      # @return [Array<RowSpecification>] row specifications limiting the result
      #   rows returned by this data set
      attr_reader :row_specifications
      # @return [Hash<Symbol,Symbol>] map of column names to sort directions
      attr_reader :sort_order
      # @return [Integer] maximum number of rows to return, `nil` if no limit
      attr_reader :row_limit

      def_delegator :keyspace, :execute, :execute_cql
      private :execute_cql
      def_delegator :keyspace, :write

      #
      # @param table_name [Symbol] column family for this data set
      # @param keyspace [Keyspace] keyspace this data set's table lives in
      #
      # @see Keyspace#[]
      # @api private
      #
      def initialize(table_name, keyspace)
        @table_name, @keyspace = table_name, keyspace
        @select_columns, @ttl_columns, @writetime_columns, @row_specifications,
          @sort_order = [], [], [], [], {}
      end

      #
      # Insert a row into the column family.
      #
      # @param data [Hash] column-value pairs
      # @param options [Options] options for persisting the row
      # @option (see Writer#initialize)
      # @return [void]
      #
      # @note `INSERT` statements will succeed even if a row at the specified
      #   primary key already exists. In this case, column values specified in
      #   the insert will overwrite the existing row.
      # @note If a enclosed in a Keyspace#batch block, this method will be executed as part of the batch.
      # @see http://www.datastax.com/documentation/cql/3.0/webhelp/index.html#cql/cql_reference/insert_r.html CQL documentation for INSERT
      #
      def insert(data, options = {})
        inserter(options) { insert(data) }.execute
      end

      #
      # Upsert data into one or more rows
      #
      # @overload update(column_values, options = {})
      #   Update the rows specified in the data set with new values
      #
      #   @param column_values [Hash] map of column names to new values
      #   @param options [Options] options for persisting the column data
      #   @option (see #generate_upsert_options)
      #
      #   @example
      #     posts.where(blog_subdomain: 'cassandra', permalink: 'cequel').
      #       update(title: 'Announcing Cequel 1.0')
      #
      # @overload update(options = {}, &block)
      #   Construct an update statement consisting of multiple operations
      #
      #   @param options [Options] options for persisting the data
      #   @option (see #generate_upsert_options)
      #   @yield DSL context for adding write operations
      #
      #   @see Updater
      #   @since 1.0.0
      #
      #   @example
      #     posts.where(blog_subdomain: 'cassandra', permalink: 'cequel').update do
      #       set(title: 'Announcing Cequel 1.0')
      #       list_append(categories: 'ORMs')
      #     end
      #
      # @return [void]
      #
      # @note `UPDATE` statements will succeed even if targeting a row that does
      #   not exist. In this case a new row will be created.
      # @note This statement will fail unless one or more rows are fully
      #   specified by primary key using `where`
      # @note If a enclosed in a Keyspace#batch block, this method will be executed as part of the batch.
      # @see http://www.datastax.com/documentation/cql/3.0/webhelp/index.html#cql/cql_reference/update_r.html CQL documentation for UPDATE
      #
      def update(*args, &block)
        if block
          updater(args.extract_options!, &block).execute
        else
          data = args.shift
          updater(args.extract_options!) { set(data) }.execute
        end
      end

      #
      # Increment one or more counter columns
      #
      # @param deltas [Hash<Symbol,Integer>] map of counter column names to
      #   amount by which to increment each column
      # @return [void]
      #
      # @example
      #   post_analytics.
      #     where(blog_subdomain: 'cassandra', permalink: 'cequel').
      #     increment(pageviews: 10, tweets: 2)
      #
      # @note This can only be used on counter tables
      # @since 0.5.0
      # @see #decrement
      # @see http://www.datastax.com/documentation/cql/3.0/webhelp/index.html#cql/cql_reference/../cql_using/use_counter_t.html CQL documentation for counter columns
      #
      def increment(deltas, options = {})
        incrementer(options) { increment(deltas) }.execute
      end
      alias_method :incr, :increment

      #
      # Decrement one or more counter columns
      #
      # @param deltas [Hash<Symbol,Integer>] map of counter column names to
      #   amount by which to decrement each column
      # @return [void]
      #
      # @see #increment
      # @see http://www.datastax.com/documentation/cql/3.0/webhelp/index.html#cql/cql_reference/../cql_using/use_counter_t.html CQL documentation for counter columns
      # @since 0.5.0
      #
      def decrement(deltas, options = {})
        incrementer(options) { decrement(deltas) }.execute
      end
      alias_method :decr, :decrement

      #
      # Prepend element(s) to a list in the row(s) matched by this data set.
      #
      # @param column [Symbol] name of list column to prepend to
      # @param elements [Object,Array] one element or an array of elements to
      #   prepend
      # @param options [Options] options for persisting the column data
      # @option (see Writer#initialize)
      # @return [void]
      #
      # @example
      #   posts.list_prepend(:categories, ['CQL', 'ORMs'])
      #
      # @note If multiple elements are passed, they will appear in the list in
      #   reverse order.
      # @note If a enclosed in a Keyspace#batch block, this method will be
      #   executed as part of the batch.
      # @see #list_append
      # @see #update
      #
      def list_prepend(column, elements, options = {})
        updater(options) { list_prepend(column, elements) }.execute
      end

      #
      # Append element(s) to a list in the row(s) matched by this data set.
      #
      # @param column [Symbol] name of list column to append to
      # @param elements [Object,Array] one element or an array of elements to
      #   append
      # @param options [Options] options for persisting the column data
      # @option (see Writer#initialize)
      # @return [void]
      #
      # @example
      #   posts.list_append(:categories, ['CQL', 'ORMs'])
      #
      # @note If a enclosed in a Keyspace#batch block, this method will be
      #   executed as part of the batch.
      # @see #list_append
      # @see #update
      # @since 1.0.0
      #
      def list_append(column, elements, options = {})
        updater(options) { list_append(column, elements) }.execute
      end

      #
      # Replace a list element at a specified index with a new value
      #
      # @param column [Symbol] name of list column
      # @param index [Integer] which element to replace
      # @param value [Object] new value at this index
      # @param options [Options] options for persisting the data
      # @option (see Writer#initialize)
      # @return [void]
      #
      # @example
      #   posts.list_replace(:categories, 2, 'Object-Relational Mapper')
      #
      # @note if a enclosed in a Keyspace#batch block, this method will be executed as part of the batch.
      # @see #update
      # @since 1.0.0
      #
      def list_replace(column, index, value, options = {})
        updater(options) { list_replace(column, index, value) }.execute
      end

      #
      # Remove all occurrences of a given value from a list column
      #
      # @param column [Symbol] name of list column
      # @param value [Object] value to remove
      # @param options [Options] options for persisting the data
      # @option (see Writer#initialize)
      # @return [void]
      #
      # @example
      #   posts.list_remove(:categories, 'CQL3')
      #
      # @note If enclosed in a Keyspace#batch block, this method will be
      #   executed as part of the batch.
      # @see #list_remove_at
      # @see #update
      # @since 1.0.0
      #
      def list_remove(column, value, options = {})
        updater(options) { list_remove(column, value) }.execute
      end

      #
      # @overload list_remove_at(column, *positions, options = {})
      #   Remove the value from a given position or positions in a list column
      #
      #   @param column [Symbol] name of list column
      #   @param positions [Integer] position(s) in list to remove value from
      #   @param options [Options] options for persisting the data
      #   @option (see Writer#initialize)
      #   @return [void]
      #
      #   @example
      #     posts.list_remove_at(:categories, 2)
      #
      #   @note If enclosed in a Keyspace#batch block, this method will be
      #     executed as part of the batch.
      #   @see #list_remove
      #   @see #update
      #   @since 1.0.0
      #
      def list_remove_at(column, *positions)
        options = positions.extract_options!
        deleter(options) { list_remove_at(column, *positions) }.execute
      end

      #
      # @overload map_remove(column, *keys, options = {})
      #   Remove a given key from a map column
      #
      #   @param column [Symbol] name of map column
      #   @param keys [Object] map key to remove
      #   @param options [Options] options for persisting the data
      #   @option (see Writer#initialize)
      #   @return [void]
      #
      #   @example
      #     posts.map_remove(:credits, 'editor')
      #
      #   @note If enclosed in a Keyspace#batch block, this method will be
      #     executed as part of the batch.
      #   @see #update
      #   @since 1.0.0
      #
      def map_remove(column, *keys)
        options = keys.extract_options!
        deleter(options) { map_remove(column, *keys) }.execute
      end

      #
      # Add one or more elements to a set column
      #
      # @param column [Symbol] name of set column
      # @param values [Object,Set] value or values to add
      # @param options [Options] options for persisting the data
      # @option (see Writer#initialize)
      # @return [void]
      #
      # @example
      #   posts.set_add(:tags, 'cql3')
      #
      # @note If enclosed in a Keyspace#batch block, this method will be
      #   executed as part of the batch.
      # @see #update
      # @since 1.0.0
      #
      def set_add(column, values, options = {})
        updater(options) { set_add(column, values) }.execute
      end

      #
      # Remove an element from a set
      #
      # @param column [Symbol] name of set column
      # @param value [Object] value to remove
      # @param options [Options] options for persisting the data
      # @option (see Writer#initialize)
      # @return [void]
      #
      # @example
      #   posts.set_remove(:tags, 'cql3')
      #
      # @note If enclosed in a Keyspace#batch block, this method will be
      #   executed as part of the batch.
      # @see #update
      # @since 1.0.0
      #
      def set_remove(column, value, options = {})
        updater(options) { set_remove(column, value) }.execute
      end

      #
      # Update one or more keys in a map column
      #
      # @param column [Symbol] name of set column
      # @param updates [Hash] map of map keys to new values
      # @param options [Options] options for persisting the data
      # @option (see Writer#initialize)
      # @return [void]
      #
      # @example
      #   posts.map_update(:credits, 'editor' => 34)
      #
      # @note If enclosed in a Keyspace#batch block, this method will be
      #   executed as part of the batch.
      # @see #update
      # @since 1.0.0
      #
      def map_update(column, updates, options = {})
        updater(options) { map_update(column, updates) }.execute
      end

      #
      # @overload delete(options = {})
      #   Delete one or more rows from the table
      #
      #   @param options [Options] options for persistence
      #   @option (See Writer#initialize)
      #
      #   @example
      #     posts.where(blog_subdomain: 'cassandra', permalink: 'cequel').
      #       delete
      #
      # @overload delete(*columns, options = {})
      #   Delete data from given columns in the specified rows. This is
      #   equivalent to setting columns to `NULL` in an SQL database.
      #
      #   @param columns [Symbol] columns to remove
      #   @param options [Options] options for persistence
      #   @option (see Writer#initialize)
      #
      #   @example
      #     posts.where(blog_subdomain: 'cassandra', permalink: 'cequel').
      #       delete(:body)
      #
      # @overload delete(options = {}, &block)
      #   Construct a `DELETE` statement with multiple operations (column
      #   deletions, collection element removals, etc.)
      #
      #   @param options [Options] options for persistence
      #   @option (see Writer#initialize)
      #   @yield DSL context for construction delete statement
      #
      #   @example
      #     posts.where(blog_subdomain: 'cassandra', permalink: 'cequel').delete do
      #       delete_columns :body
      #       list_remove_at :categories, 2
      #     end
      #
      #   @see Deleter
      #
      # @return [void]
      #
      # @note If enclosed in a Keyspace#batch block, this method will be
      #   executed as part of the batch.
      # @see http://www.datastax.com/documentation/cql/3.0/webhelp/index.html#cql/cql_reference/delete_r.html CQL documentation for DELETE
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
      # @param columns [Symbol] columns columns to select
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
      # @param columns [Symbol] columns to select
      # @return [DataSet] new data set scoped to specified columns
      #
      # @since 1.0.0
      #
      def select_ttl(*columns)
        clone.tap do |data_set|
          data_set.ttl_columns.concat(columns.flatten)
        end
      end

      #
      # Return the write time for the specified columns in the data set
      #
      # @param columns [Symbol] columns to select
      # @return [DataSet] new data set scoped to specified columns
      #
      # @since 1.0.0
      #
      def select_writetime(*columns)
        clone.tap do |data_set|
          data_set.writetime_columns.concat(columns.flatten)
        end
      end

      #
      # Select specified columns from this data set, overriding chained scope.
      #
      # @param columns [Symbol,Array] columns to select
      # @return [DataSet] new data set scoped to specified columns
      #
      def select!(*columns)
        clone.tap do |data_set|
          data_set.select_columns.replace(columns.flatten)
        end
      end

      #
      # Filter this data set with a row specification
      #
      # @overload where(column_values)
      #   @param column_values [Hash] Map of column name to values to match
      #
      #   @example
      #     database[:posts].where(title: 'Hey')
      #
      # @overload where(cql, *bind_vars)
      #   @param cql [String] CQL fragment representing `WHERE` statement
      #   @param bind_vars [Object] Bind variables for the CQL fragment
      #
      #   @example
      #     DB[:posts].where('title = ?', 'Hey')
      #
      # @return [DataSet] New data set scoped to the row specification
      #
      def where(row_specification, *bind_vars)
        clone.tap do |data_set|
          data_set.row_specifications
            .concat(build_row_specifications(row_specification, bind_vars))
        end
      end

      #
      # Replace existing row specifications
      #
      # @see #where
      # @return [DataSet] New data set with only row specifications given
      #
      def where!(row_specification, *bind_vars)
        clone.tap do |data_set|
          data_set.row_specifications
            .replace(build_row_specifications(row_specification, bind_vars))
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
      # Control how the result rows are sorted
      #
      # @param pairs [Hash] Map of column name to sort direction
      # @return [DataSet] new data set with the specified ordering
      #
      # @note The only valid ordering column is the first clustering column
      # @since 1.0.0
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
      # @overload each
      #   @return [Enumerator] enumerator for rows, if no block given
      #
      # @overload each(&block)
      #   @yield [Hash] result rows
      #   @return [void]
      #
      # @return [Enumerator,void]
      #
      def each
        return enum_for(:each) unless block_given?
        execute_cql(*cql).fetch { |row| yield Row.from_result_row(row) }
      end

      #
      # @return [Hash] the first row in this data set
      #
      def first
        row = execute_cql(*limit(1).cql).fetch_row
        Row.from_result_row(row)
      end

      #
      # @return [Fixnum] the number of rows in this data set
      #
      def count
        execute_cql(*count_cql).fetch_row['count']
      end

      #
      # @return [String] CQL `SELECT` statement encoding this data set's scope.
      #
      def cql
        statement = Statement.new
          .append(select_cql)
          .append(" FROM #{table_name}")
          .append(*row_specifications_cql)
          .append(sort_order_cql)
          .append(limit_cql)
          .args
      end

      #
      # @return [String] CQL statement to get count of rows in this data set
      #
      def count_cql
        Statement.new
          .append("SELECT COUNT(*) FROM #{table_name}")
          .append(*row_specifications_cql)
          .append(limit_cql).args
      end

      #
      # @return [String]
      #
      def inspect
        "#<#{self.class.name}: #{CassandraCQL::Statement.sanitize(cql.first, cql[1..-1])}>"
      end

      #
      # @return [Boolean]
      #
      def ==(other)
        cql == other.cql
      end

      # @private
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

      # @private
      def updater(options = {}, &block)
        Updater.new(self, options, &block)
      end

      # @private
      def deleter(options = {}, &block)
        Deleter.new(self, options, &block)
      end

      protected
      attr_writer :row_limit

      private

      def inserter(options = {}, &block)
        Inserter.new(self, options, &block)
      end

      def incrementer(options = {}, &block)
        Incrementer.new(self, options, &block)
      end

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
          order = sort_order
            .map { |column, direction| "#{column} #{direction.to_s.upcase}" }
            .join(', ')
          " ORDER BY #{order}"
        end
      end

      def build_row_specifications(row_specification, bind_vars)
        case row_specification
        when Hash
          RowSpecification.build(row_specification)
        when String
          CqlRowSpecification.build(row_specification, bind_vars)
        else
          fail ArgumentError,
               "Invalid argument #{row_specification.inspect}; " \
               "expected Hash or String"
        end
      end

    end

  end

end
