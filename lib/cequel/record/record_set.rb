module Cequel
  module Record
    #
    # This class represents a subset of records from a particular table. Record
    # sets encapsulate a CQL query, and are constructed using a chained builder
    # interface.
    #
    # The primary mechanism for specifying which rows should be returned by a
    # CQL query is by specifying values for one or more primary key columns. A
    # record set acts like a deeply-nested hash, where each primary key column
    # is a level of nesting. The {#[]} method is used to narrow the result set
    # by successive primary key values.
    #
    # If {#[]} is used successively to specify all of the columns of a primary
    # key, the result will be a single {Record} or a {LazyRecordCollection},
    # depending on whether multiple values were specified for one of the key
    # columns. In either case, the record instances will be unloaded.
    #
    # Certain methods have behavior that is dependent on which primary keys have
    # been specified using {#[]}. In many methods, such as {#[]}, {#values_at},
    # {#before}, {#after}, {#from}, {#upto}, and {#in}, the *first unscoped
    # primary key column* serves as implicit context for the method: the value
    # passed to those methods is an exact or bounding value for that column.
    #
    # CQL does not allow ordering by arbitrary columns; the ordering of a table
    # is determined by its clustering column(s). You read records in reverse
    # clustering order using {#reverse}.
    #
    # Record sets are enumerable collections; under the hood, results are
    # paginated. This pagination can be made explicit using {#find_in_batches}.
    # RecordSets do not store their records in memory; each time {#each} or an
    # `Enumerable` method is called, the database is queried.
    #
    # All `RecordSet` methods are also exposed directly on {Record}
    # classes. So, for instance, `Post.limit(10)` or `Post.select(:id, :title)`
    # work as expected.
    #
    # Conversely, you may call any class method of a record class on a record
    # set that targets that class. The class method will be executed in the
    # context of the record set that the method is called on. See below for
    # examples.
    #
    # @example Model class used for further examples
    #   class Post
    #     include Cequel::Record
    #
    #     belongs_to :blog # defines key :blog_subdomain
    #     key :id, :timeuuid, auto: true
    #
    #     column :title, :text
    #     column :author_id, :integer, index: true
    #
    #     def self.for_author(author)
    #       where(:author_id, author.id)
    #     end
    #   end
    #
    # @example A record set scoped to all posts
    #   Post.all # returns a record set with no scope restrictions
    #
    # @example The first ten posts
    #   # returns a ten-element array of loaded posts
    #   Post.first(10)
    #
    #   # returns a record set scoped to yield the first 10 posts
    #   Post.limit(10)
    #
    # @example The posts in the "cassandra" blog
    #   # returns a record set where blog_subdomain = "cassandra"
    #   Post['cassandra']
    #
    # @example The post in the "cassandra" blog with id `params[:id]`
    #   # returns an unloaded Post instance
    #   Post['cassandra'][params[:id]]
    #
    # @example The posts in the "cassandra" blog with ids `id1, id2`
    #   # returns a LazyRecordCollection containing two unloaded Post instances
    #   Post['cassandra'].values_at('id1', 'id2')
    #
    # @example The posts in the "cassandra" blog in descending order of id
    #   # returns a LazyRecordCollection where blog_subdomain="cassandra" in
    #   # descending order of creation
    #   Post['cassandra'].reverse
    #
    # @example The posts in the "cassandra" blog created in the last week
    #   # returns a LazyRecordCollection where blog_subdomain="cassandra" and
    #   the timestamp encoded in the uuid is in the last week. This only works
    #   for timeuuid clustering columns
    #   Post['cassandra'].reverse.after(1.week.ago)
    #
    # @example 10 posts by a given author
    #   # Scoped to 10 posts where author_id=author.id. Results will not be in a
    #   # defined order because the partition key is not specified
    #   Post.for_author(author).limit(10)
    #
    # @see Scoped
    # @see LazyRecordCollection
    # @since 1.0.0
    #
    class RecordSet < SimpleDelegator
      extend Forwardable
      extend Cequel::Util::HashAccessors
      include Enumerable
      include BulkWrites

      # @private
      def self.default_attributes
        {:scoped_key_values => [], :select_columns => []}
      end

      # @return [Class] the Record class that this collection yields instances
      #   of
      attr_reader :target_class

      #
      # @param target_class [Class] the Record class that this collection yields
      #   instances of
      # @param attributes [Hash] initial scoping attributes
      #
      # @api private
      #
      def initialize(target_class, attributes = {})
        attributes = self.class.default_attributes.merge!(attributes)
        @target_class, @attributes = target_class, attributes
        super(target_class)
      end

      #
      # @return [RecordSet] self
      #
      def all
        self
      end

      #
      # @overload select
      #
      #   @yieldparam record [Record] each record in the record set
      #   @return [Array] records that pass the test given by the block
      #
      #   @see
      #     http://ruby-doc.org/core-2.0.0/Enumerable.html#method-i-select
      #     Enumerable#select
      #
      # @overload select(*columns)
      #   Restrict which columns are selected when records are retrieved from
      #   the database
      #
      #   @param columns [Symbol] column names
      #   @return [RecordSet] record set with the given column selections
      #     applied
      #
      #   @see
      #     http://www.datastax.com/documentation/cql/3.0/webhelp/index.html#cql/cql_reference/select_r.html
      #     CQL SELECT documentation
      #
      # @return [Array,RecordSet]
      #
      def select(*columns)
        return super if block_given?
        scoped { |attributes| attributes[:select_columns].concat(columns) }
      end

      #
      # Restrict the number of records that the RecordSet can contain.
      #
      # @param count [Integer] the maximum number of records to return
      # @return [RecordSet] record set with limit applied
      #
      # @see
      #   http://www.datastax.com/documentation/cql/3.0/webhelp/index.html#cql/cql_reference/select_r.html
      #   CQL SELECT documentation
      #
      def limit(count)
        scoped(:row_limit => count)
      end

      #
      # Filter the record set to records containing a given value in an indexed
      # column
      #
      # @param column_name [Symbol] column for filter
      # @param value value to match in given column
      # @return [RecordSet] record set with filter applied
      # @raise [IllegalQuery] if this record set is already filtered by an indexed
      #   column
      # @raise [ArgumentError] if the specified column is not an data column
      #   with a secondary index
      #
      # @note This should only be used with data columns that have secondary
      #   indexes. To filter a record set using a primary key, use {#[]}
      # @note Only one secondary index filter can be used in a given query
      # @note Secondary index filters cannot be mixed with primary key filters
      #
      def where(column_name, value)
        column = target_class.reflect_on_column(column_name)
        if scoped_indexed_column
          fail IllegalQuery,
               "Can't scope by more than one indexed column in the same query"
        end
        unless column
          fail ArgumentError,
               "No column #{column_name} configured for #{target_class.name}"
        end
        unless column.data_column?
          fail ArgumentError,
               "Use the `at` method to restrict scope by primary key"
        end
        unless column.indexed?
          fail ArgumentError,
               "Can't scope by non-indexed column #{column_name}"
        end
        scoped(scoped_indexed_column: {column_name => column.cast(value)})
      end

      #
      # @deprecated Use {#[]} instead
      #
      # Scope to values for one or more primary key columns
      #
      # @param scoped_key_values values for primary key columns
      # @return (see #[])
      #
      def at(*scoped_key_values)
        warn "`at` is deprecated. Use `[]` instead"
        scoped_key_values
          .reduce(self) { |record_set, key_value| record_set[key_value] }
      end

      #
      # Restrict this record set to a given value for the next unscoped
      # primary key column
      #
      # Record sets can be thought of like deeply-nested hashes, where each
      # primary key column is a level of nesting. For instance, if a table
      # consists of a single record with primary key `(blog_subdomain,
      # permalink) = ("cassandra", "cequel")`, the record set can be thought of
      # like so:
      #
      # ```ruby
      # {
      #   "cassandra" => {
      #     "cequel" => #<Post blog_subdomain: "cassandra", permalink: "cequel", title: "Cequel">
      #   }
      # }
      # ```
      #
      # If `[]` is invoked enough times to specify all primary keys, then an
      # unloaded `Record` instance is returned; this is the same behavior you
      # would expect from a `Hash`. If only some subset of the primary keys have
      # been specified, the result is still a `RecordSet`.
      #
      # @param primary_key_value value for the first unscoped primary key
      # @return [RecordSet] record set with primary key filter applied, if not
      #   all primary keys are specified
      # @return [Record] unloaded record, if all primary keys are specified
      #
      # @example Partially specified primary key
      #   Post['cequel'] # returns a RecordSet
      #
      # @example Fully specified primary key
      #   Post['cequel']['cassandra'] # returns an unloaded Record
      #
      # @note Accepting multiple values is deprecated behavior. Use {#values_at}
      #   instead.
      #
      def [](*primary_key_value)
        if primary_key_value.many?
          warn "Calling #[] with multiple arguments is deprecated. Use #values_at"
          return values_at(*primary_key_value)
        end

        primary_key_value = cast_range_key(primary_key_value.first)

        scope_and_resolve do |attributes|
          attributes[:scoped_key_values] << primary_key_value
        end
      end
      alias_method :/, :[]

      #
      # Restrict the records in this record set to those containing any of a set
      # of values
      #
      # @param primary_key_values values to match in the next unscoped primary
      #   key
      # @return [RecordSet] record set with primary key scope applied if not all
      #   primary key columns are specified
      # @return [LazyRecordCollection] collection of unloaded records if all
      #   primary key columns are specified
      # @raise IllegalQuery if the scoped key column is neither the last
      #   partition key column nor the last clustering column
      #
      # @see #[]
      #
      def values_at(*primary_key_values)
        unless next_unscoped_key_column_valid_for_in_query?
          fail IllegalQuery,
               "Only the last partition key column and the last clustering " \
               "column can match multiple values"
        end

        primary_key_values = primary_key_values.map(&method(:cast_range_key))

        scope_and_resolve do |attributes|
          attributes[:scoped_key_values] << primary_key_values
        end
      end

      #
      # Return a loaded Record or collection of loaded Records with the
      # specified primary key values
      #
      # @param scoped_key_values one or more values for the final primary key
      #   column
      # @return [Record] if a single key is specified, return the loaded
      #   record at that key
      # @return [LazyRecordCollection] if multiple keys are specified, return a
      #   collection of loaded records at those keys
      # @raise [RecordNotFound] if not all the keys correspond to records in the
      #   table
      # @raise [ArgumentError] if not all primary key columns have been
      #   specified
      #
      # @note This should only be called when all but the last column in the
      #   primary key is already specified in this record set
      def find(*scoped_key_values)
        (scoped_key_values.one? ?
          self[scoped_key_values.first] :
          values_at(*scoped_key_values)).load!
      end

      #
      # Restrict records to ones whose value in the first unscoped primary key
      # column are strictly greater than the given start_key.
      #
      # @param start_key the exclusive lower bound for the key column
      # @return [RecordSet] record set with lower bound applied
      #
      # @see #from
      #
      def after(start_key)
        scoped(lower_bound: bound(true, false, start_key))
      end

      #
      # Restrict records to ones whose value in the first unscoped primary key
      # column are strictly less than the given end_key.
      #
      # @param end_key the exclusive upper bound for the key column
      # @return [RecordSet] record set with upper bound applied
      #
      # @see #upto
      #
      def before(end_key)
        scoped(upper_bound: bound(false, false, end_key))
      end

      #
      # Restrict records to those whose value in the first unscoped primary key
      # column are in the given range. Will accept both inclusive ranges (`1..5`)
      # and end-exclusive ranges (`1...5`). If you need a range with an
      # exclusive start value, use {#after}, which can be combined with
      # {#before} or {#from} to create a range.
      #
      # @param range [Range] range of values for the key column
      # @return [RecordSet] record set with range restriction applied
      #
      # @see #after
      # @see #before
      # @see #from
      # @see #upto
      #
      def in(range)
        scoped(
          lower_bound: bound(true, true, range.first),
          upper_bound: bound(false, !range.exclude_end?, range.last)
        )
      end

      #
      # Restrict records to those whose value in the first unscoped primary key
      # column are greater than or equal to the given start key.
      #
      # @param start_key the inclusive lower bound for values in the key column
      # @return [RecordSet] record set with the lower bound applied
      #
      # @see #after
      #
      def from(start_key)
        unless partition_specified?
          fail IllegalQuery,
               "Can't construct exclusive range on partition key " \
               "#{range_key_name}"
        end
        scoped(lower_bound: bound(true, true, start_key))
      end

      #
      # Restrict records to those whose value in the first unscoped primary key
      # column are less than or equal to the given start key.
      #
      # @param end_key the inclusive upper bound for values in the key column
      # @return [RecordSet] record set with the upper bound applied
      #
      # @see #before
      #
      def upto(end_key)
        unless partition_specified?
          fail IllegalQuery,
               "Can't construct exclusive range on partition key " \
               "#{range_key_name}"
        end
        scoped(upper_bound: bound(false, true, end_key))
      end

      #
      # Reverse the order in which records will be returned from the record set
      #
      # @return [RecordSet] record set with order reversed
      #
      # @note This method can only be called on record sets whose partition key
      #   columns are fully specified. See {#[]} for a discussion of partition
      #   key scoping.
      #
      def reverse
        unless partition_specified?
          fail IllegalQuery,
               "Can't reverse without scoping to partition key " \
               "#{range_key_name}"
        end
        scoped(reversed: !reversed?)
      end

      #
      # @overload first
      #   @return [Record] the first record in this record set
      #
      # @overload first(count)
      #   @param count [Integer] how many records to return
      #   @return [Array] the first `count` records of the record set
      #
      # @return [Record,Array]
      #
      def first(count = nil)
        count ? limit(count).entries : limit(1).each.first
      end

      #
      # @overload last
      #   @return [Record] the last record in this record set
      #
      # @overload last(count)
      #   @param count [Integer] how many records to return
      #   @return [Array] the last `count` records in the record set in
      #     ascending order
      #
      # @return [Record,Array]
      #
      def last(count = nil)
        reverse.first(count).tap do |results|
          results.reverse! if count
        end
      end

      #
      # @return [Integer] the total number of records in this record set
      #
      def count
        data_set.count
      end

      #
      # Enumerate over the records in this record set
      #
      # @yieldparam record [Record] each successive record in the record set
      # @return [Enumerator] if no block given
      # @return [void]
      #
      # @see find_each
      #
      def each(&block)
        find_each(&block)
      end

      #
      # Enumerate over the records in this record set, with control over how the
      # database is queried
      #
      # @param (see #find_rows_in_batches)
      # @yieldparam (see #each)
      # @option (see #find_rows_in_batches)
      # @return (see #each)
      #
      # @see #find_in_batches
      #
      def find_each(options = {})
        return enum_for(:find_each, options) unless block_given?
        find_each_row(options) { |row| yield target_class.hydrate(row) }
      end

      #
      # Enumerate over the records in this record set in batches. Note that the
      # given batch_size controls the maximum number of records that can be
      # returned per query, but no batch is guaranteed to be exactly the given
      # `batch_size`
      #
      # @param (see #find_rows_in_batches)
      # @option (see #find_rows_in_batches)
      # @yieldparam batch [Array<Record>] batch of records
      # @return (see #each)
      #
      def find_in_batches(options = {})
        return enum_for(:find_in_batches, options) unless block_given?
        find_rows_in_batches(options) do |rows|
          yield rows.map { |row| target_class.hydrate(row) }
        end
      end

      #
      # Enumerate over the row data for each record in this record set, without
      # hydrating an actual {Record} instance. Useful for operations where speed
      # is at a premium.
      #
      # @param (see #find_rows_in_batches)
      # @option (see #find_rows_in_batches)
      # @yieldparam row [Hash<Symbol,Object>] a hash of column names to values
      #   for each row
      # @return (see #each)
      #
      # @see #find_rows_in_batches
      #
      def find_each_row(options = {}, &block)
        return enum_for(:find_each_row, options) unless block
        find_rows_in_batches(options) { |rows| rows.each(&block) }
      end

      #
      # Enumerate over batches of row data for the records in this record set.
      #
      # @param options [Options] options for querying the database
      # @option options [Integer] :batch_size (1000) the maximum number of rows
      #   to return per batch query
      # @yieldparam batch [Array<Hash<Symbol,Object>>] a batch of rows
      # @return (see #each)
      #
      # @see #find_each_row
      # @see #find_in_batches
      #
      def find_rows_in_batches(options = {}, &block)
        return find_rows_in_single_batch(options, &block) if row_limit
        options.assert_valid_keys(:batch_size)
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

      #
      # @return [Cequel::Metal::DataSet] the data set underlying this record set
      #
      def data_set
        @data_set ||= construct_data_set
      end

      #
      # @return [Hash] map of key column names to the values that have been
      #   specified in this record set
      #
      def scoped_key_attributes
        Hash[scoped_key_columns.map { |col| col.name }.zip(scoped_key_values)]
      end

      # (see BulkWrites#delete_all)
      def delete_all
        if partition_specified?
          data_set.delete
        else
          super
        end
      end

      def_delegators :entries, :inspect

      # @private
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
          at(row[range_key_name])
            .next_batch_from(row)
            .find_rows_in_batches(options, &block)
        end
      end

      def find_rows_in_single_batch(options = {})
        if options.key?(:batch_size)
          fail ArgumentError,
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

      def partition_exactly_specified?
        scoped_key_values.length == target_class.partition_key_columns.length
      end

      def multiple_records_specified?
        scoped_key_values.any? { |values| values.is_a?(Array) }
      end

      def next_unscoped_key_column_valid_for_in_query?
        next_unscoped_key_column = unscoped_key_columns.first

        next_unscoped_key_column == partition_key_columns.last ||
          next_unscoped_key_column == clustering_columns.last
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
          target_class.reflect_on_column(column_name).collection_column?
            is_a?(Cequel::Schema::CollectionColumn)
        end
      end

      def select_non_collection_columns!
        if selects_collection_columns?
          fail ArgumentError,
               "Can't scope by multiple keys when selecting a collection " \
               "column."
        end
        if select_columns.empty?
          non_collection_columns = target_class.columns
            .reject { |column| column.collection_column? }
            .map { |column| column.name }
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
        DataSetBuilder.build_for(self)
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

      def load!
        fail ArgumentError, "Not all primary key columns have specified values"
      end

      def scoped(new_attributes = {}, &block)
        attributes_copy = Marshal.load(Marshal.dump(attributes))
        attributes_copy.merge!(new_attributes)
        attributes_copy.tap(&block) if block
        RecordSet.new(target_class, attributes_copy)
      end

      def scope_and_resolve(&block)
        scoped(&block).resolve_if_fully_specified
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
