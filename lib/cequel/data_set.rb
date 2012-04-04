module Cequel

  class DataSet

    include Helpers

    # @return [Keyspace] the keyspace this data set lives in
    attr_reader :keyspace

    # @return [Symbol] the name of the column group this data set draws from
    attr_reader :column_group

    #
    # @param column_group [Symbol] column group for this data set
    # @param keyspace [Keyspace] keyspace this data set's column group lives in
    #
    # @see Keyspace#[]
    #
    def initialize(column_group, keyspace)
      @column_group, @keyspace = column_group, keyspace
      @select_columns, @row_specifications = [], []
    end

    #
    # Insert a row into the column group.
    #
    # @param [Hash] data column-value pairs. The first entry *must* be the key column.
    # @param [Options] options options for persisting the row
    # @option (see #generate_upsert_options)
    #
    def insert(data, options = {})
      options.symbolize_keys!
      cql = "INSERT INTO #{@column_group}" <<
        " (#{data.keys.join(', ')})" <<
        " VALUES (" << (['?'] * data.length).join(', ') << ")" <<
        generate_upsert_options(options)

      @keyspace.execute(sanitize(cql, *data.values))
    end

    #
    # Update rows
    #
    # @param [Hash] data column-value pairs
    # @param [Options] options options for persisting the column data
    # @option (see #generate_upsert_options)
    #
    # TODO support counter columns
    #
    def update(data, options = {})
      cql = "UPDATE #{@column_group}" <<
        generate_upsert_options(options) <<
        " SET " << data.keys.map { |k| "#{k} = ?" }.join(' AND ') <<
        row_specifications_cql

      @keyspace.execute(sanitize(cql, *data.values))
    end

    # 
    # Delete data from the column family
    #
    # @param columns zero or more columns to delete. Deletes the entire row if none specified.
    # @param options persistence options
    #
    def delete(*columns)
      options = columns.extract_options!
      column_aliases = columns.empty? ? '' : " #{columns.join(', ')}"
      cql, values = "DELETE#{column_aliases}" <<
        " FROM #{@column_group}" <<
        generate_upsert_options(options) <<
        row_specifications_cql

      @keyspace.execute(sanitize(cql, *values))
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
    # Add consistency option for data set retrieval
    #
    # @param consistency [:one,:quorum,:local_quorum,:each_quorum]
    # @return [DataSet] new data set with specified consistency
    #
    def consistency(consistency)
      clone.tap { |data_set| data_set.consistency = consistency.to_sym }
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
    #
    def where(row_specification, *bind_vars)
      clone.tap do |data_set|
        data_set.row_specifications.concat(
          case row_specification
          when Hash then RowSpecification.build(row_specification)
          when String then CqlRowSpecification.build(row_specification, bind_vars)
          else raise ArgumentError, "Invalid argument #{row_specification.inspect}; expected Hash or String"
          end
        )
      end
    end

    #
    # Limit the number of rows returned by this data set
    #
    # @param limit [Integer] maximum number of rows to return
    #
    def limit(limit)
      clone.tap { |data_set| data_set.limit = limit }
    end

    #
    # @return [String] CQL select statement encoding this data set's scope.
    #
    def to_cql
      select_cql <<
        " FROM #{@column_group}" <<
        consistency_cql <<
        row_specifications_cql <<
        limit_cql
    end

    def inspect
      "#<#{self.class.name}: #{to_cql}>"
    end

    def ==(other)
      to_cql == other.to_cql
    end

    protected

    attr_reader :select_columns, :row_specifications
    attr_writer :consistency, :limit

    private

    def initialize_copy(source)
      super
      @select_columns = source.select_columns.clone
      @row_specifications = source.row_specifications.clone
    end

    #
    # Generate CQL option statement for inserts and updates
    #
    # @param [Hash] options options for insert
    # @option options [Symbol,String] :consistency required consistency for the write
    # @option options [Integer] :ttl time-to-live in seconds for the written data
    # @option options [Time,Integer] :timestamp the timestamp associated with the column values
    #
    def generate_upsert_options(options)
      if options.empty?
        ''
      else
        ' USING ' <<
          options.map do |key, value|
            serialized_value =
              case key
              when :consistency then value.to_s.upcase
              when :timestamp then value.to_i
              else value
              end
            "#{key.to_s.upcase} #{serialized_value}"
          end.join(' AND ')
      end
    end

    def select_cql
      if @select_columns.any?
        "SELECT #{@select_columns.join(', ')}"
      else
        "SELECT *"
      end
    end

    def consistency_cql
      if @consistency
        " USING CONSISTENCY #{@consistency.upcase}"
      else ''
      end
    end

    def row_specifications_cql
      if @row_specifications.any?
        " WHERE #{@row_specifications.map { |c| c.to_cql }.join(' AND ')}"
      else ''
      end
    end

    def limit_cql
      @limit ? " LIMIT #{@limit}" : ''
    end

  end

end
