module Cequel

  # 
  # Encapsulates a data set, specified as a column family and optionally
  # various query elements.
  #
  # @todo Support ALTER, CREATE, CREATE INDEX, DROP
  #
  class DataSet

    include Enumerable

    # @return [Keyspace] the keyspace this data set lives in
    attr_reader :keyspace

    # @return [Symbol] the name of the column family this data set draws from
    attr_reader :column_family

    #
    # @param column_family [Symbol] column family for this data set
    # @param keyspace [Keyspace] keyspace this data set's column family lives in
    #
    # @see Keyspace#[]
    #
    def initialize(column_family, keyspace)
      @column_family, @keyspace = column_family, keyspace
      @select_columns, @select_options, @row_specifications = [], {}, []
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
      options.symbolize_keys!
      cql = "INSERT INTO #{@column_family}" <<
        " (?) VALUES (?)" <<
        generate_upsert_options(options)

      @keyspace.write(cql, data.keys, data.values)
    end

    #
    # Update rows
    #
    # @param [Hash] data column-value pairs
    # @param [Options] options options for persisting the column data
    # @option (see #generate_upsert_options)
    # @note if a enclosed in a Keyspace#batch block, this method will be executed as part of the batch.
    #
    # @todo support counter columns
    #
    def update(data, options = {})
      statement = Statement.new.
        append("UPDATE #{@column_family}").
        append(generate_upsert_options(options)).
        append(" SET " << data.keys.map { |k| "? = ?" }.join(', '), *data.to_a.flatten).
        append(*row_specifications_cql)

      @keyspace.write(*statement.args)
    rescue EmptySubquery
      # Noop -- no rows to update
    end

    # 
    # Delete data from the column family
    #
    # @param columns zero or more columns to delete. Deletes the entire row if none specified.
    # @param options persistence options
    # @note if a enclosed in a Keyspace#batch block, this method will be executed as part of the batch.
    #
    def delete(*columns)
      options = columns.extract_options!
      column_aliases = columns.empty? ? '' : " #{columns.join(', ')}"
      statement = Statement.new.append('DELETE')
      statement = statement.append(' ?', columns) if columns.any?
      statement = statement.
        append(" FROM #{@column_family}").
        append(generate_upsert_options(options)).
        append(*row_specifications_cql)

      @keyspace.write(*statement.args)
    rescue EmptySubquery
      # Noop -- no rows to delete
    end

    #
    # Remove all data from the column family.
    #
    # @note This method always executes immediately, even if called within a batch block. This method does not respect scoped row specifications.
    # @see #delete
    #
    def truncate
      @keyspace.execute("TRUNCATE #{@column_family}")
    end

    #
    # Select specified columns from this data set.
    #
    # @param *columns [Symbol,Array] columns to select
    # @return [DataSet] new data set scoped to specified columns
    #
    def select(*columns)
      options = columns.extract_options!.symbolize_keys
      clone.tap do |data_set|
        if columns.length == 1 && Range === columns.first
          range = columns.first
          options[:from] = range.first
          options[:to] = range.last
        else
          data_set.select_columns.concat(columns.flatten)
        end
        data_set.select_options.merge!(options)
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
      clone.tap { |data_set| data_set.limit = limit }
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
          @keyspace.execute(*cql).fetch do |row|
            yield row.to_hash.with_indifferent_access
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
      row = @keyspace.execute(*limit(1).cql).fetch_row
      row.to_hash.with_indifferent_access if row
    rescue EmptySubquery
      nil
    end

    #
    # @return [Fixnum] the number of rows in this data set
    #
    def count
      @keyspace.execute(*count_cql).fetch_row['count']
    rescue EmptySubquery
      0
    end

    #
    # @raise [EmptySubquery] if row specifications use a subquery that returns no results
    # @return [String] CQL select statement encoding this data set's scope.
    #
    def cql
      statement = Statement.new.
        append(*select_cql).
        append(" FROM #{@column_family}").
        append(consistency_cql).
        append(*row_specifications_cql).
        append(limit_cql).
        args
    end

    #
    # @return [String] CQL statement to get count of rows in this data set
    #
    def count_cql
      Statement.new.
        append("SELECT COUNT(*) FROM #{@column_family}").
        append(consistency_cql).
        append(*row_specifications_cql).
        append(limit_cql).args
    end

    def inspect
      "#<#{self.class.name}: #{CassandraCQL::Statement.sanitize(cql.first, cql[1..-1])}>"
    end

    def ==(other)
      cql == other.cql
    end

    attr_reader :select_columns, :select_options, :row_specifications
    attr_writer :consistency, :limit

    private

    def initialize_copy(source)
      super
      @select_columns = source.select_columns.clone
      @select_options = source.select_options.clone
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
      ['SELECT '].tap do |args|
        cql = args.first
        if @select_options[:first]
          cql << "FIRST #{@select_options[:first]} "
        elsif @select_options[:last]
          cql << "FIRST #{@select_options[:last]} REVERSED "
        end
        if @select_options[:from] || @select_options[:to]
          cql << '?..?'
          args << (@select_options[:from] || '') << (@select_options[:to] || '')
        elsif @select_columns.any?
          cql << '?'
          args << @select_columns
        else
          cql << '*'
        end
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
        cql_fragments, bind_vars = [], []
        @row_specifications.each do |spec|
          cql_with_vars = spec.cql
          cql_fragments << cql_with_vars.shift
          bind_vars.concat(cql_with_vars)
        end
        [" WHERE #{cql_fragments.join(' AND ')}", *bind_vars]
      else ['']
      end
    end

    def limit_cql
      @limit ? " LIMIT #{@limit}" : ''
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
