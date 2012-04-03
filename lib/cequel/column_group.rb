module Cequel

  class ColumnGroup

    #
    # @api private
    # @see Keyspace#[]
    #
    def initialize(name, connection)
      @name, @connection = name, connection
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
      cql = "INSERT INTO #{@name}" <<
        " (#{data.keys.join(', ')})" <<
        " VALUES (" << (['?'] * data.length).join(', ') << ")" <<
        generate_upsert_options(options)

      @connection.execute(cql, *data.values)
    end

    #
    # Update rows
    #
    # @param [Hash] data column-value pairs
    # @param [Options] options options for persisting the column data
    # @option (see #generate_upsert_options)
    #
    # TODO support scoped update
    #
    def update(data, options = {})
      cql = "UPDATE #{@name}" <<
        generate_upsert_options(options) <<
        " SET " << data.keys.map { |k| "#{k} = ?" }.join(' AND ')

      @connection.execute(cql, *data.values)
    end

    # 
    # Delete data from the column family
    #
    # @param columns zero or more columns to delete. Deletes the entire row if none specified.
    # @param options persistence options
    #
    # TODO scoped delete
    #
    def delete(*columns)
      options = columns.extract_options!
      column_aliases = columns.empty? ? '' : " #{columns.join(', ')}"
      cql, values = "DELETE#{column_aliases}" <<
        " FROM #{@name}" <<
        generate_upsert_options(options)

      @connection.execute(cql, *values)
    end

    private

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

  end

end
