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
    # Update a row
    #
    # @param [Symbol, String] key_alias the name of the key column in this group
    # @param [Symbol, String] key_value the key of the row to be updated
    # @param [Hash] data column-value pairs
    # @param [Options] options options for persisting the column data
    # @option (see #generate_upsert_options)
    #
    def update(key_alias, key_value, data, options = {})
      cql = "UPDATE #{@name}" <<
        generate_upsert_options(options) <<
        " SET " << data.keys.map { |k| "#{k} = ?" }.join(' AND ') <<
        " WHERE #{key_alias} = ?"

      @connection.execute(cql, *(data.values << key_value))
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
