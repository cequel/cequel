module Cequel

  class ColumnGroup

    def initialize(name, connection)
      @name, @connection = name, connection
    end

    def insert(data, options = {})
      options.symbolize_keys!
      cql = "INSERT INTO #{@name}" <<
        " (#{data.keys.join(', ')})" <<
        " VALUES (" << (['?'] * data.length).join(', ') << ")" <<
        generate_upsert_options(options)

      @connection.execute(cql, *data.values)
    end

    private

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
