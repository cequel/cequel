module Cequel

  module Metal

    class Updater < Writer

      def set(data)
        data.each_pair do |column, value|
          prepare_upsert_value(value) do |binding, *values|
            statements << "#{column} = #{binding}"
            bind_vars.concat(values)
          end
        end
      end

      def list_prepend(column, elements)
        statements << "#{column} = [?] + #{column}"
        bind_vars << elements
      end

      def list_append(column, elements)
        statements << "#{column} = #{column} + [?]"
        bind_vars << elements
      end

      def list_remove(column, value)
        statements << "#{column} = #{column} - [?]"
        bind_vars << value
      end

      def list_replace(column, index, value)
        statements << "#{column}[#{index}] = ?"
        bind_vars << value
      end

      def set_add(column, values)
        statements << "#{column} = #{column} + {?}"
        bind_vars << values
      end

      def set_remove(column, value)
        statements << "#{column} = #{column} - {?}"
        bind_vars << ::Kernel.Array(value)
      end

      def map_update(column, updates)
        binding_pairs = ::Array.new(updates.length) { '?:?' }.join(',')
        statements << "#{column} = #{column} + {#{binding_pairs}}"
        bind_vars.concat(updates.flatten)
      end

      private

      def write_to_statement(statement)
        statement.append("UPDATE #{table_name}").
          append(generate_upsert_options).
          append(" SET ").
          append(statements.join(', '), *bind_vars)
      end

    end

  end

end
