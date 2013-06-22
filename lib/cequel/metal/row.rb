module Cequel

  module Metal

    class Row < ActiveSupport::HashWithIndifferentAccess

      def self.from_result_row(result_row)
        if result_row
          new.tap do |row|
            result_row.column_names.zip(result_row.column_values) do |name, value|
              if name =~ /^(ttl|writetime)\((.+)\)$/
                if $1 == 'ttl' then row.set_ttl($2, value)
                else row.set_writetime($2, value)
                end
              else row[name] = value
              end
            end
          end
        end
      end

      def initialize(*_)
        super
        @ttls = ActiveSupport::HashWithIndifferentAccess.new
        @writetimes = ActiveSupport::HashWithIndifferentAccess.new
      end

      def ttl(column)
        @ttls[column]
      end

      def writetime(column)
        @writetimes[column]
      end

      def set_ttl(column, value)
        @ttls[column] = value
      end

      def set_writetime(column, value)
        @writetimes[column] = value
      end

    end

  end

end
