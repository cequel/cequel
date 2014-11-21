# -*- encoding : utf-8 -*-
module Cequel
  module Metal
    #
    # A result row from a CQL query. Acts as a hash of column names to values,
    # but also exposes TTLs and writetimes
    #
    # @since 1.0.0
    #
    class Row < DelegateClass(ActiveSupport::HashWithIndifferentAccess)
      #
      # Encapsulate a result row from the driver
      #
      # @param result_row [Hash] row from underlying driver
      # @return [Row] encapsulated row
      #
      # @api private
      #
      def self.from_result_row(result_row)
        if result_row
          new.tap do |row|
            result_row.each_pair do |name, value|
              if name =~ /^(ttl|writetime)\((.+)\)$/
                if $1 == 'ttl' then row.set_ttl($2, value)
                else row.set_writetime($2, value)
                end
              elsif value.respond_to?(:in_time_zone) && value.is_a?(Time) then row[name] = value.in_time_zone
              else row[name] = value
              end
            end
          end
        end
      end

      #
      # @api private
      #
      def initialize
        super(ActiveSupport::HashWithIndifferentAccess.new)
        @ttls = ActiveSupport::HashWithIndifferentAccess.new
        @writetimes = ActiveSupport::HashWithIndifferentAccess.new
      end

      #
      # Get the TTL (time-to-live) of a column
      #
      # @param column [Symbol] column name
      # @return [Integer] TTL of column in seconds
      #
      def ttl(column)
        @ttls[column]
      end

      #
      # Get the writetime of a column
      #
      # @param column [Symbol] column name
      # @return [Integer] writetime of column in nanoseconds since epoch
      #
      def writetime(column)
        @writetimes[column]
      end
      alias_method :timestamp, :writetime

      # @private
      def set_ttl(column, value)
        @ttls[column] = value
      end

      # @private
      def set_writetime(column, value)
        @writetimes[column] = value
      end
    end
  end
end
