module Cequel

  module Record

    class LazyRecordCollection < DelegateClass(Array)

      extend Forwardable
      include BulkWrites

      def_delegators :record_set, :table, :connection

      def initialize(record_set)
        raise ArgumentError if record_set.nil?

        exploded_key_attributes = [{}].tap do |all_key_attributes|
          record_set.key_columns.zip(record_set.scoped_key_attributes.values) do |column, values|
            all_key_attributes.replace(Array(values).flat_map do |value|
              all_key_attributes.map do |key_attributes|
                key_attributes.merge(column.name => value)
              end
            end)
          end
        end

        unloaded_records = exploded_key_attributes.map do |key_attributes|
          record_set.target_class.new_empty(key_attributes, self)
        end

        super(unloaded_records)
        @record_set = record_set
      end

      def load!
        records_by_identity = index_by { |record| record.key_values }

        record_set.find_each_row do |row|
          identity = row.values_at(*record_set.key_column_names)
          records_by_identity[identity].hydrate(row)
        end
      end

      private
      attr_reader :record_set

      def key_attributes_for_each_row
        map { |record| record.key_attributes }
      end

    end

  end

end
