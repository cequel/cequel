module Cequel
  module Record
    #
    # Encapsulates a collection of unloaded {Record} instances. In the case
    # where a record set is scoped to fully specify the keys of multiple
    # records, those records will be returned unloaded in a
    # LazyRecordCollection. When an attribute is read from any of the records in
    # a LazyRecordCollection, it will eagerly load all of the records' rows from
    # the database.
    #
    # @since 1.0.0
    #
    class LazyRecordCollection < DelegateClass(Array)
      extend Forwardable
      include BulkWrites
      #
      # @!method table
      #   (see RecordSet#table)
      # @!method connection
      #   (see RecordSet#connection)
      def_delegators :record_set, :table, :connection

      #
      # @param record_set [RecordSet] record set representing the records in
      #   this collection
      # @api private
      #
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

      #
      # Hydrate all the records in this collection from a database query
      #
      # @return [LazyRecordCollection] self
      def load!
        records_by_identity = index_by { |record| record.key_values }

        record_set.find_each_row do |row|
          identity = row.values_at(*record_set.key_column_names)
          records_by_identity[identity].hydrate(row)
        end

        self
      end

      private
      attr_reader :record_set

      def key_attributes_for_each_row
        map { |record| record.key_attributes }
      end
    end
  end
end
