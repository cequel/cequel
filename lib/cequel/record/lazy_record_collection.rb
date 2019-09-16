# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # Encapsulates a collection of unloaded {Record} instances. In the case
    # where a record set is scoped to fully specify the keys of multiple
    # records, those records will be returned unloaded in a
    # LazyRecordCollection. When an attribute is read from any of the records
    # in a LazyRecordCollection, it will eagerly load all of the records' rows
    # from the database.
    #
    # @since 1.0.0
    #
    class LazyRecordCollection < DelegateClass(Array)
      extend Util::Forwardable
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
        fail ArgumentError if record_set.nil?
        @record_set = record_set

        exploded_key_attributes = [{}].tap do |all_key_attributes|
          key_columns.zip(scoped_key_values) do |column, values|
            all_key_attributes.replace([values].flatten.compact.flat_map do |value|
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
      end

      #
      # Hydrate all the records in this collection from a database query
      # 
      # Takes the record_set (Cequel::RecordSet type), indexes it by the
      # tracked_class's keys, and the loops over self to hydrate the values
      # into the records.  
      #
      # In the loop, checks to ensure that every row was hydrated.  If not,
      # the record is added to a tracking array that can be used in future
      # versions of Cequel (and/or during gem troubleshooting). 
      #
      # After hydrating the records, the error count is checked, and
      # the system returns an error that states how many records were
      # unable to by hydrated if there are unhydrated records.
      #
      # @return [LazyRecordCollection] self
      def load!
        key_values = record_set.key_column_names
        track_hydrate_failed = []
        
        rows_by_identity = {}
        record_set.find_each_row do |row|
          identity = row.values_at(*key_values)
          rows_by_identity[identity] = row
        end

        each do |record|
          row = rows_by_identity[record.key_values]
          record.hydrate(row) if row
          track_hydrate_failed << record if record.loaded? == false
        end

        if track_hydrate_failed.length > 0
          fail Cequel::Record::RecordNotFound,
               "Expected #{count} results; got #{count - track_hydrate_failed.length}"
        end

        self
      end

      # @private
      def assert_fully_specified!
        self
      end

      private

      attr_reader :record_set

      def_delegators :record_set, :key_columns, :scoped_key_values
      private :key_columns, :scoped_key_values

      def key_attributes_for_each_row
        map { |record| record.key_attributes }
      end
    end
  end
end
