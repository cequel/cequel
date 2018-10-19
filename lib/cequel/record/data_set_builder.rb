# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # This is a utility class to construct a {Metal::DataSet} for a given
    # {RecordSet}.
    #
    # @api private
    #
    class DataSetBuilder
      extend Util::Forwardable

      #
      # Build a data set for the given record set
      #
      # @param (see #initialize)
      # @return (see #build)
      #
      def self.build_for(record_set)
        new(record_set).build
      end

      #
      # @param record_set [RecordSet] record set for which to construct data
      #   set
      #
      def initialize(record_set)
        @record_set = record_set
        @data_set = record_set.connection[record_set.target_class.table_name]
      end
      private_class_method :new

      #
      # @return [Metal::DataSet] a DataSet exposing the rows for the record set
      #
      def build
        add_limit
        add_select_columns
        add_where_statement
        add_bounds
        add_order
        set_consistency
        set_allow_filtering
        set_page_size
        set_paging_state
        data_set
      end

      protected

      def record_set_delegated_methods
        %i[ row_limit select_columns scoped_key_names scoped_key_values
            scoped_indexed_column lower_bound upper_bound reversed? order_by_column
            query_consistency query_page_size query_paging_state ascends_by? allow_filtering ]
      end

      attr_accessor :data_set
      attr_reader :record_set

      private

      def add_limit
        self.data_set = data_set.limit(row_limit) if row_limit
      end

      def add_select_columns
        self.data_set = data_set.select(*select_columns) if select_columns
      end

      def add_where_statement
        if scoped_key_values
          key_conditions = Hash[scoped_key_names.zip(scoped_key_values)]
          self.data_set = data_set.where(key_conditions)
        end
        if scoped_indexed_column
          self.data_set = data_set.where(scoped_indexed_column)
        end
      end

      def add_bounds
        if lower_bound
          self.data_set =
            data_set.where(*lower_bound.to_cql_with_bind_variables)
        end
        if upper_bound
          self.data_set =
            data_set.where(*upper_bound.to_cql_with_bind_variables)
        end
      end

      def add_order
        column = order_by_column
        if column.present? && reversed?
          self.data_set = data_set.order(column.name => sort_direction)
        end
      end

      def set_consistency
        if query_consistency
          self.data_set = data_set.consistency(query_consistency)
        end
      end

      def set_allow_filtering
        if allow_filtering
          self.data_set = data_set.allow_filtering!
        end
      end

      def set_page_size
        if query_page_size
          self.data_set = data_set.page_size(query_page_size)
        end
      end

      def set_paging_state
        if query_paging_state
          self.data_set = data_set.paging_state(query_paging_state)
        end
      end

      def sort_direction
        ascends_by?(order_by_column) ? :asc : :desc
      end

      def method_missing(m, *args, &block)
        if record_set_delegated_methods.include?(m)
          record_set.send(m, *args, &block)
        else
          super
        end
      end
    end
  end
end
