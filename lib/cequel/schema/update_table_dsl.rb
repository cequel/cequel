# -*- encoding : utf-8 -*-
module Cequel
  module Schema
    #
    # DSL for describing a series of schema modification statements
    #
    class UpdateTableDSL < BasicObject
      extend ::Cequel::Util::Forwardable
      #
      # Describe a series of schema modifications and build a {TableUpdater}
      # to encapsulate them
      #
      # @param (see #initialize)
      # @yield a block evaluated in the context of an {UpdateTableDSL} instance
      # @return [void]
      #
      # @api private
      # @see Keyspace#update_table
      #
      def self.apply(updater, &block)
        dsl = new(updater)
        dsl.instance_eval(&block)
      end

      #
      # @param updater [TableUpdater]
      #
      # @api private
      #
      def initialize(updater)
        @updater = updater
      end

      #
      # @!method add_column(name, type)
      #   (see Cequel::Schema::TableUpdater#add_column)
      #
      def_delegator :@updater, :add_column

      #
      # @!method add_list(name, type)
      #   (see Cequel::Schema::TableUpdater#add_list)
      #
      def_delegator :@updater, :add_list

      #
      # @!method add_set(name, type)
      #   (see Cequel::Schema::TableUpdater#add_set)
      #
      def_delegator :@updater, :add_set

      #
      # @!method add_map(name, key_type, value_type)
      #   (see Cequel::Schema::TableUpdater#add_map)
      #
      def_delegator :@updater, :add_map

      #
      # @!method change_column(name, type)
      #   (see Cequel::Schema::TableUpdater#change_column)
      #
      def_delegator :@updater, :change_column

      #
      # @!method rename_column(old_name, new_name)
      #   (see Cequel::Schema::TableUpdater#rename_column)
      #
      def_delegator :@updater, :rename_column

      #
      # @!method change_properties(options)
      #   (see Cequel::Schema::TableUpdater#change_properties)
      #
      def_delegator :@updater, :change_properties
      alias_method :change_options, :change_properties

      #
      # @!method create_index(column_name, index_name = nil)
      #   (see Cequel::Schema::TableUpdater#create_index
      #
      def_delegator :@updater, :create_index
      alias_method :add_index, :create_index

      #
      # @!method drop_index(index_name)
      #   (see Cequel::Schema::TableUpdater#drop_index)
      #
      def_delegator :@updater, :drop_index
      alias_method :remove_index, :drop_index
    end
  end
end
