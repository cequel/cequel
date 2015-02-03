# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # This module provides functionality for loading and saving records to the
    # Cassandra database.
    #
    # @see ClassMethods
    #
    # @since 0.1.0
    #
    module Persistence
      extend ActiveSupport::Concern
      extend Util::Forwardable
      include Instrumentation

      #
      # Class-level functionality for loading and saving records
      #
      module ClassMethods
        extend Util::Forwardable

        #
        # Initialize a new record instance, assign attributes, and immediately
        # save it.
        #
        # @param attributes [Hash] attributes to assign to the new record
        # @yieldparam record [Record] record to make modifications before
        #   saving
        # @return [Record] self
        #
        # @example Create a new record with attribute assignment
        #   Post.create(
        #     blog_subdomain: 'cassandra',
        #     permalink: 'cequel',
        #     title: 'Cequel: The Next Generation'
        #   )
        #
        # @example Create a new record with a block
        #   Post.create do |post|
        #     post.blog = blog
        #     post.permalink = 'cequel'
        #     post.title = 'Cequel: The Next Generation'
        #   end
        #
        def create(attributes = {}, &block)
          new(attributes, &block).tap { |record| record.save }
        end

        # @private
        def table
          connection[table_name]
        end

        # @return [Cequel::Record] a new instance of this record class
        # populated with the attributes from `row`
        #
        # @param row [Hash] attributes from the database with which
        #   the new instance should be populated.
        #
        # @private
        def hydrate(row)
          new_empty.hydrate(row)
        end

        # @private
        def_delegator 'Cequel::Record', :connection
      end

      #
      # @return [Hash] the attributes of this record that make up the primary
      #   key
      #
      # @example
      #   post = Post.new
      #   post.blog_subdomain = 'cassandra'
      #   post.permalink = 'cequel'
      #   post.title = 'Cequel: The Next Generation'
      #   post.key_attributes
      #     #=> {:blog_subdomain=>'cassandra', :permalink=>'cequel'}
      #
      # @since 1.0.0
      #
      def key_attributes
        @attributes.slice(*self.class.key_column_names)
      end

      #
      # @return [Array] the values of the primary key columns for this record
      #
      # @see #key_attributes
      # @since 1.0.0
      #
      def key_values
        key_attributes.values
      end
      alias_method :to_key, :key_values

      #
      # Check if an unloaded record exists in the database
      #
      # @return  `true` if the record has a corresponding row in the
      #   database
      #
      # @since 1.0.0
      #
      def exists?
        load!
        true
      rescue RecordNotFound
        false
      end
      alias_method :exist?, :exists?

      #
      # Load an unloaded record's row from the database and hydrate the
      # record's attributes
      #
      # @return [Record] self
      #
      # @since 1.0.0
      #
      def load
        assert_keys_present!
        record_collection.load! unless loaded?
        self
      end

      #
      # Attempt to load an unloaded record and raise an error if the record
      # does not correspond to a row in the database
      #
      # @return [Record] self
      # @raise [RecordNotFound] if row does not exist in the database
      #
      # @see #load
      # @since 1.0.0
      #
      def load!
        load.tap do
          if transient?
            fail RecordNotFound,
                 "Couldn't find #{self.class.name} with " \
                 "#{key_attributes.inspect}"
          end
        end
      end

      #
      # @overload loaded?
      #   @return [Boolean] true if this record's attributes have been loaded
      #     from the database
      #
      # @overload loaded?(column)
      #   @param [Symbol] column name of column to check if loaded
      #   @return [Boolean] true if the named column is loaded in memory
      #
      # @return [Boolean]
      #
      # @since 1.0.0
      #
      def loaded?(column = nil)
        !!@loaded && (column.nil? || @attributes.key?(column.to_sym))
      end

      #
      # Persist the record to the database. If this is a new record, it will
      # be saved using an INSERT statement. If it is an existing record, it
      # will be persisted using a series of `UPDATE` and `DELETE` statements
      # which will persist all changes to the database, including atomic
      # collection modifications.
      #
      # @param options [Options] options for save
      # @option options [Boolean] :validate (true) whether to run validations
      #   before saving
      # @option options [Symbol] :consistency (:quorum) what consistency with
      #   which to persist the changes
      # @option options [Integer] :ttl time-to-live of the updated rows in
      #   seconds
      # @option options [Time] :timestamp the writetime to use for the column
      #   updates
      # @return [Boolean] true if record saved successfully, false if invalid
      #
      # @see Validations#save!
      #
      def save(options = {})
        options.assert_valid_keys(:consistency, :ttl, :timestamp)
        if new_record? then create(options)
        else update(options)
        end
        @new_record = false
        true
      end

      #
      # Set attributes and save the record
      #
      # @param attributes [Hash] hash of attributes to update
      # @return [Boolean] true if saved successfully
      #
      # @see #save
      # @see Properties#attributes=
      # @see Validations#update_attributes!
      #
      def update_attributes(attributes)
        self.attributes = attributes
        save
      end

      #
      # Remove this record from the database
      #
      # @param options [Options] options for deletion
      # @option options [Symbol] :consistency (:quorum) what consistency with
      #   which to persist the deletion
      # @option options [Time] :timestamp the writetime to use for the deletion
      #
      # @return [Record] self
      #
      def destroy(options = {})
        options.assert_valid_keys(:consistency, :timestamp)
        assert_keys_present!
        metal_scope.delete(options)
        transient!
        self
      end
      instrument :destroy, data: ->(rec) { {table_name: rec.table_name} }

      #
      # @return true if this is a new, unsaved record
      #
      # @since 1.0.0
      #
      def new_record?
        !!@new_record
      end

      #
      # @return true if this record is persisted in the database
      #
      # @see #transient?
      #
      def persisted?
        !!@persisted
      end

      #
      # @return true if this record is not persisted in the database
      #
      # @see persisted?
      #
      def transient?
        !persisted?
      end

      # @private
      def hydrate(row)
        init_attributes(row)
        hydrated!
        self
      end

      protected

      def persisted!
        @persisted = true
        self
      end

      def transient!
        @persisted = false
        self
      end

      def create(options = {})
        assert_keys_present!
        metal_scope
          .insert(attributes.reject { |attr, value| value.nil? }, options)
        loaded!
        persisted!
      end
      instrument :create, data: ->(rec) { {table_name: rec.table_name} }

      def update(options = {})
        assert_keys_present!
        connection.batch do |batch|
          batch.on_complete { @updater, @deleter = nil }
          updater.execute(options)
          deleter.execute(options.except(:ttl))
        end
      end
      instrument :update, data: ->(rec) { {table_name: rec.table_name} }

      def updater
        raise ArgumentError, "Can't get updater for new record" if new_record?
        @updater ||= Metal::Updater.new(metal_scope)
      end

      def deleter
        raise ArgumentError, "Can't get deleter for new record" if new_record?
        @deleter ||= Metal::Deleter.new(metal_scope)
      end

      private

      def_delegators 'self.class', :connection, :table
      private :connection, :table

      def read_attribute(attribute)
        super
      rescue MissingAttributeError
        load
        super
      end

      def write_attribute(name, value)
        column = self.class.reflect_on_column(name)
        fail UnknownAttributeError, "unknown attribute: #{name}" unless column
        value = column.cast(value) unless value.nil?

        if !new_record? && key_attributes.keys.include?(name)
          if read_attribute(name) != value
            fail ArgumentError,
                 "Can't update key #{name} on persisted record"
          end
        else
          super.tap { stage_attribute_update(name, value) }
        end
      end

      def stage_attribute_update(name, value)
        unless new_record?
          if value.nil?
            deleter.delete_columns(name)
          else
            updater.set(name => value)
          end
        end
      end

      def record_collection
        @record_collection ||=
          LazyRecordCollection.new(self.class.at(*key_values))
          .tap { |set| set.__setobj__([self]) }
      end

      def hydrated!
        loaded!
        persisted!
        self
      end

      def loaded!
        @loaded = true
        collection_proxies.each_value { |collection| collection.loaded! }
        self
      end

      def metal_scope
        table.where(key_attributes)
      end

      def attributes_for_create
        @attributes.each_with_object({}) do |(column, value), attributes|
          attributes[column] = value unless value.nil?
        end
      end

      def attributes_for_update
        @attributes_for_update ||= {}
      end

      def attributes_for_deletion
        @attributes_for_deletion ||= []
      end

      def assert_keys_present!
        missing_keys = key_attributes.select { |k, v| v.nil? }
        if missing_keys.any?
          fail MissingKeyError,
               "Missing required key values: #{missing_keys.keys.join(', ')}"
        end
      end
    end
  end
end
