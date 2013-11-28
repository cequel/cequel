module Cequel

  module Record

    module Persistence

      extend ActiveSupport::Concern
      extend Forwardable

      module ClassMethods

        extend Forwardable
        def_delegator 'Cequel::Record', :connection

        def create(attributes = {}, &block)
          new(attributes, &block).tap { |record| record.save }
        end

        def table
          connection[table_name]
        end

        def hydrate(row)
          new_empty(row).__send__(:hydrated!)
        end

      end

      def_delegators 'self.class', :connection, :table

      def key_attributes
        @attributes.slice(*self.class.key_column_names)
      end

      def key_values
        key_attributes.values
      end

      def exists?
        load!
        true
      rescue RecordNotFound
        false
      end
      alias :exist? :exists?

      def load
        assert_keys_present!
        record_collection.load! unless loaded?
        self
      end

      def load!
        load.tap do
          if transient?
            raise RecordNotFound,
              "Couldn't find #{self.class.name} with #{key_attributes.inspect}"
          end
        end
      end

      def loaded?(column = nil)
        !!@loaded && (column.nil? || @attributes.key?(column.to_sym))
      end

      def save(options = {})
        options.assert_valid_keys
        if new_record? then create
        else update
        end
        @new_record = false
        true
      end

      def update_attributes(attributes)
        self.attributes = attributes
        save
      end

      def destroy
        assert_keys_present!
        metal_scope.delete
        transient!
        self
      end

      def new_record?
        !!@new_record
      end

      def persisted?
        !!@persisted
      end

      def transient?
        !persisted?
      end

      def hydrate(row)
        @attributes = row
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

      def create
        assert_keys_present!
        metal_scope.insert(attributes.reject { |attr, value| value.nil? })
        loaded!
        persisted!
      end

      def update
        assert_keys_present!
        connection.batch do
          updater.execute
          deleter.execute
          @updater, @deleter = nil
        end
      end

      def updater
        @updater ||= metal_scope.updater
      end

      def deleter
        @deleter ||= metal_scope.deleter
      end

      private

      def read_attribute(attribute)
        super
      rescue MissingAttributeError
        load
        super
      end

      def write_attribute(name, value)
        column = self.class.reflect_on_column(name)
        raise UnknownAttributeError, "unknown attribute: #{name}" unless column
        value = column.cast(value) unless value.nil?

        super.tap do
          unless new_record?
            if key_attributes.keys.include?(name)
              raise ArgumentError, "Can't update key #{name} on persisted record"
            end

            if value.nil?
              deleter.delete_columns(name)
            else
              updater.set(name => value)
            end
          end
        end
      end

      def record_collection
        @record_collection ||=
          LazyRecordCollection.new(self.class.at(*key_values)).
          tap { |set| set.__setobj__([self]) }
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
          raise MissingKeyError,
            "Missing required key values: #{missing_keys.keys.join(', ')}"
        end
      end

    end

  end

end
