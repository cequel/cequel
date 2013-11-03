module Cequel

  module Record

    module Persistence

      extend ActiveSupport::Concern
      extend Forwardable

      KeyError = Class.new RuntimeError

      module ClassMethods

        extend Forwardable
        def_delegator 'Cequel::Record', :connection

        def create(attributes = {}, &block)
          new(attributes, &block).tap { |record| record.save }
        end

        def hydrate(row)
          new_empty { hydrate(row) }
        end

      end

      def_delegator 'self.class', :connection

      def key_attributes
        @attributes.slice(*self.class.key_column_names).tap do |keys|
          missing_keys = keys.select { |k, v| v.nil? }
          if missing_keys.any?
            raise KeyError,
              "Missing required key values: #{missing_keys.keys.join(',')}"
          end
        end
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
        unless loaded?
          row = metal_scope.first
          hydrate(row) unless row.nil?
          collection_proxies.each_value { |collection| collection.loaded! }
        end
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

      protected

      def persisted!
        @persisted = true
      end

      def transient!
        @persisted = false
      end

      def create
        metal_scope.insert(attributes.reject { |attr, value| value.nil? })
        loaded!
        persisted!
      end

      def update
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

      def write_attribute(attribute, value)
        super.tap do
          unless new_record?
            if value.nil?
              deleter.delete_columns(attribute)
            else
              if key_attributes.keys.include?(attribute)
                raise KeyError, "Can't update key #{attribute} on persisted record"
              else
                updater.set(attribute => value)
              end
            end
          end
        end
      end

      def hydrate(row)
        @attributes = row
        loaded!
        persisted!
        self
      end

      def loaded!
        @loaded = true
        collection_proxies.each_value { |collection| collection.loaded! }
      end

      def metal_scope
        connection[table_name].
          where(key_attributes)
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

    end

  end

end
