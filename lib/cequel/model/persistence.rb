module Cequel

  module Model

    module Persistence

      extend ActiveSupport::Concern

      module ClassMethods

        def find(id)
          self[id].load!
        end

        def [](id)
          attributes = {local_key_column.name => id}
          new_empty { @attributes = attributes; self }
        end

        private

        def hydrate(row)
          new_empty { hydrate(row) }
        end

      end

      def load
        unless loaded?
          row = metal_scope.first
          hydrate(row) unless row.nil?
        end
        self
      end

      def load!
        load.tap do
          if transient?
            key_name = self.class.local_key_column.name
            raise Cequel::Model::RecordNotFound,
              "Couldn't find #{self.class.name} with #{key_name}=#{attributes[key_name]}"
          end
        end
      end

      def loaded?
        !!@loaded
      end

      def save
        if new_record? then create
        else update
        end
        @new_record = false
        attributes_for_update.clear
        attributes_for_deletion.clear
        true
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
        if attributes_for_create.except(self.class.local_key_column.name).present?
          metal_scope.insert(attributes_for_create)
        end
        loaded!
        persisted!
      end

      def update
        connection.batch do
          if attributes_for_update.present?
            metal_scope.update(attributes_for_update)
          end
          collection_proxies.each_value { |proxy| proxy._update(metal_scope) }
          if attributes_for_deletion.present?
            metal_scope.delete(attributes_for_deletion)
          end
        end
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
          unless attribute.to_sym == self.class.local_key_column.name
            if value.nil?
              attributes_for_deletion << attribute
            else
              attributes_for_update[attribute] = value
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
      end

      def metal_scope
        key_column_name = self.class.local_key_column.name
        connection[table_name].
          where(key_column_name => read_attribute(key_column_name))
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
