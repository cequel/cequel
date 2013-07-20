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
          allocate.instance_eval { @attributes = attributes; self }
        end

        private

        def hydrate(row)
          allocate.instance_eval { hydrate(row) }
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
        if persisted? then update
        else create
        end
        true
      end

      def destroy
        metal_scope.delete
        transient!
        self
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
        metal_scope.insert(attributes_for_create)
        persisted!
      end

      def update
        connection.batch do
          metal_scope.update(attributes_for_update)
          metal_scope.delete(nil_attributes_for_update)
        end
      end

      private

      def read_attribute(attribute)
        super
      rescue MissingAttributeError
        load
        super
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
        attributes_for_create.except(self.class.local_key_column.name) #XXX
      end

      def nil_attributes_for_update
        @attributes.each_with_object([]) do |(column, value), columns|
          columns << column if value.nil?
        end
      end

    end

  end

end
