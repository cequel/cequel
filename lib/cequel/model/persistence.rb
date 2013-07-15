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

    end

  end

end
