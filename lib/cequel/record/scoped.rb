module Cequel

  module Record

    module Scoped

      extend ActiveSupport::Concern

      module ClassMethods

        extend Forwardable

        def_delegators :current_scope,
          *(RecordSet.public_instance_methods(false) +
            BulkWrites.public_instance_methods -
            Object.instance_methods)

        def current_scope
          delegating_scope || RecordSet.new(self)
        end

        def with_scope(record_set)
          previous_scope = delegating_scope
          self.delegating_scope = record_set
          yield
        ensure
          self.delegating_scope = previous_scope
        end

        protected

        def delegating_scope
          Thread.current[delegating_scope_key]
        end

        def delegating_scope=(delegating_scope)
          Thread.current[delegating_scope_key] = delegating_scope
        end

        def delegating_scope_key
          @delegating_scope_key ||= :"#{name}::delegating_scope"
        end

      end

      def initialize_new_record(*)
        super
        @attributes.merge!(self.class.current_scope.scoped_key_attributes)
      end

    end

  end

end
