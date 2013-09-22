module Cequel

  module Record

    module Scoped

      extend ActiveSupport::Concern

      module ClassMethods

        extend Forwardable

        def_delegators :current_scope,
          *(RecordSet.public_instance_methods(false) - Object.instance_methods)

        def current_scope
          Thread.current[current_scope_key] || RecordSet.new(self)
        end

        def with_scope(record_set)
          previous_scope = current_scope
          self.current_scope = record_set
          yield
        ensure
          self.current_scope = previous_scope
        end

        protected

        def current_scope=(current_scope)
          Thread.current[current_scope_key] = current_scope
        end

        def current_scope_key
          @current_scope_key ||= :"#{name}::current_scope"
        end

      end

      def initialize_new_record(*)
        super
        @attributes.merge!(self.class.current_scope.scoped_key_attributes)
      end

    end

  end

end
