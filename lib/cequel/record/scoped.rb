# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # All of the instance methods of {RecordSet} are also available as class
    # methods on {Record} implementations.
    #
    # @since 0.1.0
    #
    module Scoped
      extend ActiveSupport::Concern

      #
      # Scoping-related methods for {Record} classes
      #
      module ClassMethods
        extend Util::Forwardable

        def_delegators :current_scope,
                       *(RecordSet.public_instance_methods(false) +
                         BulkWrites.public_instance_methods -
                         Object.instance_methods -
                         [:to_ary])

        # @private
        def current_scope
          delegating_scope || RecordSet.new(self)
        end

        # @private
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

      # @private
      def assert_fully_specified!
        self
      end

      private

      def initialize_new_record(*)
        super
        @attributes.merge!(self.class.current_scope.scoped_key_attributes)
      end
    end
  end
end
