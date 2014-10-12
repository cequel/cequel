module Cequel
  module Record
    module KeyCollisions
      extend ActiveSupport::Concern

      included do
        class_attribute :duplicate_key_behavior
        on_duplicate_key :overwrite
      end

      module ClassMethods
        def on_duplicate_key(behavior)
          unless [:overwrite, :error, :ignore].include?(behavior.to_sym)
            raise ArgumentError, "Invalid behavior #{behavior.inspect}. " \
              "Valid behaviors are :overwrite, :error, :ignore"
          end
          self.duplicate_key_behavior = behavior.to_sym
        end
      end

      private

      def create(options = {})
        unless duplicate_key_behavior == :overwrite
          options = options.reverse_merge(if_not_exists: true)
        end
        super(options).tap do |result|
          if result == false && duplicate_key_behavior == :error
            raise DuplicateKey, "There is an existing record with key #{key_attributes.inspect}"
          end
        end
      end
    end
  end
end
