module Cequel
  module Record
    #
    # Extends {Record} to enable detection of duplicate primary keys, and to
    # customize the behavior when a key collision occurs.
    #
    # @see ClassMethods#on_duplicate_key
    #
    # @since 2.0.0
    #
    module KeyCollisions
      extend ActiveSupport::Concern

      included do
        class_attribute :duplicate_key_behavior
        on_duplicate_key :overwrite
      end

      #
      # @since 2.0.0
      #
      module ClassMethods
        #
        # Enables detection of duplicate primary keys when creating a new
        # record. By default, if a new record is created with a primary key
        # that already exists in the database, *the new record will overwrite
        # the existing record*. This method allows you to change that behavior.
        # Valid arguments are:
        #
        # * `:overwrite`, the default behavior, does not check for duplicate
        #   keys
        # * `:error` will check for duplicate keys and raise an error at
        #   creation time if a new record's primary key conflicts with that of
        #   an existing record
        # * `:ignore` will check for duplicate keys and silently fail to create
        #   a new record if there is a key collision
        #
        # @param behavior [Symbol] `:overwrite`, `:error`, or `:ignore`
        # @return [void]
        #
        # @note checking for duplicate keys at creation time imposes a
        #   substantial performance penalty. Only use duplicate key detection
        #   if keys are not naturally unique. Whenever possible, use a unique
        #   natural key or a UUID as a primary key
        #
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
        if duplicate_key_behavior == :error && self.class.in_batch?
          raise IllegalOperation, "Cannot create a record in a batch when " \
            "duplicate key behavior is set to :error. You may set the " \
            "`duplicate_key_behavior` property on this instance to :ignore " \
            " or :overwrite to allow creation within a batch."
        end

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
