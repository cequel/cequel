# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # {Record} classes can define validations that are run before saving a
    # record instance.
    #
    # @example Validations
    #   class User
    #     include Cequel::Record
    #
    #     key :login, :text
    #     column :email, :text
    #
    #     validates :email, presence: true, format: RFC822::EMAIL
    #   end
    #
    # @see http://api.rubyonrails.org/classes/ActiveModel/Validations.html
    #   ActiveModel::Validations
    #
    # @since 0.1.0
    #
    module Validations
      extend ActiveSupport::Concern

      included do
        include ActiveModel::Validations
        define_model_callbacks :validation
        prepend Callback
      end

      #
      # Validation-related methods exposed on Record class singletons
      #
      module ClassMethods
        #
        # Attempt to create a new record, or raise an exception otherwise
        #
        # @param (see Persistence::ClassMethods#create)
        # @yieldparam (see Persistence::ClassMethods#create)
        # @return (see Persistence::ClassMethods#create)
        # @raise (see Validations#save!)
        #
        def create!(attributes = {}, &block)
          new(attributes, &block).save!
        end
      end

      # @private
      def save(options = {})
        validate = options.fetch(:validate, true)
        options.delete(:validate)
        (!validate || valid?) && super
      end

      #
      # Attempt to save the record, or raise an exception if there is a
      # validation error
      #
      # @param (see Persistence#save)
      # @return [Record] self
      # @raise [RecordInvalid] if there are validation errors
      #
      def save!(attributes = {})
        tap do
          unless save(attributes)
            fail RecordInvalid, errors.full_messages.join("; ")
          end
        end
      end

      #
      # Set the given attributes and attempt to save, raising an exception if
      # there is a validation error
      #
      # @param (see Persistence#update_attributes)
      # @return (see #save!)
      # @raise (see #save!)
      def update_attributes!(attributes)
        self.attributes = attributes
        save!
      end
    end

    module Callback
      def valid?(context=nil)
        run_callbacks(:validation) { super context }
      end
    end
  end
end
