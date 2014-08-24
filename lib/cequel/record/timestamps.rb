# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # This module provides `created_at` and `updated_at` functionality for
    # records. It does this in two ways:
    #
    # * If a record's primary key is a `timeuuid` with the `:auto` option set,
    #   the `created_at` method will return the time extracted from the primary
    #   key.
    # * Calling the `timestamps` macro in the class definition will define the
    #   `updated_at` and (if necessary) `created_at` columns, and set up
    #   lifecycle hooks to populate them appropriately.
    #
    # @example Record class with timestamps
    #   class Blog
    #     include Cequel::Record
    #     key :subdomain, :text
    #     column :name, :text
    #
    #     timestamps
    #   end
    #
    # @since 1.3.0
    #
    module Timestamps
      extend ActiveSupport::Concern

      #
      # Provides class methods for the Timestamps module
      #
      module ClassMethods
        protected

        def key(name, type, options = {})
          super
          if type == :timeuuid && options[:auto]
            module_eval(<<-RUBY, __FILE__, __LINE__+1)
              def created_at
                read_attribute(#{name.inspect}).try(:to_time)
              end
            RUBY
          end
        end

        def timestamps
          column :updated_at, :timestamp

          if method_defined?(:created_at)
            before_save :set_updated_at
          else
            column :created_at, :timestamp

            before_create :set_created_and_updated_at
            before_update :set_updated_at
          end
        end
      end

      private

      def set_created_and_updated_at
        now = Time.now
        self.created_at = now
        self.updated_at = now
      end

      def set_updated_at
        self.updated_at = Time.now
      end
    end
  end
end
