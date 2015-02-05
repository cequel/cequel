# -*- encoding : utf-8 -*-
module Cequel
  module Util
    #
    # @api private
    #
    module HashAccessors
      def hattr_reader(hash, *attributes)
        attributes.each do |attribute|
          module_eval <<-RUBY, __FILE__, __LINE__ + 1
            def #{attribute}
              #{hash}[#{attribute.to_sym.inspect}]
            end
          RUBY
        end
      end

      def hattr_inquirer(hash, *attributes)
        attributes.each do |attribute|
          module_eval <<-RUBY, __FILE__, __LINE__ + 1
            def #{attribute}?
              !!#{hash}[#{attribute.to_sym.inspect}]
            end
          RUBY
        end
      end

      def hattr_writer(hash, *attributes)
        attributes.each do |attribute|
          module_eval <<-RUBY, __FILE__, __LINE__ + 1
            def #{attribute}=(value)
              #{hash}[#{attribute.to_sym.inspect}] = value
            end
          RUBY
        end
      end

      def hattr_accessor(hash, *attributes)
        hattr_reader(hash, *attributes)
        hattr_writer(hash, *attributes)
      end
    end

    #
    # Rails defines the `delegate` method directly on the `Module` class,
    # meaning that `Forwardable#delegate` overrides it any time a class
    # extends `Forwardable`.
    #
    # This module provides the methods Cequel uses from Forwardable,
    # specifically `#def_delegator` and `#def_delegators`, but reverts the
    # implementation of `#delegate` back to the one defined by ActiveSupport.
    #
    module Forwardable
      include ::Forwardable

      def delegate_with_argument_check(*args, &block)
        if args.size == 1
          delegate_without_argument_check(args.first)
        else
          Module.instance_method(:delegate).bind(self).call(*args, &block)
        end
      end
      alias_method_chain :delegate, :argument_check
    end
  end
end
