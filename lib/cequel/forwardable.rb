# -*- encoding : utf-8 -*-
require "forwardable"

module Forwardable
  if Module.respond_to? :delegate
    module MultiDelegate
      #
      # select implements of delegate
      # Forwardable#instance_delegate or
      # ActiveSupport's Module#delegate
      #

      def self.included(base)
        base.alias_method_chain :delegate, :argument_check
      end

      #
      # ActiveSupport's Module#delegate
      #
      AS_DELEGATE = Module.instance_method(:delegate)

      #
      # Select implements of delegate from follows,
      # Forwardable#instance_delegate or ActiveSupport's Module#delegate
      #
      def delegate_with_argument_check(*args)
        if args.size == 1
          delegate_without_argument_check args.first
        else
          AS_DELEGATE.bind(self).call *args
        end
      end
    end
    include MultiDelegate
  end
end
