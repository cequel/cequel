# -*- encoding : utf-8 -*-
require_relative 'spec_helper'
require 'forwardable'
require_relative '../../lib/cequel/util'

describe Cequel::Util do
  let(:test_klass) do
    module ActiveSupportAbbreviated
      # refine so that we don't monkey patch this for the whole test suite
      refine Module do
        # matches method signature for ActiveSupport.delegate
        # abbreviated form of that method with most options removed for brevity
        # https://github.com/rails/rails/blob/a19b13b61f7af612569943ec7d536185cbec875c/activesupport/lib/active_support/core_ext/module/delegation.rb#L171
        def delegate(*methods, to: nil, prefix: nil, allow_nil: nil, private: nil)
          unless to
            raise ArgumentError, "Delegation needs a target. Supply a keyword argument 'to' (e.g. delegate :hello, to: :greeter)."
          end
    
          location = caller_locations(1, 1).first
          file, line = location.path, location.lineno
    
          receiver = to.to_s
    
          method_def = []
          method_names = []
    
          methods.each do |method|
            method_name = prefix ? "#{method_prefix}#{method}" : method
            method_names << method_name.to_sym
    
            # Attribute writer methods only accept one argument. Makes sure []=
            # methods still accept two arguments.
            definition = \
              if /[^\]]=\z/.match?(method)
                "arg"
              else
                method_object =
                  begin
                    if to.is_a?(Module)
                      to.method(method)
                    elsif receiver == "self.class"
                      method(method)
                    end
                  rescue NameError
                    # Do nothing. Fall back to `"..."`
                  end
    
                if method_object
                  parameters = method_object.parameters
    
                  if (parameters.map(&:first) & [:opt, :rest, :keyreq, :key, :keyrest]).any?
                    "..."
                  else
                    defn = parameters.filter_map { |type, arg| arg if type == :req }
                    defn << "&block"
                    defn.join(", ")
                  end
                else
                  "..."
                end
              end
    
            # The following generated method calls the target exactly once, storing
            # the returned value in a dummy variable.
            #
            # Reason is twofold: On one hand doing less calls is in general better.
            # On the other hand it could be that the target has side-effects,
            # whereas conceptually, from the user point of view, the delegator should
            # be doing one call.
            method = method.to_s
            method_name = method_name.to_s
    
            method_def <<
              "def #{method_name}(#{definition})" <<
              "  _ = #{receiver}" <<
              "  _.#{method}(#{definition})" <<
              "rescue NoMethodError => e" <<
              "  if _.nil? && e.name == :#{method}" <<
              %(   raise DelegationError, "#{self}##{method_name} delegated to #{receiver}.#{method}, but #{receiver} is nil: \#{self.inspect}") <<
              "  else" <<
              "    raise" <<
              "  end" <<
              "end"
          end
          module_eval(method_def.join(";"), file, line)
          method_names
        end
      end
    end

    Class.new do
      include ActiveSupportAbbreviated
      extend ::Cequel::Util::Forwardable

      attr_reader :queue

      def initialize
        @queue = []    # prepare delegate object
      end

      # setup preferred interface, enqueue() and dequeue()...
      # Forwardable#def_delegator
      def_delegator :@queue, :push, :enqueue

      # ActiveSupport.delegate
      delegate :shift, to: :@queue, allow_nil: true
      alias :dequeue :shift
    end
  end

  subject { test_klass.new }

  it "successfully uses Forwardable's def_delegator method" do
    allow(subject.queue).to receive(:push).and_call_original
    expect(subject.enqueue(1)).to eq([1])
    expect(subject.queue).to have_received(:push)
  end

  it "successfully uses ActiveSupport's delegate method" do
    allow(subject.queue).to receive(:shift).and_call_original
    expect(subject.dequeue).to eq(nil)
    expect(subject.queue).to have_received(:shift)
  end
end
