# -*- encoding : utf-8 -*-
require 'active_model'
require 'weakref'

require 'cequel'
require 'cequel/record/errors'
require 'cequel/record/schema'
require 'cequel/record/properties'
require 'cequel/record/collection'
require 'cequel/record/persistence'
require 'cequel/record/bulk_writes'
require 'cequel/record/record_set'
require 'cequel/record/data_set_builder'
require 'cequel/record/bound'
require 'cequel/record/lazy_record_collection'
require 'cequel/record/scoped'
require 'cequel/record/finders'
require 'cequel/record/associations'
require 'cequel/record/association_collection'
require 'cequel/record/belongs_to_association'
require 'cequel/record/has_many_association'
require 'cequel/record/mass_assignment'
require 'cequel/record/callbacks'
require 'cequel/record/validations'
require 'cequel/record/dirty'
require 'cequel/record/conversion'
require 'cequel/record/timestamps'

require 'cequel/record'

if defined? Rails
  require 'cequel/record/railtie'
end

module Cequel
  #
  # Cequel::Record is an active record-style data modeling library and
  # object-row mapper. Model classes inherit from Cequel::Record, define their
  # columns in the class definition, and have access to a full and robust set
  # of read and write functionality.
  #
  # Individual components are documented in their respective modules. See below
  # for links.
  #
  # @example A Record class showing off many of the possibilities
  #   class Post
  #     include Cequel::Record
  #
  #     belongs_to :blog
  #     key :id, :timeuuid, auto: true
  #     column :title, :text
  #     column :body, :text
  #     column :author_id, :uuid, index: true
  #     set :categories
  #
  #     has_many :comments, dependent: destroy
  #
  #     after_create :notify_followers
  #
  #     validates :title, presence: true
  #
  #     def self.for_author(author_id)
  #       where(:author_id, author_id)
  #     end
  #   end
  #
  # @see Properties Defining properties
  # @see Collection Collection columns
  # @see Associations Defining associations between records
  # @see Persistence Creating, updating, and destroying records
  # @see BulkWrites Updating and destroying records in bulk
  # @see RecordSet Loading records from the database
  # @see Finders Magic finder methods
  # @see MassAssignment Mass-assignment protection and strong attributes
  # @see Callbacks Lifecycle hooks
  # @see Validations
  # @see Dirty Dirty attribute tracking
  #
  module Record
    extend ActiveSupport::Concern
    extend Util::Forwardable

    included do
      include Properties
      include Schema
      include Persistence
      include Associations
      include Scoped
      extend Finders
      include MassAssignment
      include Callbacks
      include Validations
      include Dirty
      extend ActiveModel::Naming
      include Conversion
      include ActiveModel::Serializers::JSON
      include ActiveModel::Serializers::Xml
      include Timestamps
    end

    #
    # Empty, but third-party libraries can add class-level functionality here
    #
    module ClassMethods
    end

    class <<self
      # @return [Metal::Keyspace] the keyspace used for record persistence
      attr_accessor :connection

      #
      # Establish a connection with the given configuration
      #
      # @param (see Cequel.connect)
      # @option (see Cequel.connect)
      # @return [void]
      #
      def establish_connection(configuration)
        self.connection = Cequel.connect(configuration)
      end

      # @return [Array<Class>] All the record classes that are
      #   currently defined.
      def descendants
        weak_descendants.map do |clazz|
          begin
            clazz.__getobj__ if clazz.weakref_alive?
          rescue WeakRef::RefError
            nil
          end
        end.compact
      end

      # Hook called when new record classes are created.
      def included(base)
        weak_descendants << WeakRef.new(base)
      end

      private

      def weak_descendants
        @weak_descendants ||= []
      end
    end
  end
end
