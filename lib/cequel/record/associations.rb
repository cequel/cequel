# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # Cequel records can have parent-child relationships defined by
    # {ClassMethods#belongs_to belongs_to} and {ClassMethods#has_many has_many}
    # associations. Unlike in a relational database ORM, associations are not
    # represented by foreign keys; instead they use CQL3's compound primary
    # keys. A child object's primary key begins with it's parent's primary key.
    #
    # In the below example, the `blogs` table has a one-column primary key
    # `(subdomain)`, and the `posts` table has a two-column primary key
    # `(blog_subdomain, permalink)`. All posts that belong to the blog with
    # subdomain `"cassandra"` will have `"cassandra"` as their
    # `blog_subdomain`.
    #
    # @example Blogs and Posts
    #
    #   class Blog
    #     include Cequel::Record
    #
    #     key :subdomain, :text
    #
    #     column :name, :text
    #
    #     has_many :posts
    #   end
    #
    #   class Post
    #     include Cequel::Record
    #
    #     # This defines the first primary key column as `blog_subdomain`.
    #     # Because `belongs_to` associations implicitly define columns in the
    #     # primary key, it must come before any explicit key definition. For
    #     # the same reason, a Record class can only have a single `belongs_to`
    #     # declaration.
    #     belongs_to :blog
    #
    #     # We also define an additional primary key column so that each post
    #     # has a unique compound primary key
    #     key :permalink
    #
    #     column :title, :text
    #     column :body, :text
    #   end
    #
    #   blog = Blog.new(subdomain: 'cassandra')
    #   post = blog.posts.new(permalink: 'cequel')
    #   post.blog_subdomain #=> "cassandra"
    #
    # @since 1.0.0
    #
    module Associations
      extend ActiveSupport::Concern

      included do
        class_attribute :parent_association
        class_attribute :child_associations
        self.child_associations = {}
      end

      #
      # Class macros for declaring associations
      #
      # @see Associations
      #
      module ClassMethods
        include Util::Forwardable

        # @!attribute parent_association
        #   @return [BelongsToAssociation] association declared by
        #     {#belongs_to}
        # @!attribute child_associations
        #   @return [Hash<Symbol,HasManyAssociation>] associations declared by
        #     {#has_many}

        #
        # Declare the parent association for this record. The name of the class
        # is inferred from the name of the association. The `belongs_to`
        # declaration also serves to define key columns, which are derived from
        # the key columns of the parent class. So, if the parent class `Blog`
        # has a primary key `(subdomain)`, this will declare a key column
        # `blog_subdomain` of the same type.
        #
        # If the parent class has multiple keys, e.g. it belongs to a parent
        # class, defining a `partition: true` option will declare all of the
        # parent's keys as partition key columns for this class.
        #
        # Parent associations are read/write, so declaring `belongs_to :blog`
        # will define a `blog` getter and `blog=` setter, which will update the
        # underlying key column. Note that a record's parent cannot be changed
        # once the record has been saved.
        #
        # @param name [Symbol] name of the parent association
        # @param options [Options] options for association
        # @option (see BelongsToAssociation#initialize)
        # @return [void]
        #
        # @see Associations
        #
        def belongs_to(name, options = {})
          if parent_association
            fail InvalidRecordConfiguration,
                 "Can't declare more than one belongs_to association"
          end
          if table_schema.key_columns.any?
            fail InvalidRecordConfiguration,
                 "belongs_to association must be declared before declaring " \
                 "key(s)"
          end
          
          key_options = options.extract!(:partition)

          self.parent_association =
            BelongsToAssociation.new(self, name.to_sym, options)

          parent_association.association_key_columns.each do |column|
            key :"#{name}_#{column.name}", column.type, key_options
          end
          def_parent_association_accessors
        end

        #
        # Declare a child association. The child association should have a
        # `belongs_to` referencing this class or, at a minimum, must have a
        # primary key whose first N columns have the same types as the N
        # columns in this class's primary key.
        #
        # `has_many` associations are read-only, so `has_many :posts` will
        # define a `posts` reader but not a `posts=` writer; and the collection
        # returned by `posts` will be immutable.
        #
        # @param name [Symbol] plural name of association
        # @param options [Options] options for association
        # @option (see HasManyAssociation#initialize)
        # @return [void]
        #
        # @see Associations
        #
        def has_many(name, options = {})
          association = HasManyAssociation.new(self, name.to_sym, options)
          self.child_associations =
            child_associations.merge(name => association)
          def_child_association_reader(association)
        end

        private

        def def_parent_association_accessors
          def_parent_association_reader
          def_parent_association_writer
        end

        def def_parent_association_reader
          def_delegator 'self', :read_parent_association,
                        parent_association.name
        end

        def def_parent_association_writer
          def_delegator 'self', :write_parent_association,
                        "#{parent_association.name}="
        end

        def def_child_association_reader(association)
          module_eval <<-RUBY, __FILE__, __LINE__+1
            def #{association.name}(reload = false)
              read_child_association(#{association.name.inspect}, reload)
            end
          RUBY
        end
      end

      #
      # @private
      #
      def destroy(*)
        super.tap do
          self.class.child_associations.each_value do |association|
            case association.dependent
            when :destroy
              __send__(association.name).destroy_all
            when :delete
              __send__(association.name).delete_all
            end
          end
        end
      end

      private

      def read_parent_association
        ivar_name = parent_association.instance_variable_name
        if instance_variable_defined?(ivar_name)
          return instance_variable_get(ivar_name)
        end
        parent_key_values = key_values
          .first(parent_association.association_key_columns.length)
        if parent_key_values.none? { |value| value.nil? }
          clazz = parent_association.association_class
          parent = parent_key_values.reduce(clazz) do |record_set, key_value|
            record_set[key_value]
          end
          instance_variable_set(ivar_name, parent)
        end
      end

      def write_parent_association(parent)
        unless parent.is_a?(parent_association.association_class)
          fail ArgumentError,
               "Wrong class for #{parent_association.name}; expected " \
               "#{parent_association.association_class.name}, got " \
               "#{parent.class.name}"
        end
        instance_variable_set "@#{parent_association.name}", parent
        key_column_names = self.class.key_column_names
        parent.key_attributes
          .zip(key_column_names) do |(parent_column_name, value), column_name|
            if value.nil?
              fail ArgumentError,
                   "Can't set parent association " \
                   "#{parent_association.name.inspect} " \
                   "without value in key #{parent_column_name.inspect}"
            end
            write_attribute(column_name, value)
          end
      end

      def read_child_association(association_name, reload = false)
        association = child_associations[association_name]
        ivar = association.instance_variable_name
        if !reload && instance_variable_defined?(ivar)
          return instance_variable_get(ivar)
        end

        base_scope = association.association_class
        association_record_set =
          key_values.reduce(base_scope) do |record_set, key_value|
            record_set[key_value]
          end

        instance_variable_set(
          ivar, AssociationCollection.new(association_record_set))
      end
    end
  end
end
