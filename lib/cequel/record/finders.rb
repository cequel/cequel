# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # Cequel provides finder methods to construct scopes for looking up records
    # by primary key or secondary indexed columns.
    #
    # @example Example model class
    #   class Post
    #     key :blog_subdomain, :text
    #     key :id, :timeuuid, auto: true
    #     column :title, :text
    #     column :body, :text
    #     column :author_id, :uuid, index: true # this column has a secondary
    #                                           # index
    #   end
    #
    # @example Using some but not all primary key columns
    #   # return an Array of all posts with given subdomain (greedy load)
    #   Post.find_all_by_blog_subdomain(subdomain)
    #
    #   # return a {RecordSet} of all posts with the given subdomain (lazy
    #   # load)
    #   Post.with_subdomain(subdomain)
    #
    # @example Using all primary key columns
    #   # return the first post with the given subdomain and id, or nil if none
    #   Post.find_by_blog_subdomain_and_id(subdomain, id)
    #
    #   # return a record set to the post with the given subdomain and id
    #   # (one element array if exists, empty array otherwise)
    #   Post.with_blog_subdomain_and_id(subdomain, id)
    #
    # @example Chaining
    #   # return the first post with the given subdomain and id, or nil if none
    #   # Note that find_by_id can only be called on a scope that already has a
    #   # filter value for blog_subdomain
    #   Post.with_blog_subdomain(subdomain).find_by_id(id)
    #
    # @example Using a secondary index
    #   # return the first record with the author_id
    #   Post.find_by_author_id(id)
    #
    #   # return an Array of all records with the author_id
    #   Post.find_all_by_author_id(id)
    #
    #   # return a RecordSet scoped to the author_id
    #   Post.with_author_id(id)
    #
    # @since 1.2.0
    #
    module Finders
      private

      def key(*)
        if key_columns.any?
          def_key_finders('find_all_by', '.entries')
          undef_key_finders('find_by')
        end
        super
        def_key_finders('find_by', '.first')
        def_key_finders('with')
      end

      def column(name, type, options = {})
        super
        if options[:index]
          def_finder('with', [name])
          def_finder('find_by', [name], '.first')
          def_finder('find_all_by', [name], '.entries')
        end
      end

      def def_key_finders(method_prefix, scope_operation = '')
        def_finder(method_prefix, key_column_names, scope_operation)
        def_finder(method_prefix, key_column_names.last(1), scope_operation)
      end

      def def_finder(method_prefix, column_names, scope_operation = '')
        arg_names = column_names.join(', ')
        method_suffix = finder_method_suffix(column_names)
        column_filter_expr = column_names
          .map { |name| "#{name}: #{name}" }.join(', ')

        singleton_class.module_eval(<<-RUBY, __FILE__, __LINE__+1)
          def #{method_prefix}_#{method_suffix}(#{arg_names})
            where(#{column_filter_expr})#{scope_operation}
          end
        RUBY
      end

      def undef_key_finders(method_prefix)
        method_suffix = finder_method_suffix(key_column_names)
        method_name = "#{method_prefix}_#{method_suffix}"
        singleton_class.module_eval { undef_method(method_name) }
      end

      def finder_method_suffix(column_names)
        column_names.join('_and_')
      end
    end
  end
end
