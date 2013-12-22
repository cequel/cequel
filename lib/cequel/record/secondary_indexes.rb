module Cequel
  module Record
    #
    # Data columns may be given secondary indexes. This allows you to query the
    # table for records where the indexed column contains a single value.
    # Secondary indexes are not nearly as flexible as primary keys: you cannot
    # query for multiple values or for ranges of values. You also cannot combine
    # a secondary index restriction with a primary key restriction in the same
    # query, nor can you combine more than one secondary index restrictions in
    # the same query.
    #
    # Secondary indexes are fairly expensive for Cassandra and should only be
    # defined where needed.
    #
    # @example Defining a secondary index
    #   class Post
    #     belongs_to :blog
    #     key :id, :timeuuid, auto: true
    #     column :title, :text
    #     column :body, :text
    #     column :author_id, :uuid, index: true # this column has a secondary index
    #   end
    #
    # @example Using a secondary index
    #   Post.where(:author_id, id)
    #
    # @since 1.0.0
    #
    module SecondaryIndexes
      # @private
      def column(name, type, options = {})
        super
        name = name.to_sym
        if options[:index]
          instance_eval <<-RUBY, __FILE__, __LINE__+1
            def with_#{name}(value)
              all.where(#{name.inspect}, value)
            end

            def find_by_#{name}(value)
              with_#{name}(value).first
            end

            def find_all_by_#{name}(value)
              with_#{name}(value).to_a
            end
          RUBY
        end
      end
    end
  end
end
