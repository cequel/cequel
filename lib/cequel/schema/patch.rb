# -*- encoding : utf-8 -*-
module Cequel
  module Schema
    #
    # The set of changes needed to transform a table from its current form to a
    # desired form. `Patch`es are immutable.
    #
    class Patch
      extend Forwardable

      protected def initialize(changes)
        @changes = changes
      end

      attr_reader :changes

      def_delegator :changes, :empty?

      def statements
        changes.map(&:to_cql)
      end

      class AbstractChange
        protected def initialize(table, *post_init_args)
          @table = table

          post_init(*post_init_args)
        end

        attr_reader :table

        def to_cql
          fail NotImplementedError
        end

        def inspect
          "#<#{self.class.name} #{to_cql}>"
        end

        def ==(other)
          other.class == self.class &&
            other.table == self.table &&
            subclass_eql?(other)
        end

        def eql?(other)
          self == other
        end

        protected

        def subclass_eql?(other)
          fail NotImplementedError
        end
      end

      class SetTableProperties < AbstractChange
        protected def post_init()
        end

        def to_cql
          %Q|ALTER TABLE "#{table.name}" WITH #{properties.map(&:to_cql).join(' AND ')}|
        end

        def properties
          table.properties.values
        end

        protected

        def subclass_eql?(other)
          other.properties == propreties
        end
      end

      class DropIndex < AbstractChange
        protected def post_init(column_with_obsolete_idx)
          @index_name = column_with_obsolete_idx.index_name
        end

        attr_reader :index_name

        def to_cql
          %Q|DROP INDEX IF EXISTS "#{index_name}"|
        end

        protected

        def subclass_eql?(other)
          other.index_name == index_name
        end
      end

      class AddIndex < AbstractChange
        protected def post_init(column)
          @column = column
          @index_name = column.index_name
        end

        attr_reader :column, :index_name

        def to_cql
          %Q|CREATE INDEX "#{index_name}" ON "#{table.name}" ("#{column.name}")|
        end

        protected

        def subclass_eql?(other)
          other.column == column &&
            other.index_name == index_name
        end
      end

      class AddColumn < AbstractChange
        protected def post_init(column)
          @column = column
        end

        attr_reader :column

        def to_cql
          %Q|ALTER TABLE "#{table.name}" ADD #{column.to_cql}|
        end

        protected

        def subclass_eql?(other)
          other.column == column
        end
      end

      class RenameColumn < AbstractChange
        protected def post_init(old_column, new_column)
          @old_name, @new_name = old_column.name, new_column.name
        end

        attr_reader :old_name, :new_name

        def to_cql
          %Q|ALTER TABLE "#{table.name}" RENAME "#{old_name}" TO "#{new_name}"|
        end

        protected

        def subclass_eql?(other)
          other.old_name == old_name &&
            other.new_name == new_name
        end
      end
    end
  end
end
