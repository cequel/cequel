module Cequel

  module Model

    class Dictionary

      class <<self

        attr_writer :column_family

        def key_alias
          @key_alias ||= :KEY
        end

        def key_type
          @key_type ||= :text
        end

        def comparator
          @comparator ||= :text
        end

        def validation
          @validation ||= :text
        end

        def key(key_alias, type)
          @key_alias, @key_type = key_alias, type

          module_eval(<<-RUBY)
          def #{key_alias.downcase}
            @key
          end
          RUBY
        end

        def maps(options)
          @comparator, @validation = *options.first
        end

        def column_family
          return @column_family if @column_family
          self.column_family_name = name.underscore.to_sym
          @column_family
        end

        def column_family_name=(column_family_name)
          self.column_family = Cequel::Model.keyspace[column_family_name]
        end

        def [](key)
          new(key)
        end
        private :new
      end

      def initialize(key)
        @key = key
        setup
      end

      def []=(column, value)
        if value.nil?
          @deleted_columns << column
          @changed_columns.delete(column)
        else
          @changed_columns << column
          @deleted_columns.delete(column)
        end
        @row[column] = value
      end

      def [](column)
        @row[column]
      end

      def destroy
        scope.delete
        setup
      end

      def save
        updates = @row.slice(*@changed_columns)
        scope.update(updates) if updates.any?
        scope.delete(*@deleted_columns.to_a) if @deleted_columns.any?
        @changed_columns.clear
        @deleted_columns.clear
        self
      end

      def load(*args)
        return load_all if args.empty?
        row = scope.select(*args).first
        row.delete(self.class.key_alias)
        if args.length == 1 && (Hash === args.first || Range === args.first)
          @row.merge!(row)
        else # set values, and also set missing columns to nil to prevent double lookup
          args.each { |column| @row[column] = row[column] }
        end
        self
      end

      def load_each_pair(options = {}, &block)
        return Enumerator.new(self, :load_each_pair, options) unless block
        batch_size = options[:batch_size] || 1000
        batch_scope = scope.select(:first => batch_size)
        @row = {}
        begin
          batch_results = batch_scope.first
          batch_results.each_pair(&block)
          batch_scope = batch_scope.select(:from => batch_results.keys.last)
        end while batch_results.length == batch_size
      end

      def each_pair(&block)
        @row.each_pair(&block)
      end

      private

      def setup
        @row = Hash.new do |h, k|
          self.load(k)
          @row[k]
        end
        @changed_columns = Set[]
        @deleted_columns = Set[]
      end

      def scope
        self.class.column_family.where(self.class.key_alias => @key)
      end

      def load_all
        @row = {}
        load_each_pair { |column, value| @row[column] = value }
        self
      end

    end

  end

end
