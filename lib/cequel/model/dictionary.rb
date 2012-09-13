module Cequel

  module Model

    class Dictionary

      class <<self

        attr_writer :column_family, :default_batch_size

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

        def default_batch_size
          @default_batch_size || 1000
        end

        def [](key)
          new(key)
        end
        private :new
      end

      include Enumerable

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
        if @loaded || @changed_columns.include?(column)
          @row[column]
        elsif !@deleted_columns.include?(column)
          value = scope.select(column).first[column]
          deserialize_value(column, value) if value
        end
      end

      def keys
        @loaded ? @row.keys : each_pair.map { |key, value| key }
      end

      def values
        @loaded ? @row.values : each_pair.map { |key, value| value }
      end

      def slice(*columns)
        if @loaded
          @row.slice(*columns)
        else
          {}.tap do |slice|
            row = scope.select(*columns).first.except(self.class.key_alias)
            row.each { |col, value| slice[col] = deserialize_value(col, value) }
            slice.merge!(@row.slice(*columns))
            @deleted_columns.each { |column| slice.delete(column) }
          end
        end
      end

      def destroy
        scope.delete
        setup
      end

      def save
        updates = {}
        @changed_columns.each do |column|
          updates[column] = serialize_value(@row[column])
        end
        scope.update(updates) if updates.any?
        scope.delete(*@deleted_columns.to_a) if @deleted_columns.any?
        @changed_columns.clear
        @deleted_columns.clear
        self
      end

      def each_pair(options = {}, &block)
        return Enumerator.new(self, :each_pair, options) unless block
        return @row.each_pair(&block) if @loaded
        new_columns = @changed_columns.dup
        batch_size = options[:batch_size] || self.class.default_batch_size
        each_slice(batch_size) do |batch_results|
          batch_results.each_pair do |key, value|
            if @changed_columns.include?(key)
              new_columns.delete(key)
              yield key, @row[key]
            elsif !@deleted_columns.include?(key)
              yield key, value
            end
          end
        end
        new_columns.each do |key|
          yield key, @row[key]
        end
      end

      def each(&block)
        each_pair(&block)
      end

      def each_slice(batch_size)
        batch_scope = scope.select(:first => batch_size)
        key_alias = self.class.key_alias
        last_key = nil
        begin
          batch_results = batch_scope.first
          batch_results.delete(key_alias)
          result_length = batch_results.length
          batch_results.delete(last_key) unless last_key.nil?
          yield deserialize_row(batch_results)
          last_key = batch_results.keys.last
          batch_scope = batch_scope.select(:from => last_key)
        end while result_length == batch_size
      end

      def load
        @row = {}
        each_pair { |column, value| @row[column] = value }
        @loaded = true
        self
      end

      def loaded?
        !!@loaded
      end

      private

      def setup
        @row = {}
        @changed_columns = Set[]
        @deleted_columns = Set[]
      end

      def scope
        self.class.column_family.where(self.class.key_alias => @key)
      end

      #
      # Subclasses may override this method to implement custom serialization
      # strategies
      #
      def serialize_value(value)
        value
      end

      #
      # Subclasses may override this method to implement custom deserialization
      # strategies
      #
      def deserialize_value(column, value)
        value
      end

      def deserialize_row(row)
        {}.tap do |slice|
          row.each_pair do |column, value|
            slice[column] = deserialize_value(column, value)
          end
        end
      end

    end

  end

end
