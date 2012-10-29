require 'cequel/model/readable_dictionary'

module Cequel

  module Model

    class Dictionary < ReadableDictionary

      class <<self

        def validation
          @validation ||= :text
        end

        def maps(options)
          @comparator, @validation = *options.first
        end

      end

      def each_pair(options = {})
        return super if !block_given? || @loaded
        new_columns = @changed_columns.dup
        super do |column, value|
          if @changed_columns.include?(column)
            new_columns.delete(column)
            yield column, @row[column]
          elsif !@deleted_columns.include?(column)
            yield column, value
          end
        end
        new_columns.each do |column|
          yield column, @row[column]
        end
        self
      end

      def [](column)
        if @loaded || @changed_columns.include?(column)
          @row[column]
        elsif !@deleted_columns.include?(column)
          value = super
          deserialize_value(column, value) if value
        end
      end

      def slice(*columns)
        super.tap do |slice|
          unless @loaded
            slice.merge!(@row.slice(*columns))
            @deleted_columns.each { |column| slice.delete(column) }
          end
        end
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

      private

      def setup(init_row = nil)
        super
        @changed_columns = Set[]
        @deleted_columns = Set[]
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
