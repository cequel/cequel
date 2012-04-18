module Cequel

  module Model

    module Magic

      def self.scope(scope, columns_string, args)
        scope.where(extract_row_specifications(columns_string, args))
      end

      def self.find_or_create_by(scope, columns_string, args, &block)
        row_specifications = extract_row_specifications(columns_string, args)
        instance = scope.where(row_specifications).first
        if instance.nil?
          if args.length == 1 && args.first.is_a?(Hash)
            attributes = args.first
          else
            attributes = row_specifications
          end
          instance = scope.create(attributes, &block)
        end
        instance
      end

      def self.extract_row_specifications(columns_string, args)
        columns = columns_string.split('_and_').map { |column| column.to_sym }
        if args.length == 1 && args.first.is_a?(Hash)
          args.first.symbolize_keys.slice(*columns)
        else
          if columns.length != args.length
            raise ArgumentError,
              "wrong number of arguments(#{args.length} for #{columns.length})"
          end
          Hash[columns.zip(args)]
        end
      end

      def method_missing(method, *args, &block)
        case method.to_s
        when /^find_by_(\w+)$/
          Magic.scope(all, $1, args).first
        when /^find_all_by_(\w+)$/
          Magic.scope(all, $1, args).to_a
        when /^find_or_create_by_(\w+)$/
          Magic.find_or_create_by(all, $1, args, &block)
        else
          super
        end
      end

    end

  end

end
