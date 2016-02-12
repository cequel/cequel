module Cequel
  module Record
    class UserType

      TYPES = {}

      attr_accessor :name, :types
      def initialize(name)
        @name = name
      end

      def column(name, type, options = {})
        Cequel::Record::UserType::TYPES[@name] ||= {}
        Cequel::Record::UserType::TYPES[@name][name] = {type: type, options: options }
      end

      def list(name, type, options = {})
        Cequel::Record::UserType::TYPES[@name] ||= {}
        Cequel::Record::UserType::TYPES[@name][name] = {type: type, options: options, collection_type: :list }
      end

      def set(name, type, options = {})
        Cequel::Record::UserType::TYPES[@name] ||= {}
        Cequel::Record::UserType::TYPES[@name][name] = {type: type, options: options, collection_type: :set }
      end      

      def self.build
        Cequel::Record::UserType::TYPES.each do |type_name, h|
          cql = "CREATE TYPE #{Cequel::Record.connection.configuration[:keyspace]}.#{type_name} ("
          cql += h.map { |name,t| " #{to_cql(name, t)} "}.join(",\n")
          cql += "\n);"
          begin
            Cequel::Record.connection.execute(cql)
          rescue Exception => e
            puts e.message
          end

          h.each do |name, t|
            cql = "ALTER TYPE #{Cequel::Record.connection.configuration[:keyspace]}.#{type_name} ADD #{to_cql(name, t)}; "
            begin
              Cequel::Record.connection.execute(cql)
            rescue Exception => e
              puts e.message
            end
          end
        end
      end

      def self.to_cql(name, type)
        if type[:options][:frozen]
          t = "FROZEN<#{type[:type]}>"
        else
          t = type[:type]
        end

        if type[:collection_type]
          t = "#{type[:collection_type].upcase}<#{t}>"
        end
        "#{name} #{t}"
      end
    end
  end
end


