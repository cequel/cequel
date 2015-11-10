module Cequel
  module Record
    class UserType
      attr_accessor :name, :types
      def initialize(name)
        @name = name
        @types = {}
      end

      def column(name, type, options = {})
        @types[name] = {type: type, options: options }
      end

      def build
        cql = "CREATE TYPE #{Cequel::Record.connection.configuration[:keyspace]}.#{@name} ("
        cql += @types.map { |name,t| " #{to_cql(name, t)} "}.join(",\n")
        cql += "\n);"
        begin
          Cequel::Record.connection.execute(cql)
        rescue Exception => e
          puts e.message
        end

        @types.each do |name, t|
          cql = "ALTER TYPE #{Cequel::Record.connection.configuration[:keyspace]}.#{@name} ADD #{to_cql(name, t)}; "
          begin
            Cequel::Record.connection.execute(cql)
          rescue Exception => e
            puts e.message
          end
        end
      end

      def to_cql(name, type)
        if type[:options][:frozen]
          "#{name} FROZEN <#{type[:type]}>"
        else
          "#{name} #{type[:type]}"
        end

      end
    end
  end
end
