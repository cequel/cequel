module Cequel
  class Migration
    attr_accessor :mode #up or down

    def up?
      mode.to_s == 'up'
    end

    #DSL
    def k
      Cequel::Model.keyspace
    end

    def create_table name, options={}, columns#&block
      if up?
        cql = "CREATE TABLE #{name} (
          #{columns}
          )"
        cql << " WITH comparator=#{options[:comparator]}" if options[:comparator]
      else
        cql = "DROP TABLE #{name}"
      end
      k.execute cql
    end

    #Migration
    def up
      if respond_to? :change
        self.mode='up'
        change
      else
        puts "#{self.class.name}: no up or change method defined"
      end
    end

    def down
      if respond_to? :change
        self.mode='down'
        change
      else
        puts "#{self.class.name}: no down or change method defined"
      end
    end
  end
end