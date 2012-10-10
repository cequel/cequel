module Cequel
  class Migrator
    cattr_accessor :migration_directory
    def self.migration_directory
      @migration_directory || 'db/cassandra'
    end

    def self.available_migrations
      Dir["#{Rails.root}/#{migration_directory}/*.rb"]
    end

    # outsanding request for [up, down]
    def self.divide_migrations(versions_already_run, available_files, target_version=nil)
      versions_already_run = versions_already_run.map(&:to_i)
      target_version = target_version.blank? ? (1.0/0) : target_version.to_i

      to_up = []
      to_down = []
      available_files.select do |mf|
        version = separate_version(mf).first.to_i

        #puts "#{versions_already_run.inspect}.include?(#{version}): #{versions_already_run.include?(version)}"
        if version <= target_version
          to_up   << mf if ! versions_already_run.include?(version)
        else
          to_down << mf if versions_already_run.include?(version)
        end
      end
      [to_up.sort, to_down.sort {|a,b| b <=> a }]
    end

    #[version, class_name]
    def self.separate_version(migration_file)
      migration_file =~ /^(?:.*\/)?([^_]+)_([^.]*)(?:\.rb)?$/
      [$1, $2.camelcase]
    end

    def self.run_migration(migration_file, direction)
      load migration_file
      version, class_name = separate_version(migration_file)
      puts "#{direction.to_s == 'up' ? 'running' : 'downgrading'} #{class_name} (#{version})"
      begin
        class_name.constantize.new().send(direction)
        Schema.send(direction, version)
      ensure
        Object.send(:remove_const, class_name)
      end
    end

    def self.run_all
      to_up, to_down = divide_migrations(Schema.versions_already_run, available_migrations, ENV['VERSION'])

      to_up.each do |migration_file_name|
        run_migration(migration_file_name, :up)
      end

      to_down.each do |migration_file_name|
        run_migration(migration_file_name, :down)
      end
    end

    class Schema
      def self.versions_already_run
        Cequel::Model.keyspace[:schema_migrations].where(migration: 'migration').first.values - %w(migration)
      end
      def self.up(version)
        Cequel::Model.keyspace[:schema_migrations].insert(migration: 'migration', version.to_s => version.to_i)
      end

      def self.down(version)
        #TODO: broken
        Cequel::Model.keyspace[:schema_migrations].where(migration: 'migration').delete(version.to_s)
      end
    end
  end
end