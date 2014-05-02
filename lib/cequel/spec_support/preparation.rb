module Cequel
  module SpecSupport
    # Database preparation behavior that is useful for spec/test suites.
    #
    # Adding the following code to the bottom of one's
    # `spec_helper.rb` (below the `RSpec.configure` block) ensures a
    # clean and fully synced test db before each test run.
    #
    #     # one time database setup
    #     Cequel::SpecSupport::Preparation.instance.tap do |prep|
    #       prep.model_dirs << Rails.root + "app/models"
    #
    #       prep.drop_keyspace
    #       prep.create_keyspace
    #       prep.sync_schema
    #     end
    class Preparation
      include Singleton

      def initialize()
        @model_dirs = []
      end

      # Ensure the current keyspace does not exist.
      def drop_keyspace
        Cequel::Record.connection.schema.drop!

      rescue Cql::QueryError => e
        raise unless /exist/i === e.message
        # it's just complaining about the keyspace not existing so
        # mission accomplished.
      end

      # Ensure that the necessary keyspace exists.
      def create_keyspace
        Cequel::Record.connection.schema.create!

      rescue Cql::QueryError => e
        raise unless /exist/i === e.message
        # it's just complaining about the keyspace already existing so
        # mission accomplished.
      end

      # Ensure that the necessary column families exist and match the
      # models.
      def sync_schema
        record_classes.each do |a_record_class|
          a_record_class.synchronize_schema
          puts "Synchronized schema for #{a_record_class.name}"
        end
      end

      attr_reader :model_dirs

      protected

      # @return [Array<Class>] all Cequel record classes
      def record_classes
        load_all_models

        ObjectSpace.each_object
          .select {|an_obj| begin
                              an_obj.kind_of?(Class) && Cequel::Record > an_obj
                            rescue TypeError=> e
                              # something was masquerading as a class but wasn't really.
                              false
                            end }
      end

      # Loads all files in the models directory under the assumption
      # that Cequel record classes live there.
      def load_all_models
        model_dirs.each do |a_directory|
          Dir.glob(Pathname(a_directory).join("**", "*.rb")).each do |file_name|
            require_dependency(file_name)
          end
        end
      end
    end
  end
end