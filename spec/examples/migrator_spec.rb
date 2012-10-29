describe Cequel::Migrator do
  context "#separate_versions" do
    it "should parse migration file names" do
      Cequel::Migrator.separate_version("505_class_name").should == ["505","ClassName"]
    end
    it "should parse migration file names with paths" do
      Cequel::Migrator.separate_version("dir/to/thingy/505_class_name").should == ["505","ClassName"]
    end
    it "should parse migration file names with rb" do
      Cequel::Migrator.separate_version("505_class_name.rb").should == ["505","ClassName"]
    end
  end

  context "#divide_migrations up" do
    it "should say we need to run versions that havent run" do
      Cequel::Migrator.divide_migrations(%w(), %w(44_go 55_go)).should == [%w(44_go 55_go),[]]
    end

    it "should say we need to run versions that havent run with target" do
      Cequel::Migrator.divide_migrations(%w(), %w(44_go 55_go), "55").should == [%w(44_go 55_go),[]]
    end

    it "should say we need to run versions that are under and including a target" do
      Cequel::Migrator.divide_migrations(%w(), %w(44_go 55_go 66_no_go), "55").should == [%w(44_go 55_go),[]]
    end

    it "should say we dont need to run versions that we have run" do
      $debug=true
      Cequel::Migrator.divide_migrations(%w(44 55), %w(44_dontgo 55_dontgo)).should == [[],[]]
    end

    it "should say we dont need to run versions that we have not run but under the version" do
      Cequel::Migrator.divide_migrations(%w(), %w(44_dontgo 55_dontgo), "33").should == [[],[]]
    end

    it "should upgrade to a version" do
      Cequel::Migrator.divide_migrations(%w(44), %w(44_no 55_go 66_no), "55").should == [%w(55_go),[]]
    end
  end

  context "#divide_migrations down" do
    it "should downgrade to a version" do
      Cequel::Migrator.divide_migrations(%w(44 55), %w(44_no_go 55_go), "44").should == [[], %w(55_go)]
    end
    it "should downgrade to base" do
      Cequel::Migrator.divide_migrations(%w(44 55), %w(44_go 55_go), "0").should == [[], %w(55_go 44_go)]
    end
    it "should no op to a base when at base" do
      Cequel::Migrator.divide_migrations(%w(), %w(44_no_go 55_no_go), "0").should == [[], []]
    end
  end
  context "migrations" do
    before do
      Cequel::Migrator.migration_directory="spec/migrations"
    end
    describe "migration schema" do
      #Cequel::Migrator
    end
  end
end