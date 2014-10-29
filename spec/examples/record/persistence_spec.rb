# -*- encoding : utf-8 -*-
require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Record::Persistence do
  model :Blog do
    key :subdomain, :text
    column :name, :text
    column :description, :text
    column :owner_id, :uuid
  end

  model :Post do
    key :blog_subdomain, :text
    key :permalink, :text
    column :title, :text
    column :body, :text
    column :author_id, :uuid
  end

  context 'simple keys' do
    subject { cequel[Blog.table_name].where(:subdomain => 'cequel').first }

    let!(:blog) do
      Blog.new do |blog|
        blog.subdomain = 'cequel'
        blog.name = 'Cequel'
        blog.description = 'A Ruby ORM for Cassandra 1.2'
      end.tap(&:save)
    end

    describe 'new record' do
      specify { expect(Blog.new).not_to be_persisted }
      specify { expect(Blog.new).to be_transient }
    end

    describe '#save' do
      context 'on create' do
        it 'should save row to database' do
          expect(subject[:name]).to eq('Cequel')
        end

        it 'should mark row persisted' do
          expect(blog).to be_persisted
        end

        it 'should fail fast if keys are missing' do
          expect {
            Blog.new.save
          }.to raise_error(Cequel::Record::MissingKeyError)
        end

        it 'should save with specified consistency' do
          expect_query_with_consistency(/INSERT/, :one) do
            Blog.new do |blog|
              blog.subdomain = 'cequel'
              blog.name = 'Cequel'
            end.save(consistency: :one)
          end
        end

        it 'should save with specified TTL' do
          Blog.new(subdomain: 'cequel', name: 'Cequel').save(ttl: 10)
          expect(cequel[Blog.table_name].select_ttl(:name).first.ttl(:name))
            .to be_within(0.1).of(9.9)
        end

        it 'should save with specified timestamp' do
          timestamp = 1.minute.from_now
          Blog.new(subdomain: 'cequel-create-ts', name: 'Cequel')
            .save(timestamp: timestamp)
          expect(cequel[Blog.table_name].select_timestamp(:name).first.timestamp(:name))
            .to eq((timestamp.to_f * 1_000_000).to_i)
          Blog.connection.schema.truncate_table(Blog.table_name)
        end
      end

      context 'on update' do
        uuid :owner_id

        before do
          blog.name = 'Cequel 1.0'
          blog.owner_id = owner_id
          blog.description = nil
          blog.save
        end

        it 'should change existing column value' do
          expect(subject[:name]).to eq('Cequel 1.0')
        end

        it 'should add new column value' do
          expect(subject[:owner_id]).to eq(owner_id)
        end

        it 'should remove old column values' do
          expect(subject[:description]).to be_nil
        end

        it 'should not allow changing key values' do
          expect {
            blog.subdomain = 'soup'
            blog.save
          }.to raise_error(ArgumentError)
        end

        it 'should allow setting a key value to the same thing it already is' do
          expect {
            blog.subdomain = 'cequel'
            blog.save
          }.to_not raise_error
        end

        it 'should save with specified consistency' do
          expect_query_with_consistency(/UPDATE/, :one) do
            blog.name = 'Cequel'
            blog.save(consistency: :one)
          end
        end

        it 'should save with specified TTL' do
          blog.name = 'Cequel 1.4'
          blog.save(ttl: 10)
          expect(cequel[Blog.table_name].select_ttl(:name).first.ttl(:name)).
            to be_within(0.1).of(9.9)
        end

        it 'should save with specified timestamp' do
          timestamp = 1.minute.from_now
          blog.name = 'Cequel 1.4'
          blog.save(timestamp: timestamp)
          expect(cequel[Blog.table_name].select_timestamp(:name).first.timestamp(:name))
            .to eq((timestamp.to_f * 1_000_000).to_i)
          Blog.connection.schema.truncate_table(Blog.table_name)
        end

        it 'should not query database if no attributes have been changed' do
          disallow_queries!
          blog.save
        end

        it 'should not mark itself as clean if save failed at Cassandra level' do
          blog.name = 'Pizza'
          with_client_error(Cql::QueryError.new(1, 'error')) do
            begin
              blog.save
            rescue Cql::QueryError
            end
          end
          blog.save
          expect(subject[:name]).to eq('Pizza')
        end
      end
    end

    describe '::create' do
      uuid :owner_id

      describe 'with block' do
        let! :blog do
          Blog.create do |blog|
            blog.subdomain = 'big-data'
            blog.name = 'Big Data'
          end
        end

        it 'should initialize with block' do
          expect(blog.name).to eq('Big Data')
        end

        it 'should save instance' do
          expect(Blog.find(blog.subdomain).name).to eq('Big Data')
        end

        it 'should fail fast if keys are missing' do
          expect {
            Blog.create do |blog|
              blog.name = 'Big Data'
            end
          }.to raise_error(Cequel::Record::MissingKeyError)
        end
      end

      describe 'with attributes' do
        let!(:blog) do
          Blog.create(:subdomain => 'big-data', :name => 'Big Data')
        end

        it 'should initialize with block' do
          expect(blog.name).to eq('Big Data')
        end

        it 'should save instance' do
          expect(Blog.find(blog.subdomain).name).to eq('Big Data')
        end

        it 'should fail fast if keys are missing' do
          expect {
            Blog.create(:name => 'Big Data')
          }.to raise_error(Cequel::Record::MissingKeyError)
        end
      end
    end

    describe '#update_attributes' do
      let! :blog do
        Blog.create(:subdomain => 'big-data', :name => 'Big Data')
      end

      before { blog.update_attributes(:name => 'The Big Data Blog') }

      it 'should update instance in memory' do
        expect(blog.name).to eq('The Big Data Blog')
      end

      it 'should save instance' do
        expect(Blog.find(blog.subdomain).name).to eq('The Big Data Blog')
      end

      it 'should not allow updating key values' do
        expect { blog.update_attributes(:subdomain => 'soup') }
          .to raise_error(ArgumentError)
      end
    end

    describe '#destroy' do
      before { blog.destroy }

      it 'should delete entire row' do
        expect(subject).to be_nil
      end

      it 'should mark record transient' do
        expect(blog).to be_transient
      end

      it 'should destroy with specified consistency' do
        blog = Blog.create(:subdomain => 'big-data', :name => 'Big Data')
        expect_query_with_consistency(/DELETE/, :one) do
          blog.destroy(consistency: :one)
        end
      end

      it 'should destroy with specified timestamp' do
        blog = Blog.create(subdomain: 'big-data', name: 'Big Data')
        blog.destroy(timestamp: 1.minute.ago)
        expect(cequel[Blog.table_name].where(subdomain: 'big-data').first).to be
      end
    end
  end

  context 'compound keys' do
    subject do
      cequel[Post.table_name].
        where(:blog_subdomain => 'cassandra', :permalink => 'cequel').first
    end

    let!(:post) do
      Post.new do |post|
        post.blog_subdomain = 'cassandra'
        post.permalink = 'cequel'
        post.title = 'Cequel'
        post.body = 'A Ruby ORM for Cassandra 1.2'
      end.tap(&:save)
    end

    describe '#save' do
      context 'on create' do
        it 'should save row to database' do
          expect(subject[:title]).to eq('Cequel')
        end

        it 'should mark row persisted' do
          expect(post).to be_persisted
        end

        it 'should fail fast if parent keys are missing' do
          expect {
            Post.new do |post|
              post.permalink = 'cequel'
              post.title = 'Cequel'
            end.tap(&:save)
          }.to raise_error(Cequel::Record::MissingKeyError)
        end

        it 'should fail fast if row keys are missing' do
          expect {
            Post.new do |post|
              post.blog_subdomain = 'cassandra'
              post.title = 'Cequel'
            end.tap(&:save)
          }.to raise_error(Cequel::Record::MissingKeyError)
        end
      end

      context 'on update' do
        uuid :author_id

        before do
          post.title = 'Cequel 1.0'
          post.author_id = author_id
          post.body = nil
          post.save
        end

        it 'should change existing column value' do
          expect(subject[:title]).to eq('Cequel 1.0')
        end

        it 'should add new column value' do
          expect(subject[:author_id]).to eq(author_id)
        end

        it 'should remove old column values' do
          expect(subject[:body]).to be_nil
        end

        it 'should not allow changing parent key values' do
          expect {
            post.blog_subdomain = 'soup'
            post.save
          }.to raise_error(ArgumentError)
        end

        it 'should not allow changing row key values' do
          expect {
            post.permalink = 'soup-recipes'
            post.save
          }.to raise_error(ArgumentError)
        end
      end
    end

    describe '#destroy' do
      before { post.destroy }

      it 'should delete entire row' do
        expect(subject).to be_nil
      end

      it 'should mark record transient' do
        expect(post).to be_transient
      end
    end
  end
end
