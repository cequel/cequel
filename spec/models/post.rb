class Post

  include Cequel::Model

  key :id, :integer
  column :title, :varchar
  column :body, :varchar
  column :blog_id, :integer

  def self.for_blog(blog_id)
    where(:blog_id => blog_id)
  end

end
