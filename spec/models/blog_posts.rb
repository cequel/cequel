class BlogPosts < Cequel::Model::Dictionary
  key :blog_id, :int
  maps :uuid => :int
end
