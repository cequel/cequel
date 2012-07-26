class BlogPosts < Cequel::Model::Dictionary
  key :blog_id, :int
  maps :uuid => :int

  self.default_batch_size = 2
end
