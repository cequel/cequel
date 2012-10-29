class CommentCounts < Cequel::Model::Counter

  key :blog_id, :int
  columns :uuid

  self.default_batch_size = 2

end
