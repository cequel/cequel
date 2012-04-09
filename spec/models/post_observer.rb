class PostObserver < ActiveModel::Observer

  def before_create(post)
    post.observed!(:before_create)
  end

  def after_create(post)
    post.observed!(:after_create)
  end

  def before_save(post)
    post.observed!(:before_save)
  end

  def after_save(post)
    post.observed!(:after_save)
  end

  def before_update(post)
    post.observed!(:before_update)
  end

  def after_update(post)
    post.observed!(:after_update)
  end

  def before_destroy(post)
    post.observed!(:before_destroy)
  end

  def after_destroy(post)
    post.observed!(:after_destroy)
  end

  def before_validation(post)
    post.observed!(:before_validation)
  end

  def after_validation(post)
    post.observed!(:after_validation)
  end

end
