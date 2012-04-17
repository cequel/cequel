class AssetObserver < Cequel::Model::Observer
  def before_save(asset)
    asset.observed!(:before_save)
  end
end
