# -- encoding : utf-8 --
require 'pointy_hair'

require 'pointy_hair/worker/test'

describe PointyHair::Manager do
  it "should start up N workers" do
    mgr = PointyHair::Manager.new
    mgr.worker_config = {
      :kind_1 => {
        :class => 'PointyHair::Worker::Test',
        :enabled => true,
        :instances => 2,
        :options => {
          :a => 1,
          :b => 2,
        }
      }
    }
  end
end
