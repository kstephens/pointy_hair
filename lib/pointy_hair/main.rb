require 'pointy_hair'

module PointyHair
  class Main
    def run!
      require 'pointy_hair/worker/test'

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
      mgr.run!
    end
  end
end

PointyHair::Main.new.run!


