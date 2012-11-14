# -- encoding : utf-8 --
require 'pointy_hair'

require 'pointy_hair/worker/test'

describe PointyHair::Manager do
  attr_accessor :m, :base_dir, :exit_code

  before(:each) do
    self.base_dir = File.expand_path("../../tmp/spec/#{$$}", __FILE__)
    self.m = PointyHair::Manager.new
    m.base_dir = base_dir
  end

  after(:each) do
    FileUtils.rm_rf(base_dir)
  end

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
    def mgr.get_work!
      if @work_id >= 5
        $stderr.puts "stop! at #{@work_id}"
        stop!
        return nil
      else
        $stderr.puts "working #{@work_id}"
        pp get_child_pids
      end
      super
    end
    mgr.run!
  end
end
