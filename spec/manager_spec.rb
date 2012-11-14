# -- encoding : utf-8 --
require 'pointy_hair'

require 'pointy_hair/worker/test'
require 'fileutils'

describe PointyHair::Manager do
  attr_accessor :m, :base_dir, :exit_code

  @@instance = 0

  before(:each) do
    self.base_dir = File.expand_path("../../tmp/spec/#{File.basename(__FILE__)}", __FILE__)
    puts "base_dir = #{self.base_dir}"
    self.m = PointyHair::Manager.new
    m.base_dir = base_dir
    m.keep_files = 2
    m.poll_interval = 0.5
    m.instance = @@instance += 1
  end

  after(:each) do
    # FileUtils.rm_rf(base_dir)
  end

  it "should start up N workers" do
    m.worker_config = {
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
    start_manager!
  end

  it "should use Worker objects" do
    worker_objects = [
      PointyHair::Worker::Test.new,
      PointyHair::Worker::Test.new,
      PointyHair::Worker::Test.new, # unused instance
    ]

    m.worker_config = {
      :kind_1 => {
        :worker_objects => worker_objects,
        :enabled => true,
        :instances => 2,
        :options => {
          :a => 1,
          :b => 2,
        }
      }
    }
    start_manager!

    worker_objects[0].work_id.should > 0
    worker_objects[1].work_id.should > 0
    worker_objects[2].work_id.should == 0
  end

  def start_manager!
    def m.get_work!
      if @work_id >= 10
        $stderr.puts "stop! at #{@work_id}"
        stop!
        return nil
      else
        $stderr.puts "working #{@work_id}"
        # pp get_child_pids
      end
      super
    end
    m.run!
  end

end
