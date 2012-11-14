# -- encoding : utf-8 --
require 'pointy_hair'

require 'pointy_hair/worker/test'
require 'fileutils'

describe PointyHair::Manager do
  attr_accessor :m, :base_dir, :exit_code
  class << self
    attr_accessor :instance
  end

  before(:each) do
    self.base_dir = File.expand_path("../../tmp/spec/#{File.basename(__FILE__)}", __FILE__)
    self.m = PointyHair::Manager.new
    m.base_dir = base_dir
    m.keep_files = 2
    m.poll_interval = 0.5
    self.class.instance ||= -1
    m.instance = self.class.instance += 1
    m.worker_config = {
      :kind_1 => {
        :class => 'PointyHair::Worker::Test',
        :enabled => true,
        :instances => 2,
        :options => {
          :a => 1,
          :b => 2,
 #         :redirect_stdio => false,
        }
      }
    }
    def m.get_work!
      log "get_work! #{work_id}"
      if work_id >= 10
        log "stop!"
        stop!
      else
        # pp get_child_pids
      end
      super
    end
    m.options[:redirect_stdio] = false
    m.options[:setup_signal_handlers] = false
    m.logger = false
    def m._exit! exit_code
      log { "_exit! #{exit_code}" }
    end
  end

  after(:each) do
    # FileUtils.rm_rf(base_dir)
  end

  it "should start up N workers" do
    start_manager!

    [ 0, 1 ].each do | i |
      w = m.find_worker(:kind_1, i)
      w.options.should == m.worker_config[:kind_1][:options]
      w.status.should == :exited
      w.exit_code.should == 0
    end

    m.find_worker(:kind_1, 2).should == nil
    m.find_worker(:kind_2, 0).should == nil
  end

  it "should use Worker objects" do
    worker_objects = [
      PointyHair::Worker::Test.new,
      PointyHair::Worker::Test.new,
      PointyHair::Worker::Test.new, # unused instance
    ]

    m.worker_config[:kind_1][:worker_objects] = worker_objects;
    m.worker_config[:kind_1].delete(:class)
    start_manager!

    m.find_worker(:kind_1, 0).should == worker_objects[0]
    m.find_worker(:kind_1, 1).should == worker_objects[1]

    worker_objects[0].work_id.should > 0
    worker_objects[1].work_id.should > 0
    worker_objects[2].work_id.should == 0
  end

  it 'should pause workers' do
    def m.get_work!
      log "get_work! #{work_id}"
      case
      when work_id < 5
        w = find_worker(:kind_1, 0)
        w.paused.class.should == NilClass if w
      when work_id == 5
        w = find_worker(:kind_1, 0)
        log "pause! #{w}"
        w.pause!
        w.wait_until_paused!
        w.paused.class.should == Time
        w[:last_work_id] = w.work_id
      when work_id < 15
        w = find_worker(:kind_1, 0)
        w.paused.class.should == Time
        w[:last_work_id].should == w.work_id
      when work_id >= 15
        log "stop!"
        stop!
      end
      w = find_worker(:kind_1, 1)
      w.paused.class.should == NilClass if w
      super
    end

    start_manager!

    w0 = m.find_worker(:kind_1, 0)
    w1 = m.find_worker(:kind_1, 1)
    w1.work_id.should > w0.work_id
  end

  def start_manager!
    m.go!
    m.exit_code.should == 0
    m.workers.each do | w |
      w.exit_code.should == 0
    end
  end

end
