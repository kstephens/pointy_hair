require 'pointy_hair'

require 'fileutils'

describe PointyHair::Worker do
  attr_accessor :w, :base_dir

  before(:each) do
    self.base_dir = "./tmp/spec/#{$$}"
    self.w = PointyHair::Worker.new
    w.base_dir = base_dir
  end

  after(:each) do
    FileUtils.rm_rf(base_dir)
  end

  it "should initialize correctly" do
    w.pid.should == Process.pid
    w.ppid.should == Process.ppid
    w.process_count.should == 0
    w.work_history.should == [ ]
    w.running?.should == false
    w.exit_code.should == nil
    w.pause_interval.should == 5
    w.state.should == { }
    w.status.should == nil
  end

  it "should construct dir from base_dir and pid" do
    w.base_dir = "/tmp/foo/bar"
    w.kind = 'hello'
    w.instance = 9
    w.dir.should == "/tmp/foo/bar/hello/9/#{$$}"
    w.pid = $$ + 1
    w.dir.should == "/tmp/foo/bar/hello/9/#{$$ + 1}"
  end

  it "should store #status in status[:status]" do
    w.status = :foobar
    w.state[:status].should == :foobar
    w.status.should == :foobar
  end

  it "should store #checked_at in status[:checked_at]" do
    now = Time.now
    w.checked_at = now
    w.state[:checked_at].should == now
    w.checked_at.should == now
  end

  it "should store #exit_code in status[:exit_code]" do
    w.exit_code = 1234
    w.state[:exit_code].should == 1234
    w.exit_code.should == 1234
  end

  it "should initalize state" do
    w.clear_state!
    w.state[:hostname].should == Socket.gethostname
    w.status.should == :created
    w.state[:status_time].should_not == nil
    w.state[:created_at].should == w.state[:status_time]
  end

end
