require 'pointy_hair'

require 'fileutils'

describe PointyHair::Worker do
  attr_accessor :w, :base_dir, :exit_code

  before(:each) do
    self.base_dir = File.expand_path("../../tmp/spec/#{$$}", __FILE__)
    self.w = PointyHair::Worker.new
    w.kind = :test
    w.instance = 123
    w.base_dir = base_dir
    # $stderr.puts "base_dir = #{base_dir}"
  end

  after(:each) do
    FileUtils.rm_rf(base_dir)
  end

  it "should initialize correctly" do
    w.pid.should == Process.pid
    w.ppid.should == Process.ppid
    w.process_count.should == 0
    w.work_history.should == [ ]
    w.running?.should == nil
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

  it "should initialize state" do
    w.clear_state!
    w.state[:hostname].should == Socket.gethostname
    w.status.should == :created
    w.state[:status_time].should_not == nil
    w.state[:created_at].should == w.state[:status_time]
  end

  it "should return files relative to #dir" do
    w.base_dir = "foobar"
    w.expand_file("baz").should == "foobar/test/123/#{$$}/baz"
    w.expand_file("/baz").should == "/baz"
  end

  it "should store state in file" do
    state_file = w.expand_file(:state)
    File.exist?(state_file).should == false
    w.file_exists?(:state).should == false
    w.clear_state!
    w.status
    File.exist?(state_file).should == true
    w.file_exists?(:state).should == true
  end

  it "should restore state from a file" do
    w.set_status!(:xyz)
    w.instance_variable_set("@state", nil)
    w.get_state!
    w.status == :xyz
    w.state[:status_time].class.should == Time
  end

  it "should initialize state in #start_process" do
    w.pid = -1
    w.pid_running = nil
    w.ppid = -1
    w.exit_code = nil
    $this = self
    self.exit_code = nil
    def w.run!
      5
    end
    def w.redirect_stdio!
    end
    def w.setup_signal_handlers!
    end
    def w._exit! code
      $this.exit_code = code
    end
    w.start_process!

    w.pid.should == $$
    w.ppid.should == Process.ppid
    w.pid_running.class.should == Time
    w.status.should == :exited
    w.process_count.should == 1
    w.exit_code.should == 0
    self.exit_code.should == 0

    w.file_exists?(:status).should == true
    w.file_exists?(:state).should == true
    File.read(w.expand_file(:status)).should == "exited\n"
    w.file_exists?(:exited).should == true
    w.file_exists?(:exit_error).should == false
  end

  it "should complete all tests" do
    true.should == true
  end

end
