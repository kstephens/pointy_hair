# -- encoding : utf-8 --
require 'pointy_hair/file_support'
require 'pointy_hair/stop_support'
require 'pointy_hair/pause_support'
require 'pointy_hair/process_support'
require 'pointy_hair/state_support'

module PointyHair
  class Worker
    include FileSupport, StopSupport, PauseSupport, ProcessSupport, StateSupport
    attr_accessor :kind, :options, :instance, :base_dir, :logger
    attr_accessor :max_work_id
    attr_accessor :pid, :pid_running, :ppid
    attr_accessor :process_count, :keep_files
    attr_accessor :work, :work_error
    attr_accessor :work_history
    attr_accessor :exited

    def state     ; @state; end
    def checked_at   ; @checked_at ; end
    def checked_at= x; state[:checked_at] = @checked_at = x ; end

    state_accessor :status, :work_id, :exit_code, :running, :stopping, :stopped, :pause, :paused, :resume, :resumed

    def to_s
      "\#<#{self.class} #{kind} #{instance} #{pid} #{status} #{work_id} >"
    end

    def to_s_short
      "#{kind}:#{instance}:#{pid}"
    end


    # SUBCLASS RESPONSIBLILTY
    def get_work!
      raise "subclass responsibility"
    end

    def put_work_back! work
      raise "subclass responsibility"
    end

    def work! work
      raise "subclass responsibility"
    end


    def initialize opts = nil
      @options = { }
      @state = { }
      @procline_prefix = "pointy_hair "
      @pid = $$
      @ppid = Process.ppid
      @pid_running = nil
      @process_count = 0
      @work_history = [ ]
      self.work_id = 0
      @pause_interval = 5
      if opts
        opts.each do | k, v|
          send("#{k}=", v)
        end
      end
    end

    def pid= x
      @dir = nil
      @pid = x
    end

    def infer_pid!
      @pid ||= current_pid
      self
    end

    def current_pid
      x = current_symlink_value and x.to_i
    end

    def dir
      @dir ||= "#{@base_dir}/#{@kind}/#{@instance}/#{@pid || '_'}".freeze
    end

    def run!
      set_status! :before_run
      before_run!
      run_loop
    ensure
      set_status! :after_run
      after_run!
      set_status! :exiting
    end

    # callback
    def before_run!
    end

    # callback
    def after_run!
    end

    # RunLoop
    def running?
      running
    end

    def run_loop
      set_status! :run_loop_begin
      self.running = true
      while running? and not stopping
        @loop_t0 = Time.now
        check_stop!
        break if stopping
        check_pause!
        unless handle_pausing
          set_status! :run_loop
          get_and_do_work!
        end
        check_max_work_id!
        check_ppid!
      end
      at_stopped!
      self.running = false
      write_status_file! :finished
    ensure
      set_status! :run_loop_end
    end

    def check_max_work_id!
      if @max_work_id && @max_work_id > 0 && @max_work_id < work_id
        self.running = false
        set_status! :max_work_id_reached
        max_work_id!
      end
    end

    # callback
    def max_work_id!
    end

    def get_and_do_work!
      @work_error = nil
      set_status! :waiting
      @wait_t0 = @wait_t1 = Time.now
      if @work = get_work!
        @wait_t1 = Time.now
        self.work_id += 1
        set_status! :working
        save_work! work
        rename_file! :work_error, :last_work_error
        @work_t0 = @work_t1 = Time.now
        begin
          work! work
          @work_t1 = Time.now
          save_last_work!
          finished_work!
        rescue ::Exception => exc
          @work_t1 = Time.now
          @work_error = exc
          work_error! exc
          raise exc
        ensure
          @loop_t1 = Time.now
          update_work_status!
        end
      end
      self
    end

    # callback
    def finished_work!
    end

    # callback
    def work_error! err
    end

    def kill! signal = 'INT'
      if pid_running
        Process.kill(signal, pid)
      end
    end

    def log msg = nil
      if @logger
        msg ||= yield if block_given?
        @logger.puts "#{Time.now.iso8601(4)} #{self} #{$$} #{msg}"
      end
    end

  end
end
