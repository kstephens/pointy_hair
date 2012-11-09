# -- encoding : utf-8 --
require 'fileutils'
require 'yaml'
require 'socket' # gethostname

require 'pointy_hair/file_support'

module PointyHair
  class Worker
    include FileSupport
    attr_accessor :kind, :options, :instance, :status, :base_dir
    attr_accessor :paused, :exit_code, :work_id, :max_work_id
    attr_accessor :pid, :process_count, :keep_files
    attr_accessor :work, :work_error

    def to_s
      "\#<#{self.class} #{kind} #{instance} #{pid} #{status[:status]} #{work_id} >"
    end

    def initialize
      @procline_prefix = "pointy_hair "
      @running = false
      @pid = $$
      @process_count = 0
      @status = { }
      @work_history = [ ]
      @work_id = 0
      @exit_code = nil
      @pause_interval = 5
    end

    def pid= x
      @dir = nil unless x.nil?
      @pid = x
    end

    def clear_status!
      @status = { :hostname => Socket.gethostname.force_encoding("UTF-8") }
      set_status! :created
    end

    def dir
      @dir ||= "#{@base_dir}/#{@kind}/#{@instance}/#{@pid}".freeze
    end

    def start_process!
      @process_count += 1
      self.exit_code = nil
      begin
        self.pid = $$
        clear_status!
        set_status! :start
        setup_process!
        run!
      rescue ::Exception => exc
        self.exit_code ||= 1
        e = make_error_hash(exc)
        set_status! :exit_error, :error => e
        write_file! :exit_error do | fh |
          fh.set_encoding("UTF-8")
          e[:work_id] = @work_id
          e[:time] = status[:status_time]
          write_yaml(fh, e)
        end
        raise exc
      ensure
        self.exit_code ||= 0
        set_status! :exited
        write_file! :exited do | fh |
          fh.set_encoding("UTF-8")
          fh.puts exit_code
        end
        Process.exit! exit_code
      end
    end

    def exited!
      unless @keep_files || file_exists?(:keep)
        remove_files!
      end
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

    def setup_process!
      current_symlink!
      redirect_stdio!
      setup_signal_handlers!
    end

    def redirect_stdio!
      $_stdin  ||= $stdin
      $_stdout ||= $stdout
      $_stderr ||= $stderr
      STDIN.close
      $stdin  = STDIN
      $stdout = STDOUT
      $stderr = STDERR
      STDOUT.reopen(File.open("#{dir}/stdout", "a+"))
      STDERR.reopen(File.open("#{dir}/stderr", "a+"))
    end

    def setup_signal_handlers!
      Signal.trap('INT') do
        log { "SIGINT" }
        stop!
      end
      Signal.trap('TERM') do
        stop!
      end
      Signal.trap('STOP') do
        pause!
      end
      Signal.trap('CONT') do
        resume!
      end
    end

    def run_loop
      set_status! :run_loop_begin
      @running = true
      while running?
        check_stop!
        check_paused!
        unless handle_paused
          set_status! :run_loop
          get_and_do_work!
        end
        check_max_work_id!
      end
      if @stopped
        stopped!
      end
      set_status! :finished
      write_file! :finished
    ensure
      set_status! :run_loop_end
    end

    def handle_paused
      if @paused
        set_status! :paused
        paused!
        loop do
          sleep(@pause_interval + rand)
          check_paused!
          check_stop!
          break unless @paused || @stop
          set_status! :paused
        end
        set_status! :resumed
        resumed!
        true
      end
    end

    def check_max_work_id!
      if @max_work_id && @max_work_id > 0 && @max_work_id < @work_id
        @running = false
        set_status! :max_work_id_reached
      end
    end

    # callback
    def paused!
    end

    # callback
    def resumed!
    end

    # callback
    def stopped!
    end

    def running?
      @running
    end

    def stop! opts = nil
      opts ||= { }
      @running = false
      @stopped = true
      if opts[:force]
        raise Error::Stop
      end
      unless file_exists? :stop
        write_file! :stop, Time.now
      end
    end

    def pause!
      @paused = true
      write_file! :paused, Time.now
    end

    def resume!
      @paused = false
      remove_file! :paused
    end

    def get_and_do_work!
      @work_error = nil
      set_status! :waiting
      @wait_t0 = @wait_t1 = Time.now
      if @work = get_work!
        @wait_t1 = Time.now
        status[:work_id] = @work_id += 1
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
          raise err
        ensure
          update_work_status!
        end
      end
    end

    # callback
    def finished_work!
    end

    # callback
    def work_error! err
    end

    def update_work_status!
      save_work_error!
      save_work_history!
    end

    def save_work_error!
      if err = @work_error
        e = make_error_hash(err)
        set_status! :error, :error => e
        write_file! :work_error do | fh |
          e[:work_id] = @work_id
          e[:time] = status[:status_time]
          write_yaml(fh, e)
        end
      end
      err
    end

    def save_work_history!
      err = @work_error

      h = {
        :work_id   => @work_id,
        :wait_time => @wait_t0,
        :work_time => @work_t0,
        :work_dt   => status[:work_dt] = @work_dt = dt(@work_t0, @work_t1),
        :wait_dt   => status[:wait_dt] = @wait_dt = dt(@wait_t0, @work_t1),
        :error     => err && err.inspect,
      }
      wh = @work_history
      wh.unshift h
      wh.pop while wh.size > 10

      t0 = wh[-1][:work_time]
      t1 = h[:work_time]
      dt = dt(t0, t1)
      if @work_per_sec = (dt && (wh.size / dt)) || nil
        write_file! :work_rate, '%10g work/min' % (@work_per_sec * 60)
      end
      @work_per_min = @work_per_sec && @work_per_sec * 60

      status[:work_per_sec] = @work_per_sec
      status[:work_per_min] = @work_per_min

      write_file! :work_history do | fh |
        x = {
          :time => @work_t0,
          :work_per_sec => @work_per_sec,
          :work_per_min => @work_per_min,
          :history => wh,
        }
        write_yaml(fh, x)
      end

      set_status!(err ? :error : :worked)
    end

    def make_error_hash err
      e = {
        :class_name => err.class.name.force_encoding('UTF-8'),
        :message    => err.message.force_encoding('UTF-8'),
        :backtrace  => err.backtrace.map{|x| x.force_encoding('UTF-8') },
      }
      e
    end

    def dt t0, t1
      t1 && t0 && t1 - t0
    end

    def get_work!
      raise "subclass responsibility"
    end

    def put_work_back! work
      raise "subclass responsibility"
    end

    def work! work
      raise "subclass responsibility"
    end

    def set_status! state = nil, data = nil
      status.update(data) if data
      if state
        now = Time.now
        status[:status] = state
        status[:status_time] = status[:"#{state}_at"] = now
        write_file! :status, status[:status].to_s
        procline!
      end
      write_file! :state do | fh |
        write_yaml(fh, status)
      end
    end

    def get_status!
      read_file! :state do | fh |
        fh.set_encoding("UTF-8")
        @status = YAML.load(fh.read) || { }
      end
    end

    def save_work! work
      write_file! :work do | fh |
        write_yaml(fh, work)
      end
    end

    def save_last_work!
      rename_file! :work, :completed_work
    end

    def check_paused!
      @paused = file_exists?(:paused)
    end

    def check_stop!
      if file_exists?(:stop)
        stop!
      end
    end

    def log msg = nil
      msg ||= yield if block_given?
      $stderr.puts "#{Time.now.iso8601(4)} #{self} #{$$} #{msg}"
    end

    def procline!
      $0 = "#{@procline_prefix}#{kind}:#{instance}:#{process_count} #{status[:status]} [#{work_id}/#{max_work_id || :*}] #{@work_per_min && ('%5g w/min' % @work_per_min)}"
    end
  end
end
