# -- encoding : utf-8 --
require 'fileutils'
require 'yaml'
require 'socket' # gethostname

require 'pointy_hair/file_support'

module PointyHair
  class Worker
    include FileSupport
    attr_accessor :kind, :options, :instance, :base_dir, :logger
    attr_accessor :max_work_id
    attr_accessor :pid, :pid_running, :ppid
    attr_accessor :process_count, :keep_files, :pause_interval
    attr_accessor :work, :work_error
    attr_accessor :work_history
    attr_accessor :exited

    def state     ; @state; end
    def checked_at   ; @checked_at ; end
    def checked_at= x; state[:checked_at] = @checked_at = x ; end

    eval(
    [ :status, :work_id, :exit_code, :running, :stopping, :stopped, :pause, :paused, :resume, :resumed ].map do | n |
      <<"END"
        def #{n}    ; state[#{n.inspect}]              ; end
        def #{n}= x ; @_#{n} = state[#{n.inspect}] = x ; end
        def _#{n}   ; @_#{n}                           ; end
END
    end * "\n")

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

    def clear_state!
      @state = {
        :hostname => Socket.gethostname.force_encoding("UTF-8"),
      }
      set_status! :created
    end

    def dir
      @dir ||= "#{@base_dir}/#{@kind}/#{@instance}/#{@pid || '_'}".freeze
    end

    # Called by Manager, before spawning.
    def before_start_process!
      self.options ||= { }
      self.exited = false
      @process_count += 1
    end

    def start_process!
      at_start_process!
      run!
    rescue ::Exception => exc
      at_process_exception! exc
    ensure
      at_end_process!
    end

    def at_start_process!
      @status_now = Time.now
      self.pid = $$
      self.pid_running = @status_now
      self.ppid = Process.ppid
      clear_state!
      self.exit_code = nil
      self.work_id = 0
      set_status! :started
      setup_process!
      self
    end

    def at_process_exception! exc
      self.exit_code ||= 1
      e = make_error_hash(exc)
      set_status! :exit_error, :error => e
      write_file! :exit_error do | fh |
        fh.set_encoding("UTF-8")
        e[:work_id] = work_id
        e[:time] = state[:status_time]
        write_yaml(fh, e)
      end
      raise exc
    end

    def at_end_process!
      self.exit_code ||= 0
      set_status! :exited
      write_file! :exited do | fh |
        fh.set_encoding("UTF-8")
        fh.puts exit_code
      end
      self.exited = true
      _exit! exit_code
    end

    def _exit! code
      Process.exit! code
    end

    def cleanup_files!
      unless keep_files || file_exists?(:keep)
        log { "cleanup_files!" }
        remove_files!
        true
      else
        false
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

    # ProcessSupport
    def setup_process!
      current_symlink!
      redirect_stdio! unless @options[:redirect_stdio] == false
      setup_signal_handlers! unless @options[:setup_signal_handlers] == false
      @logger ||= $stderr unless @logger == false
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
      set_signal_handler!('INT') do
        stop!
      end
      set_signal_handler!('TERM') do
        stop!
      end
      set_signal_handler!('STOP') do
        pause!
      end
      set_signal_handler!('CONT') do
        resume!
      end
    end

    def set_signal_handler! name
      @old_signal_handlers ||= { }
      name = name.to_s
      @old_signal_handlers[name] ||= Signal.trap(name) do
        log { "SIG#{name}" }
        write_file! :signal, name
        set_status! :signal, :signal => name
        yield
      end
      self
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

    #############################################
    #  Pausing support

    def pause!
      log { "pause!" }
      self.pause = now = Time.now
      remove_file! :resume
      write_file! :pause, now
    end

    def resume!
      log { "resume!" }
      self.resume = now = Time.now
      remove_file! :pause
      write_file! :resume, now
    end

    def check_pause!
      self.pause = file_exists?(:pause)
    end

    # Called from Manager.
    def wait_until_paused!
      until paused
        get_state!
        sleep 0.25
      end
      self
    end

    def wait_until_resumed!
      while paused
        get_state!
        sleep 0.25
      end
      self
    end

    def handle_pausing
      if pause
        at_pause!
        loop do
          sleep(pause_interval + rand)
          while_paused!
          check_pause!
          check_stop!
          break if stopping
          check_ppid!
          if ! pause
            at_resume!
            break
          end
          set_status! :paused
        end
        true
      end
    end

    def at_pause!
      remove_file! :resumed
      self.resumed = nil
      self.paused = @status_now = Time.now
      write_status_file! :paused
      log { "paused!" }
      paused!
    end

    def at_resume!
      remove_file! :resume
      remove_file! :paused
      self.paused = nil
      self.resumed = @status_now = Time.now
      write_status_file! :resumed
      log { "resumed!" }
      resumed!
    end

    # callback
    def paused!
    end

    # callback
    def while_paused!
    end

    # callback
    def resumed!
    end


    # Stop support.
    def stop! opts = nil
      unless stopping or stopped
        now = @status_now = Time.now
        self.running = false
        self.stopping = now
        unless file_exists? :stop
          write_file! :stop, now
        end
        @status_now = nil
        if opts and opts[:force] and $$ == pid
          raise Error::Stop
        end
      end
    end

    def check_stop!
      unless self.stopping
        if file_exists?(:stop)
          self.stopping = Time.now # Time.parse(file_read!(:stop))
          write_status_file! :stopping
          log { "stopping!" }
          stopping!
        end
      end
      self
    end

    def at_stopped!
      unless self.stopped
        if self.stopping
          self.stopped = @status_now = Time.now
          # remove_file! :stop
          write_status_file! :stopped
          log { "stopped!" }
          stopped!
        end
      end
      self
    end

    # callback
    def stopping!
    end

    # callback
    def stopped!
    end

    def check_ppid!
      if ppid != (current_ppid = Process.ppid)
        write_status_file! :parent_changed
        parent_changed!
        stop!
      end
    end

    # callback
    def parent_changed!
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

    def save_work! work
      write_file! :work do | fh |
        write_yaml(fh, work)
      end
      self
    end

    def save_last_work!
      rename_file! :work, :completed_work
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
        set_status! :work_error, :error => e
        write_file! :work_error do | fh |
          e[:work_id] = @work_id
          e[:time] = state[:status_time]
          write_yaml(fh, e)
        end
      end
      err
    end

    def save_work_history!
      err = @work_error

      h = {
        :work_id   => @work_id,
        :loop_time => @loop_t0,
        :wait_time => @wait_t0,
        :work_time => @work_t0,
        :loop_dt   => state[:loop_dt] = @loop_dt = dt(@loop_t0, @loop_t1),
        :wait_dt   => state[:wait_dt] = @wait_dt = dt(@wait_t0, @wait_t1),
        :work_dt   => state[:work_dt] = @work_dt = dt(@work_t0, @work_t1),
        :work_error => err && err.inspect,
      }
      wh = @work_history
      wh.unshift h
      wh.pop while wh.size > 10

      state[:loop_dt]      = @loop_dt

      t0 = wh[-1][:work_time]
      t1 = h[:work_time]
      dt = dt(t0, t1)
      state[:work_per_sec] = @work_per_sec = (dt && (wh.size / dt)) || nil
      state[:work_per_min] = @work_per_min = @work_per_sec && @work_per_sec * 60

      if @work_per_min
        write_file! :work_rate, '%10g work/min' % (@work_per_min)
      end

      write_file! :work_history do | fh |
        x = {
          :time => @work_t0,
          :loop_dt      => @loop_dt,
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
        :message    => err.message.dup.force_encoding('UTF-8'),
        :backtrace  => err.backtrace.map{|x| x.force_encoding('UTF-8') },
      }
      e
    end

    def dt t0, t1
      t1 and t0 and t1 > t0 and t1 - t0
    end

    def set_status! status = nil, data = nil
      state.update(data) if data
      if status or data
        now = @status_now || Time.now
        unless state[:kind]
          state.update(worker_to_Hash)
        end
        state[:status] = status
        state[:status_time] = now
        state[:"#{status}_at"] = now.dup
        write_file! :status, state[:status].to_s
        procline!
      end
      write_file! :state do | fh |
        write_yaml(fh, state)
      end
      @status_now = nil
      self
    end

    def worker_to_Hash
      h = {
        :kind       => kind,
        :instance   => instance,
        :class      => self.class.name,
        :pid        => pid,
        :pid_running => pid_running,
        :ppid       => ppid,
        :dir        => dir,
      }
    end

    def write_status_file! status
      set_status! status
      write_file! status, state[:status_time]
      self
    end

    # Used from Manager
    def get_state!
      read_file! :state do | fh |
        fh.set_encoding("UTF-8")
        @state = YAML.load(fh.read) || { }
      end
      self
    end

    def log msg = nil
      if @logger
        msg ||= yield if block_given?
        @logger.puts "#{Time.now.iso8601(4)} #{self} #{$$} #{msg}"
      end
    end

    def procline!
      $0 = "#{@procline_prefix}#{kind}:#{instance}:#{process_count} #{status} [#{work_id}/#{max_work_id || :*}] #{@work_per_min && ('%5g w/min' % @work_per_min)}"
    end
  end
end
