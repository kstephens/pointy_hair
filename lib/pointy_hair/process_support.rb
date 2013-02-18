# -- encoding : utf-8 --

module PointyHair
  module ProcessSupport
    # Called by Manager, before spawning.
    def before_start_process!
      now = @status_now || Time.now
      self.options ||= { }
      self.exited = false
      @process_count += 1
      clear_state!
      self.stopping = self.exited = false
      self.status = :starting
      self.state[:status_time] = now
      self.state[:starting_at] = now
      self.ppid = $$
      self.exit_code = nil
      @status_now = nil
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
      write_file! :exit_code do | fh |
        fh.set_encoding("UTF-8")
        fh.puts exit_code
      end
      write_status_file! :exited
      self.exited = true
      _exit! exit_code
    end

    def _exit! code
      Process.exit! code
    end

    def setup_process!
      current_symlink!
      redirect_stdio! unless @options[:redirect_stdio] == false
      setup_signal_handlers! unless @options[:setup_signal_handlers] == false
      @logger ||= $stderr unless @logger == false
    end

    def redirect_stdio!
      $stdin  = STDIN
      $stdout = STDOUT
      $stderr = STDERR
      STDIN.reopen(File.open("/dev/null"))
      STDOUT.reopen(File.open("#{dir}/stdout", "a+"))
      STDERR.reopen(File.open("#{dir}/stderr", "a+"))
    end

    def setup_signal_handlers!
      set_signal_handler!('INT') do
        stop!
        raise Error::Interrupt
      end
      set_signal_handler!('TERM') do
        stop!
      end
      set_signal_handler!('TSTP') do
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

    def procline!
      $0 = "#{@procline_prefix}#{kind}:#{instance}:#{process_count} #{status} #{work_id} [#{work_count}/#{max_work_count || :*}] #{@work_per_min && ('%5g w/min' % @work_per_min)}"
    end

  end
end
