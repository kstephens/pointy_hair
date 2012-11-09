require 'fileutils'
require 'yaml'

module PointyHair
  class Worker
    attr_accessor :kind, :options, :instance, :status, :base_dir
    attr_accessor :paused, :exit_code, :work_id, :max_work_id
    attr_accessor :pid, :process_count

    def to_s
      "\#<#{self.class} #{kind} #{instance} #{pid} #{status[:status]} #{work_id} >"
    end

    def initialize
      @procline_prefix = "pointy_hair "
      @running = false
      @pid = $$
      @process_count = 0
      @status = { }
      @work_id = 0
      @exit_code = nil
    end

    def pid= x
      @dir = nil unless x.nil?
      @pid = x
    end

    def clear_status!
      @status = { }
      set_status! :created
    end

    def dir; @dir ||= "#{@base_dir}/#{@kind}/#{@instance}/#{@pid}"; end

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
        raise exc
      ensure
        self.exit_code ||= 0
        set_status! :exited
        write_file! :exited do | fh |
          fh.puts exit_code
        end
        Process.exit! exit_code
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

    def before_run!
    end

    def after_run!
    end

    def setup_process!
      current_symlink!
      redirect_stdio!
      setup_signal_handlers!
    end

    def redirect_stdio!
      $_stdin ||= $stdin
      $_stdout ||= $stdout
      $_stderr ||= $stderr
      STDIN.close
      $stdin = STDIN
      $stdout = STDOUT
      $stderr = STDERR
      STDOUT.reopen(File.open("#{dir}/stdout", "a+"))
      STDERR.reopen(File.open("#{dir}/stderr", "a+"))
    end

    def setup_signal_handlers!
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
          do_work!
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
          sleep(5 + rand)
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

    def paused!
    end

    def resumed!
    end

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
        write_file! :stop, Time.now.gmtime
      end
    end

    def pause!
      @paused = true
      write_file! :paused, Time.now.gmtime
    end

    def resume!
      @paused = false
      remove_file! :paused
    end

    def do_work!
      set_status! :waiting
      if work = get_work!
        status[:work_id] = @work_id += 1
        set_status! :working
        set_work! work
        set_last_work! work
        t0 = Time.now
        begin
          err = nil
          work! work
        rescue ::Exception => exc
          err = exc
          e = {
            :class_name => err.class.name,
            :message => err.message,
            :backtrace => err.backtrace,
          }
          set_status! :error, :error => e
          write_file! :error do | fh |
            e[:work_id] = @work_id
            e[:time] = status[:status_time]
            fh.write YAML.dump(e)
          end
          raise err
        ensure
          unless err
            t1 = Time.now
            dt = t1 - t0
            status[:work_dt] = dt
            h = status[:work_history] ||= [ ]
            h << { :work_id => @work_id, :time => t1, :dt => dt }
            h.shift while h.size > 10
            set_status!
          end
          set_status! :worked
        end
      end
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
        now = Time.now.gmtime
        status[:status] = state
        status[:status_time] = status[:"#{state}_at"] = now
        write_file! :status, status[:status].to_s
        procline!
      end
      write_file! :state do | fh |
        fh.write YAML.dump(status)
      end
    end

    def get_status!
      read_file! :state do | fh |
        @status = YAML.load(fh.read) || { }
      end
    end

    def set_work! work
      write_file! :work do | fh |
        fh.write YAML.dump(work)
      end
    end

    def set_last_work! work
      write_file! :last_work do | fh |
        work = { :work => work }
        work[:time] = Time.now.gmtime
        fh.write YAML.dump(work)
      end
    end

    def expand_file file
      File.expand_path(file.to_s, dir)
    end

    def write_file! file, thing = nil, &blk
      # log { "write_file! #{file}" }
      file = expand_file(file)
      FileUtils.mkdir_p(File.dirname(file))
      case thing
      when Time
        thing = thing.iso8601(4)
      end
      blk ||= lambda { | fh | fh.puts thing }
      result = File.open(tmp = "#{file}.tmp", "w+", &blk)
      File.chmod(0644, tmp)
      File.rename(tmp, file)
      tmp = nil
      result
    ensure
      if tmp
        File.unlink(tmp) rescue nil
      end
    end

    def read_file! file, &blk
      file = expand_file(file)
      blk ||= lambda { | fh | fh.read }
      result = File.open(file, "r", &blk)
    rescue Errno::ENOENT
      nil
    end

    def remove_file! file
      file = expand_file(file)
      File.unlink(file)
    rescue Errno::ENOENT
    end

    def file_exists? file
      file = expand_file(file)
      File.exist?(file)
    end

    def remove_files!
      FileUtils.rm_rf(dir)
      unless File.exist?(file = current_symlink)
        File.unlink(file)
      end
    end

    def check_paused!
      @paused = file_exists?(:paused)
    end

    def check_stop!
      if file_exists?(:stop)
        stop!
      end
    end

    def current_symlink
      File.expand_path("../current", dir)
    end

    def current_symlink!
      file = current_symlink
      FileUtils.mkdir_p(File.dirname(file))
      File.unlink(file) rescue nil
      File.symlink(File.basename(dir), file)
    end

    def log msg = nil
      msg ||= yield if block_given?
      $stderr.puts "#{Time.now.iso8601(4)} #{self} #{$$} #{msg}"
    end

    def procline!
      $0 = "#{@procline_prefix}#{kind}:#{instance}:#{process_count} #{status[:status]} [#{work_id}/#{max_work_id || :*}]"
    end
  end
end
