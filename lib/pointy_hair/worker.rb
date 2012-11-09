require 'fileutils'
require 'yaml'

module PointyHair
  class Worker
    attr_accessor :kind, :options, :instance, :pid, :status, :base_dir
    attr_accessor :paused, :exit_code, :work_id

    def to_s
      "\#<#{self.class} #{kind} #{instance} #{pid} #{status[:status]}>"
    end

    def initialize
      @running = false
      @pid = $$
      @status = { }
      @work_id = 0
    end

    def clear_status!
      @status = { }
      set_status! :unknown
    end

    def dir; "#{@base_dir}/#{@kind}/#{@instance}/#{@pid}"; end
    def status_file;    "#{dir}/status"; end
    def work_file;      "#{dir}/work"; end
    def last_work_file; "#{dir}/last_work"; end

    def run!
      set_status! :before_run
      before_run!
      run_loop
    ensure
      set_status! :after_run
      after_run!
    end

    def setup_process!
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

    def before_run!
    end

    def after_run!
    end

    def run_loop
      set_status! :run_loop_begin
      @running = true
      while running?
        check_stop!
        check_paused!
        if @paused
          set_status! :paused
          loop do
            sleep(5 + rand)
            check_paused!
            check_stop!
            break unless @paused || @stop
          end
          set_status! :resumed
        else
          set_status! :run_loop
          do_work!
        end
      end
      worker.set_status! :finished
      worker.write_file! :finished
    ensure
      set_status! :run_loop_end
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
    end

    def pause!
      @paused = true
    end

    def resume!
      @paused = false
    end

    def do_work!
      set_status! :waiting_for_work
      if work = get_work!
        @work_id += 1
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
            e[:time] = status[:time]
            fh.write YAML.dump(e)
          end
          raise err
        ensure
          unless err
            t1 = Time.now
            dt = t1 - t0
            status[:work_dt] = dt
            h = status[:work_history] ||= [ ]
            h << { :time => t1, :dt => dt }
            h.shift while h.size > 10
            set_status!
          end
          set_status! :finished_work
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
      if state
        now = Time.now.gmtime
        status[:status] = state
        status[:time] = status[:"#{state}_at"] = now
        procline!
      end
      status.update(data) if data
      write_file! :status do | fh |
        fh.write YAML.dump(status)
      end
    end

    def get_status!
      read_file! :status do | fh |
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

    def write_file! file, &blk
      log { "write_file! #{file}" }
      file = "#{dir}/#{file}"
      FileUtils.mkdir_p(File.dirname(file))
      blk ||= lambda { | fh | }
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
      file = "#{dir}/#{file}"
      blk ||= lambda { | fh | fh.read }
      result = File.open(file, "r", &blk)
    rescue Errno::ENOENT
      nil
    end

    def file_exists? file
      file = "#{dir}/#{file}"
      File.exist?(file)
    end

    def remove_files!
      FileUtils.rm_rf("#{dir}")
    end

    def check_paused!
      @paused = file_exists?(:pause)
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
      $0 = "#{kind}:#{instance} #{status[:status]} [#{work_id}]"
    end
  end
end
