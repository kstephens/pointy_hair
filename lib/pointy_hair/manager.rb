# -- encoding : utf-8 --
require 'time'
require 'pp'
require 'thread'

module PointyHair
  class Manager < Worker
    attr_accessor :worker_config, :poll_interval, :keep_files, :verbose

    def initialize
      super
      @base_dir = '/tmp/pointy-hair'
      @workers = [ ]
      @kind = "manager"
      @instance = 0
      @poll_interval = 5
      @keep_files = (ENV['POINTY_HAIR_KEEP_FILES'] || '0').to_i > 0
    end

    def reload_config!
    end

    def workers; @workers; end

    def setup_process!
      current_symlink!
      setup_signal_handlers!
    end

    def before_run!
      start_reaper!
    end

    def get_work!
      true
    end

    def work! work
      reload_config!
      make_workers!
      prune_workers!
      spawn_workers!
      check_workers!
      write_workers_status!
      show_workers! if @verbose
      sleep poll_interval
    end

    def after_run!
      stop_workers!
      stop_reaper!
      check_workers!
      get_workers_status!
      write_workers_status!
      show_workers! if @verbose
      exited!
    end

    def paused!
      super
      workers.each do | w |
        pause_worker! w
      end
    end

    def resumed!
      workers.each do | w |
        resume_worker! w
      end
      super
    end

    def stopped!
      #workers.each do | w |
      #  stop_worker! w
      #end
      super
    end

    def start_reaper!
      @reap_pids = Queue.new
      @reaper_thread = Thread.new do
        loop do
          case pid = @reap_pids.deq
          when :stop
            break
          else
            Process.waitpid(pid)
            log { "reaped #{pid}" }
          end
        end
      end
    end

    def stop_reaper!
      @reap_pids.enq(:stop)
      @reaper_thread.join(60)
      @reaper_thread = nil
    end

    def make_workers!
      # log "make_workers!"
      # pp worker_config
      new_workers = [ ]
      worker_config.each do | kind, cfg |
        next unless cfg[:enabled]
        cls = get_class(cfg[:class])
        (cfg[:instances] || 1).times do | i |
          unless worker = workers.find { | w | w.kind == kind && w.class == cls && w.instance == i }
            worker = cls.new
            worker.pid = nil
            worker.base_dir = "#{dir}/worker"
            worker.kind = kind
            worker.instance = i
            new_workers << worker
            log { "created worker #{worker}" }
          end
          worker.options = cfg[:options]
          worker.keep_files = keep_files
        end
      end
      workers.concat(new_workers)
    end

    def prune_workers!
      # log "prune_workers!"
      remove_workers = [ ]
      worker_config.each do | kind, cfg |
        cls = get_class(cfg[:class])
        instances = cfg[:instances] || 1
        instances = 0 unless cfg[:enabled]
        unneeded = workers.select { | o | o.kind == kind && o.class == cls && o.instance >= instances }
        unneeded.each do | worker |
          log { "pruning worker #{worker}" }
          stop_worker! worker
          remove_workers << worker
        end
      end
      remove_workers.each do | w |
        workers.delete(w)
      end
    end

    def spawn_workers!
      # log "spawn_workers!"
      workers.each do | worker |
        unless worker.pid
          spawn_worker! worker
        end
      end
    end

    def spawn_worker! worker
      # log { "spawning worker #{worker}" }
      worker.pid = Process.fork do
        worker.start_process!
      end
      log { "spawned worker #{worker}" }
    end

    def check_workers!
      workers.each do | worker |
        # log { "checking worker #{worker}" }
        worker.get_status!
        now = Time.now
        case
        when worker_exited?(worker)
          worker_exited! worker
        when ! process_exists?(worker.pid)
          worker_set_status! worker, :died, now
          worker_exited! worker
        when worker_stuck?(worker)
          worker_set_status! worker, :stuck, now
          # TODO: put work back in!
          worker_exited! worker
        end
        worker.status[:checked_at] = now
        worker.write_file! :checked, now
        # pp worker
      end
    end

    def worker_set_status! worker, state, now, data = nil
      log { "worker #{state} #{worker.pid}" }
      worker.write_file! state, now
      worker.get_status!
      worker.set_status! state
    end

    def worker_exited! worker
      # log { "queue reap pid #{worker.pid}" }
      @reap_pids.enq(worker.pid)
      worker.exited!
      worker.pid = nil
    end

    def worker_exited? worker
      worker.file_exists? :exited
    end

    def pause_worker! worker
      worker.pause!
    end

    def resume_worker! worker
      worker.resume!
    end

    def stop_workers!
      workers.each do | worker |
        worker.stop!
        worker_exited! worker
      end
    end

    def stop_worker! worker
      worker.stop!
      # Process.kill('TERM', worker.pid)
    end

    def process_exists? pid
      Process.kill(0, pid)
      true
    rescue
      Errno::ESRCH
      false
    end

    def worker_stuck? worker
      false
    end

    def get_workers_status!
      workers.each do | worker |
        get_worker_status! worker
      end
    end

    def get_worker_status! worker
      save = worker.status[:checked_at]
      worker.get_status!
      worker.status[:checked_at] = save
    end

    def write_workers_status!
      data = { :time => Time.now }
      ws = data[:workers] ||= [ ]
      workers_sorted.each do | worker |
        w = {
          :kind       => worker.kind,
          :instance   => worker.instance,
          :pid        => worker.pid,
          :checked_at => worker.status[:checked_at],
          :status     => worker.status[:status],
        }
        ws << w
      end
      write_file! :workers_status do | fh |
        write_yaml fh, data
      end
    end

    def show_workers!
      $stderr.puts "workers::\n"
      workers_sorted.each do | w |
        $stderr.puts "  #{w}"
      end
      $stderr.puts "----\n\n"
    end

    def workers_sorted
      ws = workers.sort { | a, b | a.kind.to_s <=> b.kind.to_s }
      ws.sort! { | a, b | a.instance <=> b.instance }
      ws
    end

    def get_class name
      return name if Class === name
      @class_cache ||= { }
      name = name.to_s
      if x = @class_cache[name]
        return x
      end
      path = name.to_s.split("::")
      path.shift if path.first.size == 0
      cls = path.inject(Object) { | m, n | m.const_get(n) }
      @class_cache[name] = cls
    end

  end
end
