# -- encoding : utf-8 --
require 'time'
require 'pp'
require 'thread'

module PointyHair
  class Manager < Worker
    attr_accessor :worker_config, :poll_interval, :verbose

    def initialize opts = nil
      super
      @base_dir = '/tmp/pointy-hair'
      @workers = [ ]
      @workers_running = [ ]
      @workers_pruned = [ ]
      @kind = "manager"
      @instance = 0
      @poll_interval = 5
      @keep_files = (ENV['POINTY_HAIR_KEEP_FILES'] || '0').to_i
      @keep_files = false unless @keep_files > 0
    end

    def reload_config!
    end

    def workers; @workers; end
    def workers_running; @workers_running; end

    def go!
      before_start_process!
      start_process!
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
      stop_checking_at = Time.now + 5 # TODO max average run_loop time.
      until Time.now >= stop_checking_at or workers_running.empty?
        check_workers!
        sleep 1
      end
      unless workers_running.empty?
        kill_workers!
      end
      stop_reaper!
      check_workers!
      get_workers_state!
      write_workers_status!
      show_workers! if @verbose
      cleanup_files!
    end

    def cleanup_files!
      keep = false
      workers.each do | worker |
        r = worker.cleanup_files!
        keep ||= ! r
      end
      @keep_files ||= keep
      super
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
      log { "stopped!: stopping workers" }
      workers.each do | w |
        stop_worker! w
      end
      super
    end

    def start_reaper!
      @reap_pids = Queue.new
      @reaper_thread = Thread.new do
        loop do
          case cmd = @reap_pids.deq
          when :stop
            break
          else
            pid, worker = cmd
            if (Process.waitpid(pid) rescue nil)
              log { "reaped #{pid} #{worker}" }
            end
          end
        end
      end
    end

    def stop_reaper!
      log { "stop_reaper!" }
      @reap_pids.enq(:stop)
      @reaper_thread.join(60)
      @reaper_thread = nil
      log { "stop_reeaper! finished" }
    end

    def make_workers!
      # log "make_workers!"
      # pp worker_config
      new_workers = [ ]
      worker_config.each do | kind, cfg |
        next unless cfg[:enabled]
        cls ||= nil
        (cfg[:instances] || 1).times do | i |
          unless worker = find_worker(kind, i)
            if wo = cfg[:worker_objects]
              unless worker = wo[i]
                raise ArgumentError, "kind #{kind}, instance #{i}: no worker_object available"
              end
            end
            unless worker
              cls ||= cfg_class(cfg)
              worker = cls.new
            end
            worker.kind = kind
            worker.instance = i
            worker.pid = nil
            worker.pid_running = nil
            worker.base_dir = "#{dir}/worker"
            new_workers << worker
            worker_created! worker
            log { "created worker #{worker}" }
          end
          worker.options = cfg[:options]
          worker.keep_files = keep_files
        end
      end
      workers.concat(new_workers)
    end

    # Callback
    def worker_created! worker
    end

    def find_worker kind, instance
      workers.find { | w | w.kind == kind && w.instance == instance }
    end

    def prune_workers!
      # log "prune_workers!"
      remove_workers = [ ]
      worker_config.each do | kind, cfg |
        instances = cfg[:instances] || 1
        instances = 0 unless cfg[:enabled]
        unneeded = workers.select { | o | o.kind == kind && o.instance >= instances }
        unneeded.each do | worker |
          log { "pruning worker #{worker}" }
          stop_worker! worker
          remove_workers << worker
          worker_pruned! worker
        end
      end
      remove_workers.each do | w |
        workers.delete(w)
      end
      @workers_pruned.concat(remove_workers)
    end

    # Callbacks
    def worker_pruned! worker
    end

    def cfg_class cfg
      cls = get_class(cfg[:class] || (wo = cfg[:worker_objects] and wo = wo[0] and wo.class))
    end

    def spawn_workers!
      # log "spawn_workers!"
      workers.each do | worker |
        unless worker.pid and worker.pid_running
          spawn_worker! worker
        end
      end
    end

    def spawn_worker! worker
      now = Time.now
      # log { "spawning worker #{worker}" }
      worker.before_start_process!
      worker.ppid = $$
      worker.pid = Process.fork do
        worker.start_process!
      end
      worker.pid_running = now
      log { "spawned worker #{worker}" }
      worker_spawned! worker
    end

    # Callback
    def worker_spawned! worker
    end

    def check_workers!
      running = [ ]
      (workers + @workers_pruned).each do | worker |
        # log { "checking worker #{worker}" }
        worker.get_state!
        now = @status_now = Time.now
        update = true
        if worker.pid_running
          case
          when worker_exited?(worker)
            # clean exit
            at_worker_exit! worker
          when process_exists?(worker.pid) && process_terminated?(worker.pid)
            # process disappeared
            at_worker_died! worker
          when worker_stuck?(worker)
            # process stuck
            worker_stuck! worker
          else
            # process running
            running << worker
            worker.pid_running = now
          end
          worker.checked_at = now
          worker.write_file! :checked, now
        end
        # pp worker
      end
      @workers_running = running
    end

    def worker_set_status! worker, state, now, data = nil
      log { "worker #{state} #{worker.pid}" }
      worker.write_file! state, now
      worker.get_state!
      worker.set_status! state
    end

    def at_worker_exit! worker
      unless worker.exited
        log { "worker exited #{worker.pid} #{worker.exit_code}" }
        @workers_pruned.delete(worker)
        worker.exited = true
        # if it disappeared, there is no process to reap.
        unless worker.status == :died
          @reap_pids.enq([worker.pid, worker])
        end
        worker.pid_running = nil
        worker_exited! worker
      end
    end

    # Callback
    def worker_exited! worker
    end

    def worker_exited? worker
      worker.exited or
        worker.status == :exited or
        ! worker.pid_running or
        worker.file_exists? :exited
    end

    def at_worker_died! worker
      worker.exit_code ||= -1
      worker_set_status! worker, :died, @status_now
      log { "worker_died! #{worker}" }
      worker_died! worker
      at_worker_exit! worker
    end

    # Callback
    def worker_died! worker
    end

    def pause_worker! worker
      worker.pause!
    end

    def resume_worker! worker
      worker.resume!
    end

    def stop_workers!
      workers.each do | worker |
        stop_worker! worker
      end
    end

    def stop_worker! worker
      unless worker.exited
        worker.stop!
        # Process.kill('TERM', worker.pid)
      end
    end

    def kill_workers!
      log { "kill_workers!" }
      workers.each do | worker |
        Process.kill(9, worker.pid) rescue nil
      end
    end

    def process_exists? pid
      Process.kill(0, pid)
    rescue Errno::EPERM
      true
    rescue Errno::ESRCH
      false
    end

    def process_terminated? pid
      Process.waitpid(pid, Process::WNOHANG) == pid
    rescue Errno::ECHILD
      true
    end

    def worker_stuck? worker
      false
    end

    def worker_stuck! worker
      # KILL -9 worker.
      worker_set_status! worker, :stuck, now
      # TODO: put work back in!
      at_worker_exit! worker
    end

    def get_workers_state!
      workers.each do | worker |
        get_worker_state! worker
      end
    end

    def get_worker_state! worker
      worker.get_state!
    end

    def write_workers_status!
      data = { :time => Time.now }
      ws = data[:workers] ||= [ ]
      workers_sorted.each do | worker |
        w = worker.worker_to_Hash
        w[:started_at] = worker.state[:started_at]
        w[:checked_at] = worker.checked_at
        w[:status]     = worker.status
        if x = worker.exit_code
          w[:exited_at] = worker.state[:exited_at]
          w[:exit_code] = x
        end
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
      raise ArgumentError if name.nil?
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

    def get_child_pids pid = nil
      pid ||= self.pid
      # -x OS X
      lines = `ps -x -o pid,ppid,pgid,uid,gid,ruid,rgid,pcpu,pmem,tty,xstat,command`.split("\n").map{|l| l.strip.split(/\s+/)}
      header = lines.shift.map{|k| k.downcase.to_sym}
      out = [ ]
      lines.each do | f |
        h = { }
        header.each_with_index do | k, i |
          v = f[i]
          case k
          when :'%cpu', :'%mem'
            v = v.to_f
          when :pid, :ppid, :pgid, :uid, :gid, :ruid, :rgid
            v = v.to_i
          end
          h[k] = v
        end
        if h[:ppid] == pid and worker = workers.find{|w| w.pid == h[:pid] }
          h[:worker] = worker
          out << h
        end
        h
      end
      out
    end

  end
end
