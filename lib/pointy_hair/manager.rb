require 'time'
require 'pp'

module PointyHair
  class Manager
    attr_accessor :state_dir, :worker_config, :status, :loop_id

    def initialize
      @state_dir = '/tmp/pointy-hair'
      @workers = [ ]
      @loop_id = 0
    end

    def workers; @workers; end

    def reload_config!
    end

    def run!
      loop do
        @loop_id += 1
        reload_config!
        make_workers!
        prune_workers!
        spawn_workers!
        check_workers!
        sleep 5
        $stderr.puts "workers::\n"
        workers.each do | worker |
          $stderr.puts "  #{worker}"
        end
        $stderr.puts "----\n\n"
      end
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
            worker.base_dir = state_dir
            worker.kind = kind
            worker.options = cfg[:options]
            worker.instance = i
            new_workers << worker
            log { "created worker #{worker}" }
          end
        end
      end
      workers.concat(new_workers)
    end

    def prune_workers!
      # log "prune_workers!"
      worker_config.each do | kind, cfg |
        cls = get_class(cfg[:class])
        instances = cfg[:instances] || 1
        instances = 0 unless cfg[:enabled]
        unneeded = workers.select { | o | o.kind == kind && o.class == cls && o.instance >= instances }
        unneeded.each do | worker |
          log { "pruning worker #{worker}" }
          stop_worker! worker
          workers.delete(worker)
        end
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
        exit_code = 0
        begin
          worker.pid = $$
          worker.clear_status!
          worker.setup_process!
          worker.run!
        rescue ::Exception => exc
          worker.exit_code ||= 1
          raise exc
        ensure
          worker.write_file! :exited do | fh |
            fh.puts exit_code
          end
        end
        Process.exit! exit_code
      end
      log { "spawned worker #{worker}" }
    end

    def check_workers!
      workers.each do | worker |
        log { "checking worker #{worker}" }
        worker.get_status!
        now = Time.now.gmtime
        worker.status[:checked_at] = now
        case
        when worker_exited?(worker)
          log { "worker exited #{worker.pid}" }
          worker.pid = nil
        when ! process_exists?(worker.pid)
          log { "worker died #{worker.pid}" }
          worker.write_file! :died
          worker.get_status!
          worker.set_status! :died, :gone_at => now
          worker.pid = nil
        when worker_stuck?(worker)
          log { "worker stuck #{worker.pid}" }
          worker.write_file! :stuck
          worker.get_status!
          worker.set_status! :stuck, :stuck_at => now
          worker.pid = nil
        else
          worker.write_file! :checked
        end
        # pp worker
      end
    end

    def worker_exited? worker
      worker.file_exists? :exited
    end

    def stop_worker! worker
      worker.write_file! :stop
      Process.kill('TERM', worker.pid)
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

    def log msg = nil
      msg ||= yield if block_given?
      $stderr.puts "#{Time.now.iso8601(4)} #{self} #{$$} #{msg}"
    end

    def procline!
      $0 = "#{self.class.name} #{status} [#{loop_id}]"
    end

  end
end
