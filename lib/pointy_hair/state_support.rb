# -- encoding : utf-8 --
require 'socket' # gethostname
require 'fileutils'
require 'yaml'

module PointyHair
  module StateSupport
    def self.included target
      super
      target.extend(ModuleMethods)
    end

    module ModuleMethods
      def state_accessor *names
        module_eval(names.map do | n |
      <<"END"
        def #{n}    ; state[#{n.inspect}]              ; end
        def #{n}= x ; @_#{n} = state[#{n.inspect}] = x ; end
        def _#{n}   ; @_#{n}                           ; end
        def _#{n}=x ; @_#{n} = x                       ; end
END
        end * "\n")
      end
    end

    def clear_state!
      now = Time.now
      @state = {
        :hostname => Socket.gethostname.force_encoding("UTF-8"),
        :status => :created,
        :status_time => now,
        :created_at => now.dup,
      }
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

    def cleanup_files!
      unless keep_files || file_exists?(:keep)
        log { "cleanup_files!" }
        remove_files!
        true
      else
        false
      end
    end

  end
end
