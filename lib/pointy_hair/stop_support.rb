module PointyHair
  module StopSupport
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
  end
end
