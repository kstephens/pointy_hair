module PointyHair
  module PauseSupport
    attr_accessor :pause_interval

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
  end
end
