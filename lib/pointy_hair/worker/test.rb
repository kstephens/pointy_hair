# -- encoding : utf-8 --
module PointyHair
  class Worker
    class Test < self
      def initialize
        super
        @counter = 0
      end

      def get_work!
        sleep(1 + rand)
        if file_exists?(old_work_file)
          @counter = get_old_work! || 0
        else
          @counter += 1
        end
      end

      def work! work
        sleep(1 + rand)
        $_stdout.puts "#{Time.now.iso8601(4)} #{self} => #{work}"
        write_file! "output" do | fh |
          fh.puts work.to_s
        end
      end

      def put_work_back! work
        save_old_work! work
      end

      def save_old_work! work
        write_file!(old_work_file) do | fh |
          fh.write YAML.dump(work)
        end
      end

      def get_old_work!
        read_file!(old_work_file) do | fh |
          YAML.load(fh)
        end
      end

      def old_work_file
        '../old_work'
      end
    end
  end
end
