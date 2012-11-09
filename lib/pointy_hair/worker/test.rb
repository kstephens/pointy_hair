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
        @counter += 1
      end

      def work! work
        sleep(1 + rand)
        $_stdout.puts "#{Time.now.iso8601(4)} #{self} => #{work}"
        write_file! "output" do | fh |
          fh.puts work.to_s
        end
      end
    end
  end
end
