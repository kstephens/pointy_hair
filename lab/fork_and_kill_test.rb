require 'pp'

cpid = Process.fork do
  loop do
    sleep 5
    puts "  <<#{$$}>>"
  end
end
pp cpid

sleep 2
pp Process.kill(0, cpid)
pp Process.waitpid(cpid, Process::WNOHANG)

pp Process.kill(9, cpid)
pp Process.waitpid(cpid, Process::WNOHANG)
pp Process.kill(0, cpid)
sleep 1
pp Process.waitpid(cpid, Process::WNOHANG)
pp Process.kill(0, cpid) rescue Errno::ESRCH
pp Process.waitpid(cpid, Process::WNOHANG)

