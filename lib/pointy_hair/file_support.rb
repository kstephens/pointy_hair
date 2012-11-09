module PointyHair
  module FileSupport
    def expand_file file
      File.expand_path(file.to_s, dir)
    end

    def write_file! file, thing = nil, &blk
      # log { "write_file! #{file}" }
      file = expand_file(file)
      FileUtils.mkdir_p(File.dirname(file))
      if blk.nil? and thing.nil?
        thing = Time.now
      end
      case thing
      when Time
        thing = thing.iso8601(4)
      end
      blk ||= lambda { | fh | fh.puts thing }
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
      file = expand_file(file)
      blk ||= lambda { | fh | fh.read }
      result = File.open(file, "r", &blk)
    rescue Errno::ENOENT
      nil
    end

    def rename_file! a, b
      a = expand_file(a)
      b = expand_file(b)
      File.rename(a, b)
    rescue Errno::ENOENT
    end

    def remove_file! file
      file = expand_file(file)
      File.unlink(file)
    rescue Errno::ENOENT
    end

    def file_exists? file
      file = expand_file(file)
      File.exist?(file)
    end

    def remove_files!
      FileUtils.rm_rf(dir)
      unless File.exist?(file = current_symlink)
        File.unlink(file)
      end
    end

    def current_symlink
      File.expand_path("../current", dir)
    end

    def current_symlink!
      file = current_symlink
      FileUtils.mkdir_p(File.dirname(file))
      File.unlink(file) rescue nil
      File.symlink(File.basename(dir), file)
    end
  end
end
