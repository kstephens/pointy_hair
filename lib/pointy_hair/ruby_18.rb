if RUBY_VERSION =~ /^1\.8/
  class String
    def force_encoding x; self; end
  end
  class IO
    def set_encoding x; self; end
  end
end
