module PointyHair
  class Error < ::Exception
    class Stop < self; end
  end
end
