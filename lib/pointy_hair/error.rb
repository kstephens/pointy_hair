# -- encoding : utf-8 --
module PointyHair
  class Error < ::Exception
    class Internal < self; end
    class Stop < self; end
  end
end
